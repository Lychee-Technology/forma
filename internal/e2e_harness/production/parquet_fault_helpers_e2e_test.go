//go:build e2e

package production

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// schemaParquetKeys lists the schema's final parquet objects currently in S3
// (tmp leftovers and manifest JSON excluded), sorted.
func schemaParquetKeys(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) []string {
	t.Helper()
	keys, err := env.listS3Keys(ctx)
	if err != nil {
		t.Fatalf("list s3 keys: %v", err)
	}
	var got []string
	for _, k := range keys {
		if strings.HasPrefix(k, schemaKeyPrefix(env, schema)) &&
			strings.HasSuffix(k, ".parquet") && !strings.Contains(k, "/_tmp/") {
			got = append(got, k)
		}
	}
	sort.Strings(got)
	return got
}

// overwriteObjectBytes replaces one S3 object's content with mutate(original
// bytes), preserving the key. This is the raw corruption vector for #187: the
// manifest and object listing still reference the key, but DuckDB now reads
// the mutated bytes. Fails if the mutation is a no-op (a corrupted test would
// silently pass otherwise).
func overwriteObjectBytes(ctx context.Context, t *testing.T, env *Env, key string, mutate func([]byte) []byte) {
	t.Helper()
	obj, err := env.Cluster.S3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(env.Cluster.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("get object %s: %v", key, err)
	}
	data, err := io.ReadAll(obj.Body)
	_ = obj.Body.Close()
	if err != nil {
		t.Fatalf("read object %s: %v", key, err)
	}
	mutated := mutate(data)
	if bytes.Equal(mutated, data) {
		t.Fatalf("mutation left object %s unchanged (%d bytes)", key, len(data))
	}
	if _, err := env.Cluster.S3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(env.Cluster.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(mutated),
	}); err != nil {
		t.Fatalf("overwrite object %s: %v", key, err)
	}
}

// corruptMidFile flips a 64-byte span in the middle of the file: the parquet
// footer and magic stay intact, so the reader accepts the file and fails on
// the mangled page data.
func corruptMidFile(data []byte) []byte {
	out := append([]byte(nil), data...)
	start := len(out) / 2
	for i := start; i < start+64 && i < len(out); i++ {
		out[i] ^= 0xFF
	}
	return out
}

// truncateHalf drops the trailing half of the file, taking the parquet footer
// and trailing magic with it, so the reader fails while planning the scan.
func truncateHalf(data []byte) []byte {
	return append([]byte(nil), data[:len(data)/2]...)
}

// deleteObject removes one S3 object by key.
func deleteObject(ctx context.Context, t *testing.T, env *Env, key string) {
	t.Helper()
	if _, err := env.Cluster.S3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(env.Cluster.Bucket),
		Key:    aws.String(key),
	}); err != nil {
		t.Fatalf("delete object %s: %v", key, err)
	}
}

// writeParquetViaDuck materializes a parquet object at key from an arbitrary
// DuckDB SELECT — the vector for files the production exporter refuses to
// write (0-row and wrong-schema parquet, #187 scenarios 5/6).
func writeParquetViaDuck(ctx context.Context, t *testing.T, env *Env, selectSQL, key string) {
	t.Helper()
	copySQL := fmt.Sprintf("COPY (%s) TO 's3://%s/%s' (FORMAT PARQUET)", selectSQL, env.Cluster.Bucket, key)
	if _, err := env.Duck.DB.ExecContext(ctx, copySQL); err != nil {
		t.Fatalf("write parquet via duckdb to %s: %v", key, err)
	}
}

// seedMultiParquet seeds schema with two disjoint flushed batches (two final
// delta parquet files) plus hot rows, and returns the two sorted parquet
// keys. Two files with distinct flushed row sets are what make single-file
// missing/corrupt scenarios observable: the other file's rows must keep
// flowing from parquet, not be superseded by hot versions.
func seedMultiParquet(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) []string {
	t.Helper()
	first := env.GenerateScript(ScriptSpec{Schema: schema, Creates: 5})
	if err := env.ApplyEvents(ctx, first...); err != nil {
		t.Fatalf("apply first flushed batch: %v", err)
	}
	mustFlush(ctx, t, env)
	second := env.GenerateScript(ScriptSpec{Schema: schema, Creates: 5})
	if err := env.ApplyEvents(ctx, second...); err != nil {
		t.Fatalf("apply second flushed batch: %v", err)
	}
	mustFlush(ctx, t, env)
	hot := env.GenerateScript(ScriptSpec{Schema: schema, Creates: 3})
	if err := env.ApplyEvents(ctx, hot...); err != nil {
		t.Fatalf("apply hot batch: %v", err)
	}
	keys := schemaParquetKeys(ctx, t, env, schema)
	if len(keys) != 2 {
		t.Fatalf("expected exactly 2 parquet files after two flushes, got %v", keys)
	}
	return keys
}
