//go:build e2e

package production

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	fedengine "github.com/lychee-technology/forma/internal/federated"
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
	data := fetchObjectBytes(ctx, t, env, key)
	mutated := mutate(data)
	if bytes.Equal(mutated, data) {
		t.Fatalf("mutation left object %s unchanged (%d bytes)", key, len(data))
	}
	// putObjectBytes carries the read-back guard, so the corrupting write is
	// held to the same byte-identity bar as the restoring one.
	putObjectBytes(ctx, t, env, key, mutated)
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

// readParquetRowIDs reads one (still readable) parquet object's row_id set
// directly via DuckDB. Cast to VARCHAR: go-duckdb surfaces UUID columns as
// raw bytes otherwise (#147).
func readParquetRowIDs(ctx context.Context, t *testing.T, env *Env, key string) map[string]struct{} {
	t.Helper()
	q := fmt.Sprintf("SELECT row_id::VARCHAR FROM read_parquet('s3://%s/%s')", env.Cluster.Bucket, key)
	rows, err := env.Duck.DB.QueryContext(ctx, q)
	if err != nil {
		t.Fatalf("read parquet row_ids from %s: %v", key, err)
	}
	defer rows.Close()
	ids := map[string]struct{}{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan row_id from %s: %v", key, err)
		}
		ids[strings.ToLower(id)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate row_ids from %s: %v", key, err)
	}
	if len(ids) == 0 {
		t.Fatalf("parquet %s yielded no row_ids; expected a seeded batch", key)
	}
	return ids
}

// resultRowIDSet collects the lowercase row_id set of a query result.
func resultRowIDSet(t *testing.T, res *QueryResult) map[string]struct{} {
	t.Helper()
	ids := map[string]struct{}{}
	for _, rec := range res.Records {
		ids[strings.ToLower(rec.RowID.String())] = struct{}{}
	}
	return ids
}

func setMinus(a, b map[string]struct{}) map[string]struct{} {
	out := map[string]struct{}{}
	for k := range a {
		if _, drop := b[k]; !drop {
			out[k] = struct{}{}
		}
	}
	return out
}

func assertIDSetEqual(t *testing.T, got, want map[string]struct{}) {
	t.Helper()
	for k := range want {
		if _, ok := got[k]; !ok {
			t.Errorf("result missing expected row_id %s", k)
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("result has unexpected row_id %s", k)
		}
	}
}

// assertCorruptExclusionNote requires the plan to loudly name the excluded
// object. Notes are internal-plan-only (#301/#306) — this is exactly the
// surface embedders and operators get.
func assertCorruptExclusionNote(t *testing.T, res *QueryResult, key string) {
	t.Helper()
	for _, note := range res.Plan.Notes {
		if strings.Contains(note, fedengine.NotePartialParquetExclusion) && strings.Contains(note, key) {
			return
		}
	}
	t.Errorf("plan notes lack the corrupt-exclusion marker for %s: %v", key, res.Plan.Notes)
}

// assertPartialMarker requires the page to carry the #348 partial marker
// naming the excluded object — the page-level counterpart of
// assertCorruptExclusionNote, and the source of the HTTP-visible
// QueryResult.partial projection.
func assertPartialMarker(t *testing.T, res *QueryResult, key string) {
	t.Helper()
	if res.Partial == nil {
		t.Fatalf("page answered from a corrupt-excluded remainder must carry a partial marker")
	}
	for _, obj := range res.Partial.ExcludedObjects {
		if strings.Contains(obj, key) {
			return
		}
	}
	t.Errorf("partial marker lacks excluded object %s: %v", key, res.Partial.ExcludedObjects)
}
