//go:build e2e

package production

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/lychee-technology/forma/internal/manifest"
)

// s3ObjectStat is one object's mutation-detection fingerprint. An in-place
// overwrite that reuses a deterministic key can be byte-identical: the init
// re-export was measured reproducing the same parquet bytes, leaving Size and
// ETag unchanged, so for data objects this leg can rest on LastModified
// alone — which S3 reports at one-second granularity. Treat it as
// best-effort; schemaS3State's manifest leg is the unconditional detector.
type s3ObjectStat struct {
	Size         int64
	ETag         string
	LastModified time.Time
}

// snapshotS3Inventory stats every object under prefix (hoisted from the
// dry-run test when #248 gave it a second consumer; the prefix parameter
// lets callers scope to one schema's partition or a single key).
func snapshotS3Inventory(t *testing.T, ctx context.Context, env *Env, prefix string) map[string]s3ObjectStat {
	t.Helper()
	inv := make(map[string]s3ObjectStat)
	var token *string
	for {
		out, err := env.Cluster.S3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(env.Cluster.Bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			t.Fatalf("snapshot s3 inventory: %v", err)
		}
		for _, obj := range out.Contents {
			stat := s3ObjectStat{Size: aws.ToInt64(obj.Size), ETag: aws.ToString(obj.ETag)}
			if obj.LastModified != nil {
				stat.LastModified = obj.LastModified.UTC()
			}
			inv[aws.ToString(obj.Key)] = stat
		}
		if out.NextContinuationToken == nil {
			return inv
		}
		token = out.NextContinuationToken
	}
}

// snapshotManifest returns the manifest's raw bytes and ETag. Callers snapshot
// only after the schema's manifest exists — a load failure here is a test
// bug, not a skip.
func snapshotManifest(t *testing.T, ctx context.Context, env *Env, schema SchemaRef) ([]byte, string) {
	t.Helper()
	store := &manifest.S3Store{Client: env.Cluster.S3, Bucket: env.Cluster.Bucket}
	resolver := manifest.PathResolver{Prefix: env.CDC.ManifestPrefix, PathTemplate: env.CDC.ManifestTemplate}
	path, err := resolver.Resolve(schema.ID)
	if err != nil {
		t.Fatalf("resolve manifest path for schema %d: %v", schema.ID, err)
	}
	raw, etag, err := store.Load(ctx, path)
	if err != nil {
		t.Fatalf("load manifest %s (seed must have created it): %v", path, err)
	}
	return raw, etag
}

// schemaS3State is one schema's full observable S3 surface: every object
// under its partition plus its manifest object, with the manifest's raw
// bytes. The key diff in InitReport.NewObjects cannot see an overwrite that
// reuses a deterministic key, and a base-path set comparison cannot see a
// manifest rewritten with the same paths; stat + byte identity can (#248).
//
// The manifest is the leg that always fires: any rewrite increments its
// version field, so its bytes and ETag necessarily move even when the data
// objects beside it are reproduced byte-for-byte (see s3ObjectStat).
type schemaS3State struct {
	objects      map[string]s3ObjectStat
	manifestRaw  []byte
	manifestETag string
}

func captureSchemaS3State(t *testing.T, ctx context.Context, env *Env, schema SchemaRef) schemaS3State {
	t.Helper()
	objects := snapshotS3Inventory(t, ctx, env, buildSchemaKeyPrefix(env, schema))
	manifestKey := fmt.Sprintf("%s/manifest/%d.json", env.S3Prefix, schema.ID)
	for k, stat := range snapshotS3Inventory(t, ctx, env, manifestKey) {
		objects[k] = stat
	}
	raw, etag := snapshotManifest(t, ctx, env, schema)
	return schemaS3State{objects: objects, manifestRaw: raw, manifestETag: etag}
}

// assertSchemaS3StateUnchanged requires exact identity — same key set, same
// per-object size/ETag/LastModified, byte-identical manifest — so both
// creations/deletions and in-place mutations fail the no-write contract.
func assertSchemaS3StateUnchanged(t *testing.T, label string, before, after schemaS3State) {
	t.Helper()
	if !reflect.DeepEqual(before.objects, after.objects) {
		t.Errorf("%s: schema objects mutated:\n before: %+v\n after:  %+v",
			label, before.objects, after.objects)
	}
	if before.manifestETag != after.manifestETag || !bytes.Equal(before.manifestRaw, after.manifestRaw) {
		t.Errorf("%s: schema manifest rewritten in place: etag %s -> %s\n before: %s\n after:  %s",
			label, before.manifestETag, after.manifestETag, before.manifestRaw, after.manifestRaw)
	}
}
