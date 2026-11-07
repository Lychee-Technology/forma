//go:build e2e

package production

import (
	"bytes"
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/lychee-technology/forma/internal/cdc"
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

// snapshotS3Inventory stats every object under prefix (hoisted when #248
// added a consumer needing a schema-scoped prefix; the prefix parameter
// lets callers scope to one schema's partition or a single key).
func snapshotS3Inventory(t *testing.T, ctx context.Context, env *Env, prefix string) map[string]s3ObjectStat {
	t.Helper()
	inv := make(map[string]s3ObjectStat)
	err := cdc.ForEachObject(ctx, env.Cluster.S3, env.Cluster.Bucket, prefix, func(obj types.Object) error {
		stat := s3ObjectStat{Size: aws.ToInt64(obj.Size), ETag: aws.ToString(obj.ETag)}
		if obj.LastModified != nil {
			stat.LastModified = obj.LastModified.UTC()
		}
		inv[aws.ToString(obj.Key)] = stat
		return nil
	})
	if err != nil {
		t.Fatalf("snapshot s3 inventory: %v", err)
	}
	return inv
}

// snapshotManifest returns the manifest's raw bytes and ETag. Callers snapshot
// only after the schema's manifest exists — a load failure here is a test
// bug, not a skip.
func snapshotManifest(t *testing.T, ctx context.Context, env *Env, schema SchemaRef) ([]byte, string) {
	t.Helper()
	store, _ := env.manifestAccess()
	path := buildSchemaManifestKey(t, env, schema)
	raw, etag, err := store.Load(ctx, path)
	if err != nil {
		t.Fatalf("load manifest %s (caller guarantees it exists by now; a missing one "+
			"was never created or was deleted since): %v", path, err)
	}
	return raw, etag
}

// schemaS3State is one schema's full observable S3 surface: every object
// under its partition plus its manifest object, with the manifest's raw
// bytes. The key diff in InitReport.NewObjects cannot see an overwrite that
// reuses a deterministic key, and a base-path set comparison cannot see a
// manifest rewritten with the same paths; stat + byte identity can (#248).
//
// The manifest is the leg that always fires: any save through the manifest
// store increments its version field, so its bytes and ETag necessarily move
// even when the data objects beside it are reproduced byte-for-byte (see s3ObjectStat).
type schemaS3State struct {
	objects      map[string]s3ObjectStat
	manifestRaw  []byte
	manifestETag string
}

func captureSchemaS3State(t *testing.T, ctx context.Context, env *Env, schema SchemaRef) schemaS3State {
	t.Helper()
	objects := snapshotS3Inventory(t, ctx, env, buildSchemaKeyPrefix(env, schema))
	manifestKey := buildSchemaManifestKey(t, env, schema)
	manifestObjects := snapshotS3Inventory(t, ctx, env, manifestKey)
	stat, ok := manifestObjects[manifestKey]
	if !ok || len(manifestObjects) != 1 {
		// An empty listing is not an error to ListObjectsV2, so a key that
		// drifted from the stored manifest would silently drop the stat leg —
		// the only one that sees a byte-identical re-put (via LastModified).
		t.Fatalf("manifest listing for %s must hold exactly that one object, got %v",
			manifestKey, manifestObjects)
	}
	objects[manifestKey] = stat
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
