//go:build e2e

package production

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/lychee-technology/forma/internal/manifest"
)

// TestDryRunImmutability proves cdc-flush --dry-run and cdc-init --dry-run
// mutate nothing (#180): per-row change_log state, the S3 object inventory
// (keys, sizes, ETags, timestamps), and the manifest raw bytes + version are
// identical across each dry-run, and the flush dry-run issues zero mutating
// S3 calls through the Go client. The trailing real flush is the positive
// control proving the dry-runs had pending work they chose to skip.
func TestDryRunImmutability(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	simple := DefaultSchemaFixtures()[0] // e2e_simple

	seedDryRunFixture(t, ctx, env, simple)

	t.Run("flush_dry_run_mutates_nothing", func(t *testing.T) {
		testFlushDryRunMutatesNothing(t, ctx, env, simple)
	})
	t.Run("init_dry_run_mutates_nothing", func(t *testing.T) {
		testInitDryRunMutatesNothing(t, ctx, env, simple)
	})
	t.Run("real_flush_after_dry_runs_flushes", func(t *testing.T) {
		testRealFlushAfterDryRunsFlushes(t, ctx, env)
	})
}

// seedDryRunFixture builds a realistic pre-state across all three tiers:
// flushed rows (delta parquet + manifest), base parquet from a real init,
// then pending hot work (updates, a delete, a fresh create) that a real
// flush would export.
func seedDryRunFixture(t *testing.T, ctx context.Context, env *Env, schema SchemaRef) {
	t.Helper()

	var creates []*Event
	for i := 0; i < 6; i++ {
		creates = append(creates, CreateEvent(schema, map[string]any{
			"name":  fmt.Sprintf("cold-%d", i),
			"value": float64(i) + 0.25,
		}))
	}
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}
	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("seed flush: %v", err)
	}
	if _, err := env.RunInit(ctx, schema); err != nil {
		t.Fatalf("seed init: %v", err)
	}

	pending := []*Event{
		UpdateEvent(schema, creates[0].RowID, map[string]any{"name": "hot-update-0"}),
		UpdateEvent(schema, creates[1].RowID, map[string]any{"value": 42.5}),
		DeleteEvent(schema, creates[2].RowID),
		CreateEvent(schema, map[string]any{"name": "hot-new", "value": 7.5}),
	}
	if err := env.ApplyEvents(ctx, pending...); err != nil {
		t.Fatalf("apply pending hot work: %v", err)
	}
}

// changeLogRow is the value half of a change_log snapshot entry; the map key
// is the primary key (schema_id, row_id, flushed_at), so any flushed_at
// transition shows up as a removed+added key pair.
type changeLogRow struct {
	ChangedAt int64
	DeletedAt int64 // 0 when NULL
}

type s3ObjectStat struct {
	Size         int64
	ETag         string
	LastModified time.Time
}

// stateSnapshot captures the three mutation surfaces #180 guards.
type stateSnapshot struct {
	changeLog    map[string]changeLogRow
	s3           map[string]s3ObjectStat
	manifestRaw  []byte
	manifestETag string
}

func captureState(t *testing.T, ctx context.Context, env *Env, schema SchemaRef) stateSnapshot {
	t.Helper()
	raw, etag := snapshotManifest(t, ctx, env, schema)
	return stateSnapshot{
		changeLog:    snapshotChangeLog(t, ctx, env),
		s3:           snapshotS3Inventory(t, ctx, env),
		manifestRaw:  raw,
		manifestETag: etag,
	}
}

func snapshotChangeLog(t *testing.T, ctx context.Context, env *Env) map[string]changeLogRow {
	t.Helper()
	rows, err := env.Pool.Query(ctx,
		"SELECT schema_id, row_id::text, changed_at, COALESCE(deleted_at, 0), flushed_at FROM change_log")
	if err != nil {
		t.Fatalf("snapshot change_log: %v", err)
	}
	defer rows.Close()

	snap := make(map[string]changeLogRow)
	for rows.Next() {
		var schemaID int16
		var rowID string
		var changedAt, deletedAt, flushedAt int64
		if err := rows.Scan(&schemaID, &rowID, &changedAt, &deletedAt, &flushedAt); err != nil {
			t.Fatalf("scan change_log row: %v", err)
		}
		key := fmt.Sprintf("%d/%s/%d", schemaID, rowID, flushedAt)
		snap[key] = changeLogRow{ChangedAt: changedAt, DeletedAt: deletedAt}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate change_log rows: %v", err)
	}
	return snap
}

func snapshotS3Inventory(t *testing.T, ctx context.Context, env *Env) map[string]s3ObjectStat {
	t.Helper()
	inv := make(map[string]s3ObjectStat)
	var token *string
	for {
		out, err := env.Cluster.S3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(env.Cluster.Bucket),
			Prefix:            aws.String(env.S3Prefix + "/"),
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

// snapshotManifest returns the manifest's raw bytes and ETag. The seed phase
// runs a real flush + init first, so the manifest must exist by the time any
// snapshot is taken — a load failure here is a test bug, not a skip.
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

func assertStateUnchanged(t *testing.T, label string, before, after stateSnapshot) {
	t.Helper()
	if !reflect.DeepEqual(before.changeLog, after.changeLog) {
		t.Errorf("%s: change_log mutated:\n before: %v\n after:  %v", label, before.changeLog, after.changeLog)
	}
	for key, b := range before.s3 {
		a, ok := after.s3[key]
		switch {
		case !ok:
			t.Errorf("%s: s3 object deleted: %s", label, key)
		case a != b:
			t.Errorf("%s: s3 object modified: %s\n before: %+v\n after:  %+v", label, key, b, a)
		}
	}
	for key := range after.s3 {
		if _, ok := before.s3[key]; !ok {
			t.Errorf("%s: s3 object created: %s", label, key)
		}
	}
	if !bytes.Equal(before.manifestRaw, after.manifestRaw) {
		t.Errorf("%s: manifest bytes changed:\n before: %s\n after:  %s", label, before.manifestRaw, after.manifestRaw)
	}
	if before.manifestETag != after.manifestETag {
		t.Errorf("%s: manifest etag changed: %s -> %s", label, before.manifestETag, after.manifestETag)
	}
	var mBefore, mAfter manifest.Manifest
	if err := json.Unmarshal(before.manifestRaw, &mBefore); err != nil {
		t.Fatalf("%s: unmarshal before-manifest: %v", label, err)
	}
	if err := json.Unmarshal(after.manifestRaw, &mAfter); err != nil {
		t.Fatalf("%s: unmarshal after-manifest: %v", label, err)
	}
	if mBefore.Version != mAfter.Version {
		t.Errorf("%s: manifest version changed: %d -> %d", label, mBefore.Version, mAfter.Version)
	}
}

func testFlushDryRunMutatesNothing(t *testing.T, ctx context.Context, env *Env, schema SchemaRef) {
	before := captureState(t, ctx, env, schema)

	rec := &RecordingS3{Inner: env.Cluster.S3}
	report, err := env.RunFlushWith(ctx, FlushOverrides{DryRun: true, S3: rec})
	if err != nil {
		t.Fatalf("dry-run flush: %v", err)
	}
	if report.UnflushedBefore == 0 {
		t.Fatal("positive control: dry-run flush had no pending hot rows to skip")
	}

	after := captureState(t, ctx, env, schema)
	assertStateUnchanged(t, "flush dry-run", before, after)
	if n := rec.MutatingCalls(); n != 0 {
		t.Errorf("flush dry-run issued %d mutating S3 calls through the Go client", n)
	}
}

func testInitDryRunMutatesNothing(t *testing.T, ctx context.Context, env *Env, schema SchemaRef) {
	before := captureState(t, ctx, env, schema)

	report, err := env.RunInitWith(ctx, schema, InitOverrides{DryRun: true})
	if err != nil {
		t.Fatalf("dry-run init: %v", err)
	}
	if report.RowsExported == 0 {
		t.Fatal("positive control: dry-run init planned zero rows")
	}

	after := captureState(t, ctx, env, schema)
	assertStateUnchanged(t, "init dry-run", before, after)
}

// testRealFlushAfterDryRunsFlushes is the suite-level positive control: the
// same pre-state the dry-runs saw really does have exportable work, so the
// zero-delta assertions above cannot pass vacuously.
func testRealFlushAfterDryRunsFlushes(t *testing.T, ctx context.Context, env *Env) {
	flush, err := env.RunFlush(ctx)
	if err != nil {
		t.Fatalf("real flush: %v", err)
	}
	if flush.UnflushedBefore == 0 || flush.UnflushedAfter != 0 {
		t.Fatalf("real flush unflushed before/after = %d/%d, want >0/0",
			flush.UnflushedBefore, flush.UnflushedAfter)
	}
	if len(flush.NewObjects) == 0 {
		t.Fatal("real flush created no S3 objects")
	}
}
