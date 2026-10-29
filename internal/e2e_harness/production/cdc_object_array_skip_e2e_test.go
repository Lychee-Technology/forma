//go:build e2e

package production

import (
	"context"
	"errors"
	"strings"
	"testing"

	publiccdc "github.com/lychee-technology/forma/cdc"
	"github.com/lychee-technology/forma/internal/cdc"
)

// seedObjectArraySkipEnv builds an Env whose e2e_simple holds an array of
// objects (objectArrayProps), with one row in it and, when mixed, one row in
// the ordinary e2e_second.
func seedObjectArraySkipEnv(ctx context.Context, t *testing.T, mixed bool) (env *Env, objectArray, flat SchemaRef) {
	t.Helper()
	env = NewEnv(t, SharedCluster(t), WithSchemaDir(writeSimpleSchemaDir(t, objectArrayProps, objectArrayAttrs)))
	fixtures := DefaultSchemaFixtures()
	objectArray, flat = fixtures[0], fixtures[2]
	events := []*Event{CreateEvent(objectArray, map[string]any{"name": "two-items", "value": float64(1), "items": []any{
		map[string]any{"sku": "a", "qty": float64(1)},
		map[string]any{"sku": "b", "qty": float64(2)},
	}})}
	if mixed {
		events = append(events, CreateEvent(flat, map[string]any{"label": "flat", "code": 1}))
	}
	mustApplyEvents(ctx, t, env, "object-array skip rows", events...)
	return env, objectArray, flat
}

// unflushedRows counts schema's change_log rows still at flushed_at = 0.
func unflushedRows(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) int64 {
	t.Helper()
	var n int64
	if err := env.Pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM change_log WHERE flushed_at = 0 AND schema_id = $1", schema.ID).Scan(&n); err != nil {
		t.Fatalf("count unflushed rows of schema %d: %v", schema.ID, err)
	}
	return n
}

// assertObjectArraySkipped: the run's own report names schema, and the
// reason names the array of objects.
func assertObjectArraySkipped(t *testing.T, skipped []cdc.SkippedSchema, schema SchemaRef) {
	t.Helper()
	if len(skipped) != 1 || skipped[0].SchemaID != schema.ID || !strings.Contains(skipped[0].Reason, `array of objects "items"`) {
		t.Fatalf("skipped schemas %+v, want schema %d skipped for the array of objects \"items\"", skipped, schema.ID)
	}
}

// TestPublicRunOnceReportsAnObjectArraySkipWithoutALogger pins #704 at the
// public cdc package with NewFlushRunner(nil), whose no-op logger drops the
// skip warning. RunOnce succeeds and its report names the skipped schema,
// whose rows stay unflushed with nothing exported for them. In the mixed
// run the ordinary schema still flushes, and every parquet object written is
// its own.
func TestPublicRunOnceReportsAnObjectArraySkipWithoutALogger(t *testing.T) {
	ctx := context.Background()
	for name, mixed := range map[string]bool{"mixed": true, "all skipped": false} {
		t.Run(name, func(t *testing.T) {
			env, objectArray, flat := seedObjectArraySkipEnv(ctx, t, mixed)
			keysBefore, err := env.listS3Keys(ctx)
			if err != nil {
				t.Fatal(err)
			}
			runner := publiccdc.NewFlushRunner(nil)
			defer func() { _ = runner.Close() }()

			report, err := runner.RunOnce(ctx, env.CDC, env.Cluster.S3, false, env.Registry)
			if err != nil {
				t.Fatalf("a run that skips a schema must succeed: %v", err)
			}
			assertObjectArraySkipped(t, report.Skipped, objectArray)
			if n := unflushedRows(ctx, t, env, objectArray); n != 1 {
				t.Fatalf("skipped schema has %d unflushed rows, want its 1 row left unflushed", n)
			}

			keysAfter, err := env.listS3Keys(ctx)
			if err != nil {
				t.Fatal(err)
			}
			finals, _ := splitKeys(diffKeys(keysBefore, keysAfter))
			manifests, err := env.loadManifests(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if m := manifests[objectArray.ID]; m != nil && len(m.Files) != 0 {
				t.Fatalf("skipped schema's manifest lists %+v, want nothing exported", m.Files)
			}
			if !mixed {
				if len(finals) != 0 {
					t.Fatalf("an all-skipped run wrote parquet %v, want none", finals)
				}
				return
			}
			if n := unflushedRows(ctx, t, env, flat); n != 0 {
				t.Fatalf("ordinary schema has %d unflushed rows, want it flushed", n)
			}
			if len(finals) == 0 {
				t.Fatal("the ordinary schema's flush wrote no parquet")
			}
			assertManifestDeltaPaths(t, manifests, flat, finals)
		})
	}
}

// TestCDCInitSkipsOrRefusesAnObjectArraySchema pins #704 on cdc-init. A run
// over every schema skips the one the parquet tiers cannot hold, reports
// it, and initializes the others. A run whose --schema-id names it fails
// with ErrSchemaNotExportable and writes nothing.
func TestCDCInitSkipsOrRefusesAnObjectArraySchema(t *testing.T) {
	ctx := context.Background()
	env, objectArray, flat := seedObjectArraySkipEnv(ctx, t, true)

	keysBefore, err := env.listS3Keys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = env.RunInit(ctx, objectArray)
	if !errors.Is(err, cdc.ErrSchemaNotExportable) || !strings.Contains(err.Error(), `array of objects "items"`) {
		t.Fatalf("cdc-init naming the schema: err %v, want ErrSchemaNotExportable naming the array of objects", err)
	}
	keysAfter, err := env.listS3Keys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if added := diffKeys(keysBefore, keysAfter); len(added) != 0 {
		t.Fatalf("a refused cdc-init wrote %v, want nothing", added)
	}

	report, err := env.RunInitWith(ctx, flat, InitOverrides{AllSchemas: true})
	if err != nil {
		t.Fatalf("cdc-init over every schema must skip, not fail: %v", err)
	}
	assertObjectArraySkipped(t, report.Skipped, objectArray)
	if report.Manifest == nil || len(report.Manifest.Files) == 0 {
		t.Fatalf("ordinary schema %d was not initialized: manifest %+v", flat.ID, report.Manifest)
	}
	manifests, err := env.loadManifests(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if m := manifests[objectArray.ID]; m != nil && len(m.Files) != 0 {
		t.Fatalf("skipped schema's manifest lists %+v, want nothing exported", m.Files)
	}
	if n := unflushedRows(ctx, t, env, objectArray); n != 1 {
		t.Fatalf("skipped schema has %d unflushed rows, want its 1 row left unflushed", n)
	}
}
