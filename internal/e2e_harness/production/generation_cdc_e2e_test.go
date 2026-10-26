//go:build e2e

package production

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/reconcile"
	"github.com/lychee-technology/forma/internal/schemaledger"
)

// #733: a CDC run exports stored values through the declared types it
// started with. A smallint to numeric apply during the run lets a new writer
// store 1001.5, which the run would export as SMALLINT 1002, so the batch
// must stop before it leaves _tmp, and nothing may list or adopt it.

// startedRegistry is the registry of a run that started before an apply:
// once the run's start-up check has read the active generation, onStarted
// applies the new declaration and writes through it, before the run selects
// its batch.
type startedRegistry struct {
	forma.SchemaRegistry
	tracker   schemaledger.Tracker
	once      sync.Once
	onStarted func(ctx context.Context) error
	err       error
}

func (r *startedRegistry) LoadedGeneration(id int16) (int64, bool) {
	return r.tracker.LoadedGeneration(id)
}

func (r *startedRegistry) ActiveGeneration(ctx context.Context, id int16) (int64, bool, error) {
	n, ok, err := r.tracker.ActiveGeneration(ctx, id)
	r.once.Do(func() { r.err = r.onStarted(ctx) })
	return n, ok, err
}

// applyAndWrite records newDir and writes qty 1001.5 to rowID through it,
// as a writer started on the new generation would, and waits for the clock
// to pass the write so the run's snapshot covers it.
func applyAndWrite(ctx context.Context, env *Env, schema SchemaRef, newDir string, rowID uuid.UUID) (*Event, error) {
	if err := env.EvolveSchema(ctx, newDir); err != nil {
		return nil, fmt.Errorf("apply smallint to numeric: %w", err)
	}
	update := UpdateEvent(schema, rowID, map[string]any{"qty": 1001.5})
	if err := env.ApplyEvents(ctx, update); err != nil {
		return nil, fmt.Errorf("write 1001.5 under the new declaration: %w", err)
	}
	waitClockPastMillis(update.ChangedAt)
	return update, nil
}

// stoppedBatch asserts that the run's only new object is one _tmp object
// holding the converted 1002, the evidence that the stop prevented a
// silently wrong publish.
func stoppedBatch(ctx context.Context, t *testing.T, env *Env, newObjects []string, rowID uuid.UUID) string {
	t.Helper()
	finals, tmps := splitKeys(newObjects)
	if len(finals) != 0 || len(tmps) != 1 {
		t.Fatalf("the stopped run created finals %v and tmps %v, want one _tmp object only", finals, tmps)
	}
	var qty string
	query := fmt.Sprintf("SELECT CAST(qty AS VARCHAR) FROM read_parquet('s3://%s/%s') WHERE row_id::VARCHAR = '%s'",
		env.Cluster.Bucket, tmps[0], rowID)
	if err := env.Duck.DB.QueryRowContext(ctx, query).Scan(&qty); err != nil {
		t.Fatalf("read qty from the stranded %s: %v", tmps[0], err)
	}
	if qty != "1002" {
		t.Fatalf("the stranded export holds qty %s, want 1002 (1001.5 through the run's smallint)", qty)
	}
	return tmps[0]
}

func manifestPaths(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) []string {
	t.Helper()
	manifests, err := env.loadManifests(ctx)
	if err != nil {
		t.Fatalf("load manifests: %v", err)
	}
	var paths []string
	if m := manifests[schema.ID]; m != nil {
		for _, f := range m.Files {
			paths = append(paths, f.Path)
		}
	}
	return paths
}

// repairLeavesUnlisted runs manifest-reconcile --repair and asserts the
// manifest still does not list the stranded object.
func repairLeavesUnlisted(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, stranded string) {
	t.Helper()
	r, cleanup := newReconcileHarness(t, ctx, env, schema, reconcile.Options{Repair: true, MaxETagRetries: 3}, true)
	defer cleanup()
	if _, err := r.Run(ctx); err != nil {
		t.Fatalf("manifest-reconcile --repair: %v", err)
	}
	for _, path := range manifestPaths(ctx, t, env, schema) {
		if strings.HasSuffix(stranded, path) || strings.HasSuffix(path, stranded) {
			t.Fatalf("repair listed the stranded %s", stranded)
		}
	}
}

func requireQty1001Point5(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, rowID uuid.UUID) {
	t.Helper()
	oltp, federated := readQtyBothRoutes(ctx, t, env, schema, rowID)
	if fmt.Sprint(oltp) != "1001.5" || fmt.Sprint(federated) != "1001.5" {
		t.Fatalf("qty reads %v (OLTP) and %v (federated), want 1001.5 on both", oltp, federated)
	}
}

func TestCDCFlushStopsABatchExportedAcrossAnApply(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(qtyDir(t, "smallint", "double_01")))
	simple := DefaultSchemaFixtures()[0]
	mustApplyEvents(ctx, t, env, "flushed row", qtyCreate(simple, 0, float64(1)))
	mustFlush(ctx, t, env) // the manifest exists and lists one delta
	before := manifestPaths(ctx, t, env, simple)
	target := qtyCreate(simple, 1, float64(7))
	mustApplyEvents(ctx, t, env, "dirty rows", target, qtyCreate(simple, 2, float64(8)))

	newDir := qtyDir(t, "numeric", "double_01")
	registry := &startedRegistry{SchemaRegistry: env.Registry, tracker: env.Registry.(schemaledger.Tracker)}
	registry.onStarted = func(ctx context.Context) error {
		_, err := applyAndWrite(ctx, env, simple, newDir, target.RowID)
		return err
	}
	report, err := env.RunFlushWith(ctx, FlushOverrides{Registry: registry})
	if registry.err != nil {
		t.Fatalf("apply during the run: %v", registry.err)
	}
	if !errors.Is(err, schemaledger.ErrGenerationChanged) {
		t.Fatalf("flush across an apply = %v, want ErrGenerationChanged", err)
	}
	if report.UnflushedAfter != report.UnflushedBefore {
		t.Fatalf("the stopped run marked rows flushed: %d unflushed before, %d after", report.UnflushedBefore, report.UnflushedAfter)
	}
	stranded := stoppedBatch(ctx, t, env, report.NewObjects, target.RowID)
	if got := manifestPaths(ctx, t, env, simple); len(got) != len(before) {
		t.Fatalf("the stopped run changed the manifest from %v to %v", before, got)
	}
	repairLeavesUnlisted(ctx, t, env, simple, stranded)

	// A run on the new declaration exports 1001.5; both routes read it.
	mustFlush(ctx, t, env)
	requireQty1001Point5(ctx, t, env, simple, target.RowID)
}

func TestCDCInitStopsABatchExportedAcrossAnApply(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(qtyDir(t, "smallint", "double_01")))
	simple := DefaultSchemaFixtures()[0]
	target := qtyCreate(simple, 0, float64(7))
	mustApplyEvents(ctx, t, env, "rows", target, qtyCreate(simple, 1, float64(8)))

	newDir := qtyDir(t, "numeric", "double_01")
	var hookErr error
	cfg := env.CDC
	cfg.BeforeExportHook = func(hctx context.Context, _ int16, _ []uuid.UUID, _ int64) error {
		if hookErr == nil {
			_, hookErr = applyAndWrite(hctx, env, simple, newDir, target.RowID)
		}
		return hookErr
	}
	keysBefore, err := env.listS3Keys(ctx)
	if err != nil {
		t.Fatalf("list keys: %v", err)
	}
	_, err = env.RunInitWith(ctx, simple, InitOverrides{Config: &cfg})
	if hookErr != nil {
		t.Fatalf("apply during the run: %v", hookErr)
	}
	if !errors.Is(err, schemaledger.ErrGenerationChanged) {
		t.Fatalf("init across an apply = %v, want ErrGenerationChanged", err)
	}
	keysAfter, err := env.listS3Keys(ctx)
	if err != nil {
		t.Fatalf("list keys: %v", err)
	}
	stranded := stoppedBatch(ctx, t, env, diffKeys(keysBefore, keysAfter), target.RowID)
	if got := manifestPaths(ctx, t, env, simple); len(got) != 0 {
		t.Fatalf("the stopped run swapped the manifest to %v", got)
	}
	repairLeavesUnlisted(ctx, t, env, simple, stranded)

	// A run on the new declaration exports 1001.5 as the base; with the
	// change log cleared the federated route reads it from the object.
	runInitBase(ctx, t, env, simple)
	requireQty1001Point5(ctx, t, env, simple, target.RowID)
}
