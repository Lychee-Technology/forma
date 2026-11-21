//go:build e2e

package production

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/cdcv2"
)

// TestPublicationCrashBoundariesAllWriters injects crashes at every S3
// staging, manifest publish, PG transaction, commit-answer and handoff
// boundary across cdc-flush, cdc-init and compactor. Each crash must leave
// reads oracle-consistent with zero partial visibility, allow a clean follow-up
// to converge, and leave any staged orphans collectable by manifest-reconcile
// --gc.
func TestPublicationCrashBoundariesAllWriters(t *testing.T) {
	t.Parallel()

	t.Run("cdc_flush_boundaries", func(t *testing.T) {
		t.Parallel()
		for _, point := range []string{
			string(cdcv2.PointStageObjects),
			string(cdcv2.PointStageManifest),
			string(cdcv2.PointBegin),
			string(cdcv2.PointPublish),
			string(cdcv2.PointMarks),
			string(cdcv2.PointCommit),
			string(cdcv2.PointCommitAnswer),
		} {
			t.Run(point, func(t *testing.T) {
				t.Parallel()
				runFlushCrashBoundary(t, point)
			})
		}
	})

	t.Run("cdc_init_boundaries", func(t *testing.T) {
		t.Parallel()
		for _, point := range []string{
			string(cdcv2.PointStageObjects),
			string(cdcv2.PointStageManifest),
			string(cdcv2.PointBegin),
			string(cdcv2.PointPublish),
			string(cdcv2.PointCommit),
			string(cdcv2.PointCommitAnswer),
			string(cdcv2.PointHandoff),
		} {
			t.Run(point, func(t *testing.T) {
				t.Parallel()
				runInitCrashBoundary(t, point)
			})
		}
	})

	t.Run("compactor_boundaries", func(t *testing.T) {
		t.Parallel()
		for _, point := range []string{
			string(cdcv2.PointStageObjects),
			string(cdcv2.PointStageManifest),
			string(cdcv2.PointBegin),
			string(cdcv2.PointPublish),
			string(cdcv2.PointCommit),
			string(cdcv2.PointCommitAnswer),
		} {
			t.Run(point, func(t *testing.T) {
				t.Parallel()
				runCompactorCrashBoundary(t, point)
			})
		}
	})
}

func runFlushCrashBoundary(t *testing.T, point string) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1]
	seedMatrixRows(ctx, t, env, wide, "f1", 3, 10)
	mustFlush(ctx, t, env)
	before := loadSchemaManifest(ctx, t, env, wide)

	seedMatrixRows(ctx, t, env, wide, "f2", 3, 20)
	cfg := env.CDC
	cfg.PublishFailpoint = crashOnceAt(point)
	_, err := env.RunFlushWith(ctx, FlushOverrides{Config: &cfg})
	if point == string(cdcv2.PointCommitAnswer) {
		// Lost answer after a landed COMMIT resolves to committed without error.
		if err != nil {
			t.Fatalf("flush commit-answer resolution failed: %v", err)
		}
		after := loadSchemaManifest(ctx, t, env, wide)
		if after.Generation != before.Generation+1 {
			t.Fatalf("commit-answer generation = %d, want %d", after.Generation, before.Generation+1)
		}
		env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 50})
		return
	}
	if err == nil {
		t.Fatalf("flush failpoint at %s must fail", point)
	}
	if cur := loadSchemaManifest(ctx, t, env, wide); cur.Generation != before.Generation {
		t.Fatalf("failed flush at %s advanced generation %d -> %d", point, before.Generation, cur.Generation)
	}
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 50})

	// Clean retry converges and leaves oracle-matched state.
	mustFlush(ctx, t, env)
	if after := loadSchemaManifest(ctx, t, env, wide); after.Generation != before.Generation+1 {
		t.Fatalf("retry flush after %s generation = %d, want %d", point, after.Generation, before.Generation+1)
	}
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 50})
	verifyGCReconcilesCleanly(ctx, t, env, wide)
}

func runInitCrashBoundary(t *testing.T, point string) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1]
	seedMatrixRows(ctx, t, env, wide, "i1", 4, 10)
	mustFlush(ctx, t, env)
	before := loadSchemaManifest(ctx, t, env, wide)
	seedMatrixRows(ctx, t, env, wide, "i2", 2, 20)

	cfg := env.CDC
	cfg.PublishFailpoint = crashOnceAt(point)
	_, err := env.RunInitWith(ctx, wide, InitOverrides{Config: &cfg})
	switch point {
	case string(cdcv2.PointCommitAnswer):
		if err != nil {
			t.Fatalf("init commit-answer resolution failed: %v", err)
		}
		env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 50})
		return
	case string(cdcv2.PointHandoff):
		// Handoff fails after the init generation committed: reads stay
		// oracle-consistent (dirty rows deduplicate against base), and a
		// follow-up init completes handoff cleanly.
		if err == nil {
			t.Fatal("init failpoint at handoff must fail")
		}
		env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 50})
		if _, err := env.RunInit(ctx, wide); err != nil {
			t.Fatalf("follow-up init after handoff crash: %v", err)
		}
		env.MarkOnboarded(ctx, wide)
		env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 50})
		return
	}
	if err == nil {
		t.Fatalf("init failpoint at %s must fail", point)
	}
	if cur := loadSchemaManifest(ctx, t, env, wide); cur.Generation != before.Generation {
		t.Fatalf("failed init at %s advanced generation %d -> %d", point, before.Generation, cur.Generation)
	}
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 50})

	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("clean retry init after %s: %v", point, err)
	}
	env.MarkOnboarded(ctx, wide)
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 50})
	verifyGCReconcilesCleanly(ctx, t, env, wide)
}

func runCompactorCrashBoundary(t *testing.T, point string) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1]
	seedMatrixRows(ctx, t, env, wide, "c1", 3, 10)
	mustFlush(ctx, t, env)
	seedMatrixRows(ctx, t, env, wide, "c2", 3, 20)
	mustFlush(ctx, t, env)
	before := loadSchemaManifest(ctx, t, env, wide)

	_, err := env.RunCompactionWith(ctx, wide, CompactionOverrides{
		TargetBaseSizeBytes: 1,
		PublishFailpoint:    crashOnceAt(point),
	})
	if point == string(cdcv2.PointCommitAnswer) {
		if err != nil {
			t.Fatalf("compactor commit-answer resolution failed: %v", err)
		}
		after := loadSchemaManifest(ctx, t, env, wide)
		if after.Generation != before.Generation+1 {
			t.Fatalf("compactor commit-answer generation = %d, want %d", after.Generation, before.Generation+1)
		}
		env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 50})
		return
	}
	if err == nil {
		t.Fatalf("compactor failpoint at %s must fail", point)
	}
	if cur := loadSchemaManifest(ctx, t, env, wide); cur.Generation != before.Generation {
		t.Fatalf("failed compactor at %s advanced generation %d -> %d", point, before.Generation, cur.Generation)
	}
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 50})

	res, err := env.RunCompactionWith(ctx, wide, CompactionOverrides{TargetBaseSizeBytes: 1})
	if err != nil {
		t.Fatalf("clean retry compaction after %s: %v", point, err)
	}
	if res.Generation == 0 {
		t.Fatalf("retry compaction after %s did not commit a compact generation: %+v", point, res)
	}
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 50})
	verifyGCReconcilesCleanly(ctx, t, env, wide)
}

// TestPublicationConcurrentWritersAndReaders runs cdc-flush, cdc-init, and
// compactor concurrently against the same schema while concurrent filtered and
// paginated federated queries read continuously over real PostgreSQL + RustFS +
// DuckDB. Every reader must succeed with consistent snapshots (no deleted row
// resurrection, no partial state), and post-convergence queries must match the
// oracle.
func TestPublicationConcurrentWritersAndReaders(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithDuckMaxConnections(4))
	wide := DefaultSchemaFixtures()[1]

	initial := seedMatrixRows(ctx, t, env, wide, "base", 6, 100)
	deletedID := initial[0].RowID
	mustFlush(ctx, t, env)
	mustApplyEvents(ctx, t, env, "update and delete before race",
		UpdateEvent(wide, initial[1].RowID, map[string]any{"title": "updated-live", "count": float64(777)}),
		DeleteEvent(wide, deletedID),
	)
	mustFlush(ctx, t, env)
	seedMatrixRows(ctx, t, env, wide, "hot", 5, 200)

	readCtx, stopReaders := context.WithCancel(ctx)
	readsCompleted, waitReaders := startConcurrentRaceReaders(readCtx, env, wide, deletedID)

	var writeWG sync.WaitGroup
	errs := make(chan error, 3)
	writeWG.Add(3)
	go func() {
		defer writeWG.Done()
		_, err := env.RunFlushWith(ctx, FlushOverrides{})
		errs <- err
	}()
	go func() {
		defer writeWG.Done()
		_, err := env.RunInitWith(ctx, wide, InitOverrides{})
		errs <- err
	}()
	go func() {
		defer writeWG.Done()
		_, err := env.RunCompactionWith(ctx, wide, CompactionOverrides{TargetBaseSizeBytes: 1})
		errs <- err
	}()
	writeWG.Wait()
	close(errs)
	stopReaders()
	if err := waitReaders(); err != nil {
		t.Fatalf("concurrent readers saw errors: %v", err)
	}
	if readsCompleted.Load() == 0 {
		t.Fatal("concurrent readers completed 0 queries during publish race")
	}
	succeeded := 0
	for err := range errs {
		if err == nil {
			succeeded++
			continue
		}
		if !errors.Is(err, cdcv2.ErrAbandoned) && !errors.Is(err, cdc.ErrSchemaLockContended) {
			t.Fatalf("unexpected writer error in 3-way race: %v", err)
		}
	}
	if succeeded == 0 {
		t.Fatal("expected at least one racing writer to succeed")
	}

	// Drain any remaining dirty entries and verify final oracle parity and no resurrection.
	mustFlush(ctx, t, env)
	assertFilteredAndPaginated(ctx, t, env, wide, true)
	assertDeletedAbsent(ctx, t, env, wide, deletedID)
}

func startConcurrentRaceReaders(readCtx context.Context, env *Env, wide SchemaRef, deletedID any) (*atomic.Int64, func() error) {
	eng := env.Engine()
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	var completed atomic.Int64

	recordErr := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}

	for worker := 0; worker < 2; worker++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			for readCtx.Err() == nil {
				fq, err := env.buildFederatedQuery(Query{
					Schema:  wide,
					Filters: []Filter{{Attr: "count", Op: "gte", Value: "100"}},
					Sorts:   []Sort{{Attr: "count"}},
					Limit:   5,
					Offset:  offset,
				})
				if err != nil {
					recordErr(err)
					return
				}
				page, err := eng.Query(readCtx, env.Tables, fq, nil)
				if err != nil {
					if readCtx.Err() != nil {
						return
					}
					recordErr(fmt.Errorf("concurrent read: %w", err))
					return
				}
				for _, rec := range page.Records {
					if rec.RowID == deletedID {
						recordErr(fmt.Errorf("deleted row %s resurrected during concurrent publish", deletedID))
						return
					}
				}
				completed.Add(1)
			}
		}(worker * 2)
	}

	return &completed, func() error {
		wg.Wait()
		errMu.Lock()
		defer errMu.Unlock()
		return firstErr
	}
}

func crashOnceAt(point string) func(context.Context, string) error {
	var fired atomic.Bool
	return func(_ context.Context, p string) error {
		if p == point && fired.CompareAndSwap(false, true) {
			return fmt.Errorf("injected crash at %s", point)
		}
		return nil
	}
}

func verifyGCReconcilesCleanly(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) {
	t.Helper()
	time.Sleep(reconcileGrace)
	if _, err := env.reconciler(t, schema, reconcileOptions(true)).Run(ctx); err != nil {
		t.Fatalf("manifest-reconcile --gc after crash recovery: %v", err)
	}
	env.AssertQueryMatches(ctx, Query{Schema: schema, Limit: 50})
}
