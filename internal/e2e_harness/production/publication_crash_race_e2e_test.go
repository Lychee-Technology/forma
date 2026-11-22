//go:build e2e

package production

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

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
