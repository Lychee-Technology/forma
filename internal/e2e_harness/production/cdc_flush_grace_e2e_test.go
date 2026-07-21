//go:build e2e

package production

import (
	"context"
	"fmt"
	"testing"
)

// TestFlushGraceBackstopServesStaleManifestSnapshot pins the #252 read-side
// grace: even with the append-before-mark ordering, a reader that resolved
// its parquet path set BEFORE a flush's manifest append but runs its dirty
// scan AFTER the mark would see the batch in neither tier. The dirty-barrier
// cutoff anchors at the query's own path-resolution instant (minus the
// configured clock-skew margin), so rows marked after that instant stay
// hot-readable while the resolved path set may predate their delta.
//
// The e2e cannot pause a query between its path resolution and its dirty
// scan, so the race is emulated from the other side: an explicit
// S3ParquetPathTemplate hint (an explicit hint wins over the manifest
// source, #184/#187) lists only the FIRST flush's delta while the second
// flush has already marked and appended, and a large margin stands in for
// "the path set was resolved before that flush". The default-margin probe is
// the counterpart pin: in the steady state (flush completed before the
// query) the exact anchor must NOT widen, so results keep their flushed
// parquet semantics with zero drift.
func TestFlushGraceBackstopServesStaleManifestSnapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	seedTwoFlushedGenerations := func(t *testing.T, env *Env) (staleHint string) {
		t.Helper()
		wide := DefaultSchemaFixtures()[1] // e2e_wide
		gen1 := buildOpenCreates(wide, 4)
		mustApplyEvents(ctx, t, env, "seed generation 1", gen1...)
		first, err := env.RunFlushWith(ctx, FlushOverrides{})
		if err != nil {
			t.Fatalf("first flush: %v", err)
		}
		firstFinals, _ := splitKeys(first.NewObjects)
		if len(firstFinals) != 1 {
			t.Fatalf("first flush must promote exactly one final delta, got %v", firstFinals)
		}
		gen2 := make([]*Event, 0, 4)
		for i := 0; i < 4; i++ {
			gen2 = append(gen2, CreateEvent(wide, map[string]any{
				"title": fmt.Sprintf("fresh-%02d", i),
				"count": float64(300000 + i),
			}))
		}
		mustApplyEvents(ctx, t, env, "seed generation 2", gen2...)
		if _, err := env.RunFlushWith(ctx, FlushOverrides{}); err != nil {
			t.Fatalf("second flush: %v", err)
		}
		// The stale reader's path set: only the first delta, as if the second
		// flush's manifest append had not been observed yet.
		return fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, firstFinals[0])
	}

	queryStale := func(t *testing.T, env *Env, staleHint string) int {
		t.Helper()
		wide := DefaultSchemaFixtures()[1]
		res, err := env.Query(ctx, Query{
			Schema: wide, Limit: 10, S3ParquetPathTemplate: staleHint,
		})
		if err != nil {
			t.Fatalf("stale-snapshot query: %v", err)
		}
		if !res.Plan.Routing.UseDuckDB {
			t.Errorf("stale-snapshot query did not route to duckdb: %+v", res.Plan.Routing)
		}
		return len(res.Records)
	}

	t.Run("MarginServesRowsFlushedAfterSnapshot", func(t *testing.T) {
		t.Parallel()
		env := NewEnv(t, SharedCluster(t))
		// A 60s margin emulates a path set resolved before the recent flushes:
		// both generations' flushed_at land after cutoff = resolution - 60s,
		// so the widened barrier serves ALL rows from the hot tier even though
		// the stale path set lists only the first delta.
		env.DuckCfg.FlushVisibilityGraceMs = 60_000
		staleHint := seedTwoFlushedGenerations(t, env)
		if got := queryStale(t, env, staleHint); got != 8 {
			t.Errorf("margined stale-snapshot query saw %d rows, want 8 — rows flushed after the emulated snapshot must stay hot-readable (#252)", got)
		}
	})

	t.Run("ExactAnchorNeverWidensSteadyState", func(t *testing.T) {
		t.Parallel()
		env := NewEnv(t, SharedCluster(t))
		staleHint := seedTwoFlushedGenerations(t, env)
		// Default (zero margin): both flushes completed before this query
		// resolved its paths, so nothing is widened — the second generation is
		// invisible through the stale hint exactly as pre-#252. This pins both
		// the zero-drift steady state and that the margin is the serving
		// mechanism in the subtest above.
		if got := queryStale(t, env, staleHint); got != 4 {
			t.Errorf("exact-anchor stale-snapshot query saw %d rows, want 4 (steady state must not widen)", got)
		}
	})
}
