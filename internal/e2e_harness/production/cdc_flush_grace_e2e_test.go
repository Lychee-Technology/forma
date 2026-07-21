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
// scan AFTER the mark would see the batch in neither tier. The grace widens
// the dirty barrier to (flushed_at = 0 OR flushed_at > now-GRACE), so
// just-flushed rows stay hot-readable until the reader's path snapshot
// catches up.
//
// The stale snapshot is simulated with an explicit S3ParquetPathTemplate
// hint (an explicit hint wins over the manifest source, #184/#187) listing
// only the FIRST flush's delta while the second flush has already marked and
// appended. The disabled-grace probe is the red anchor proving the grace is
// the serving mechanism, not some other path.
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

	t.Run("GraceServesJustFlushedRows", func(t *testing.T) {
		t.Parallel()
		env := NewEnv(t, SharedCluster(t))
		staleHint := seedTwoFlushedGenerations(t, env)
		// Default grace (60s): both generations were flushed within the grace,
		// so the widened barrier serves ALL rows from the hot tier even though
		// the stale path set lists only the first delta.
		if got := queryStale(t, env, staleHint); got != 8 {
			t.Errorf("graced stale-snapshot query saw %d rows, want 8 — just-flushed rows must stay hot-readable (#252)", got)
		}
	})

	t.Run("DisabledGraceExposesTheGap", func(t *testing.T) {
		t.Parallel()
		env := NewEnv(t, SharedCluster(t))
		// Restore the exact pre-#252 barrier before the engine is built.
		env.DuckCfg.FlushVisibilityGraceMs = -1
		staleHint := seedTwoFlushedGenerations(t, env)
		// Red anchor: without the grace the second generation is invisible —
		// marked flushed, absent from the stale path set. This documents that
		// the grace is the mechanism serving the rows above.
		if got := queryStale(t, env, staleHint); got != 4 {
			t.Errorf("disabled-grace stale-snapshot query saw %d rows, want 4 (the pre-#252 gap)", got)
		}
	})
}
