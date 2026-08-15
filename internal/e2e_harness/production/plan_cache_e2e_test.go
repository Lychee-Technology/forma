//go:build e2e

package production

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
)

// assertPlanCacheNote pins the compiled-plan cache outcome recorded in the
// execution plan (recordPlanCache, #142). Notes are internal-plan-only, so
// asserting on them observes the engine, not the public API surface.
func assertPlanCacheNote(t *testing.T, res *QueryResult, want string) {
	t.Helper()
	if res == nil || res.Plan == nil {
		t.Fatalf("plan-cache note %q: result carries no execution plan", want)
	}
	for _, note := range res.Plan.Notes {
		if note == want {
			return
		}
	}
	t.Fatalf("plan notes lack %q, got %v", want, res.Plan.Notes)
}

// TestEnginePlanCacheServesRepeatedShape is the #345 wiring guard: the
// harness engine must carry the factory's plan cache, so an identical query
// shape asked twice compiles once and is served from the cache the second
// time. Delete the WithPlanCache wiring in Engine() and this goes red —
// without a cache, recordPlanCache never runs and no plan_cache note exists.
func TestEnginePlanCacheServesRepeatedShape(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	v1 := writeSimpleSchemaDir(t, coreProps, coreAttrs)
	env := NewEnv(t, cluster, WithSchemaDir(v1))
	simple := DefaultSchemaFixtures()[0]

	// 5 rows landed as base parquet; nothing hot remains after init, so the
	// dirty set is empty for both queries and cannot re-key the scope hash.
	seedGeneration(ctx, t, env, simple, 5, buildEvolutionProfile(buildLabeledExtras(nil)))
	runInitBase(ctx, t, env, simple)

	coldOnly := Query{
		Schema:         simple,
		PreferredTiers: []model.DataTier{model.DataTierWarm, model.DataTierCold},
		Limit:          20,
	}
	first := queryTierRestricted(ctx, t, env, coldOnly, 5, "first cold-only read compiles")
	assertUsesDuckDB(t, first)
	assertPlanCacheNote(t, first, "plan_cache=miss")

	second := queryTierRestricted(ctx, t, env, coldOnly, 5, "second identical read is cache-served")
	assertUsesDuckDB(t, second)
	assertPlanCacheNote(t, second, "plan_cache=hit")
}

// TestPlanCacheColdMissingRekeyEndToEnd restores #255's e2e poisoning probe,
// which was structurally vacuous before #345 (no cache in the harness) and
// stays masked in the manifest-wired evolution test (a flush changes the
// resolved path list and re-keys the scope hash regardless). Here the Env is
// glob-read (WithoutManifest): the path component of the scope key is one
// constant template string across the flush, and both measured queries run
// with an empty dirty set. Between the cached pre-flush serve and the
// post-flush read, the ONLY scope-key component that moves is the
// cold-missing set — which is why the post-flush plan_cache=miss below is
// this test's load-bearing assertion. The engine-seam twin is
// federated.TestEngineColdMissingSetRekeysPlanCache.
//
// Why the row count is NOT the instrument: the augmented scan renders as
//
//	SELECT * REPLACE (...), NULL::INTEGER AS score
//	FROM read_parquet(<glob>, union_by_name=true)
//
// Post-flush the `*` also expands a real `score`, so a stale cold-absent
// skeleton projects that name twice and the pinned DuckDB binds the first
// (real) occurrence — the stale NULL is shadowed, not honored. Deleting the
// cold-missing lines from duckPlanScopeParts and re-running this test was
// verified to fail on the plan_cache note alone, with the post-flush total
// still 3. So the scope component exists precisely so that correctness never
// rests on that duplicate-name binding order, and only the note observes it.
func TestPlanCacheColdMissingRekeyEndToEnd(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	v1 := writeSimpleSchemaDir(t, coreProps, coreAttrs)
	v2 := writeSimpleSchemaDir(t, scoreIntProps, scoreIntAttrs)
	env := NewEnv(t, cluster, WithSchemaDir(v1), WithoutManifest())
	simple := DefaultSchemaFixtures()[0]

	// 5 base rows under v1 (score does not exist), landed as base parquet;
	// evolve to v2, which adds score BEFORE its first flush. No hot rows
	// exist after init, so the dirty set is empty for every pre-flush query.
	seedGeneration(ctx, t, env, simple, 5, buildEvolutionProfile(buildLabeledExtras(nil)))
	runInitBase(ctx, t, env, simple)
	if err := env.EvolveSchema(ctx, v2); err != nil {
		t.Fatalf("evolve schema to v2: %v", err)
	}

	coldTiers := []model.DataTier{model.DataTierWarm, model.DataTierCold}

	// Positive control: the cold set is visible through the glob at all —
	// this is what makes the zero-row filter assertions below trustworthy.
	coldAll := queryTierRestricted(ctx, t, env, Query{
		Schema: simple, PreferredTiers: coldTiers, Limit: 20,
	}, 5, "base rows visible via glob read")
	assertUsesDuckDB(t, coldAll)

	coldFilter := Query{
		Schema: simple, PreferredTiers: coldTiers,
		Filters: []Filter{{Attr: "score", Op: "gte", Value: "50"}},
		Limit:   20,
	}

	// Pre-flush: score is cold-absent, the augmented scan projects typed
	// NULL, the filter excludes every base row. First serve compiles...
	pre := queryTierRestricted(ctx, t, env, coldFilter, 0, "pre-flush cold-only score filter")
	assertUsesDuckDB(t, pre)
	assertPlanCacheNote(t, pre, "plan_cache=miss")
	// ...and the identical shape is then served from the cached skeleton:
	// the poisoning hazard is now armed.
	pre2 := queryTierRestricted(ctx, t, env, coldFilter, 0, "pre-flush repeat armed from cache")
	assertPlanCacheNote(t, pre2, "plan_cache=hit")

	// Land score via its first flush (3 rows, ordinals 5-7 -> score
	// 50/60/70), leaving nothing hot, and re-run the same shape. The
	// cold-missing set alone must force a recompile: a hit here would mean a
	// skeleton compiled against a cold shape that no longer holds is still
	// serving reads (see the header note on why the total stays 3 anyway).
	seedGeneration(ctx, t, env, simple, 3, buildEvolutionProfile(buildLabeledExtras(func(ordinal int) map[string]any {
		return map[string]any{"score": float64(ordinal * 10)}
	})))
	mustFlush(ctx, t, env)
	post := queryTierRestricted(ctx, t, env, coldFilter, 3, "post-flush cold-only score filter")
	assertUsesDuckDB(t, post)
	assertPlanCacheNote(t, post, "plan_cache=miss")

	// Standing guard on what the miss above means. Re-running the same shape
	// must now hit: the post-flush miss produced a usable recompiled entry,
	// and the cache itself survived the flush. Without this line a future
	// harness change that discarded the engine (and with it the cache) during
	// flush would make the miss trivially true — an empty cache rather than a
	// re-key — and this probe would silently stop testing #255's keying.
	post2 := queryTierRestricted(ctx, t, env, coldFilter, 3, "post-flush repeat is cache-served")
	assertPlanCacheNote(t, post2, "plan_cache=hit")
}
