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
