//go:build e2e

package production

import (
	"context"
	"fmt"
	"strings"
	"testing"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// seedPrunedColdTier builds the state #468 is about: rows that live ONLY in
// parquet. Three creates are exported to the base tier by cdc-init, their
// change_log entries dropped, and then — standing in for the bounded
// PostgreSQL retention design §7.3 anticipates — their entity_main/EAV rows
// are pruned. Two more creates stay hot. The oracle still expects all five,
// so a Postgres-only answer is short by three and cannot pass the oracle.
func seedPrunedColdTier(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) {
	t.Helper()
	cold := env.GenerateScript(ScriptSpec{Schema: schema, Creates: 3})
	if err := env.ApplyEvents(ctx, cold...); err != nil {
		t.Fatalf("apply cold creates: %v", err)
	}
	if _, err := env.RunInit(ctx, schema); err != nil {
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", schema.ID)
	env.ExecSQL(ctx, fmt.Sprintf("DELETE FROM %s WHERE schema_id = $1", env.Tables.EAVData), schema.ID)
	env.ExecSQL(ctx, fmt.Sprintf("DELETE FROM %s WHERE ltbase_schema_id = $1", env.Tables.EntityMain), schema.ID)
	hot := env.GenerateScript(ScriptSpec{Schema: schema, Creates: 2})
	if err := env.ApplyEvents(ctx, hot...); err != nil {
		t.Fatalf("apply hot creates: %v", err)
	}
}

// TestTierCoverageUnderHybridSmallPage pins #468 in the configuration a real
// deployment runs: Routing.Strategy defaults to hybrid, whose "small result
// set" rule parks every Limit < 1000 on the Postgres-only path, and the API
// caps items_per_page at 100. Both halves of the maintainer ruling are
// covered on one seed:
//
//   - an explicit multi-tier preferred_tiers overrides the heuristic, so an
//     ordinary page size actually reaches the three-tier DuckDB merge and
//     matches the oracle — the regression net the issue asks for;
//   - an implicit (omitted) preferred_tiers keeps the heuristic, is
//     answered hot-tier-only, and says so through the coverage marker.
func TestTierCoverageUnderHybridSmallPage(t *testing.T) {
	cluster := SharedCluster(t)
	wide := DefaultSchemaFixtures()[1] // e2e_wide
	ctx := context.Background()
	env := NewEnv(t, cluster, WithRoutingStrategy(forma.RoutingStrategyHybrid))
	seedPrunedColdTier(ctx, t, env, wide)

	t.Run("explicit_tiers_reach_duckdb", func(t *testing.T) {
		result := env.AssertQueryMatches(ctx, Query{
			Schema:         wide,
			PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
			Limit:          10, // < 1000: the hybrid rule that routes postgres-only
		})
		if result == nil {
			return
		}
		if !result.Plan.Routing.UseDuckDB {
			t.Fatalf("explicit three-tier request must reach the federated path; got UseDuckDB=false, reason=%q",
				result.Plan.Routing.Reason)
		}
		// Pin the postgres-only decision the override reversed, as the #354
		// twin does: if the threshold moves this must go red rather than stay
		// green while no longer exercising the override.
		if !strings.Contains(result.Plan.Routing.Reason, "hybrid small result set") ||
			!strings.Contains(result.Plan.Routing.Reason, "preferred_tiers") {
			t.Fatalf("the overridden decision must name both the reversed rule and the override; reason=%q",
				result.Plan.Routing.Reason)
		}
		if result.Partial != nil {
			t.Fatalf("every declared tier was consulted; page must carry no coverage marker, got %+v", result.Partial)
		}
	})

	t.Run("implicit_tiers_marked_hot_tier_only", func(t *testing.T) {
		result, err := env.Query(ctx, Query{Schema: wide, Limit: 10})
		if err != nil {
			t.Fatalf("implicit-tier query: %v", err)
		}
		if result.Plan.Routing.UseDuckDB {
			t.Fatalf("an omitted preferred_tiers keeps the small-result shortcut; got %+v", result.Plan.Routing)
		}
		if result.Plan.Routing.Reason != "hybrid small result set" {
			t.Fatalf("reason = %q, want the unoverridden heuristic verdict", result.Plan.Routing.Reason)
		}
		if result.Partial == nil {
			t.Fatal("a postgres-only answer to an all-tier request must carry the coverage marker (#468)")
		}
		want := []model.DataTier{model.DataTierWarm, model.DataTierCold}
		if got := result.Partial.UnconsultedTiers; len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
			t.Fatalf("UnconsultedTiers = %v, want %v", got, want)
		}
		if len(result.Partial.ExcludedObjects) != 0 {
			t.Fatalf("a postgres-only page must not carry a corrupt-exclusion marker, got %v", result.Partial.ExcludedObjects)
		}
		// The marker is telling the truth: the pruned cold rows are absent.
		if result.Total != 2 {
			t.Fatalf("hot-tier-only answer total = %d, want 2 (the three pruned cold rows are unreachable from Postgres)", result.Total)
		}
	})
}
