//go:build e2e

package production

import (
	"context"
	"testing"
)

// TestThreeTierIsolation (#177 scenarios 1-3): each tier serves a federated
// query alone, checked against the independent oracle. Base-only mirrors the
// production onboarding contract (cdc-init then change_log cleanup),
// delta-only mirrors a fully drained flush, and hot-only mirrors the
// pre-flush window (PreferHot, because the DuckDB path needs at least one
// parquet object to exist).
func TestThreeTierIsolation(t *testing.T) {
	cluster := SharedCluster(t)
	wide := DefaultSchemaFixtures()[1] // e2e_wide: full scalar type coverage (#177 scenario 8)

	scenarios := []struct {
		name string
		run  func(ctx context.Context, t *testing.T, env *Env, schema SchemaRef)
	}{
		{"base_only", testTierBaseOnly},
		{"delta_only", testTierDeltaOnly},
		{"hot_only", testTierHotOnly},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			sc.run(context.Background(), t, NewEnv(t, cluster), wide)
		})
	}
}

// testTierBaseOnly seeds rows, exports them to base via the real cdc-init,
// and clears change_log (onboarding contract) so base parquet is the ONLY
// source. Kept minimal — TestInitBaseOnlyParity (#176) covers init batching;
// this subtest pins the tier-coverage matrix entry.
func testTierBaseOnly(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 12})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}
	report, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("run init: %v", err)
	}
	if report.RowsExported != 12 {
		t.Fatalf("init exported %d rows, want 12", report.RowsExported)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", wide.ID)

	result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	if result != nil {
		if len(result.Records) != 12 {
			t.Fatalf("base-only result has %d rows, want 12", len(result.Records))
		}
		if !result.Plan.Routing.UseDuckDB {
			t.Errorf("base-only query did not route to duckdb: %+v", result.Plan.Routing)
		}
	}
	env.AssertQueryMatches(ctx, Query{
		Schema: wide,
		Sorts:  []Sort{{Attr: "count", Desc: true}},
		Limit:  5,
	})
}

// testTierDeltaOnly seeds rows and drains them through the real CDC flusher:
// only delta parquet exists (no base files, empty dirty set), so every
// record must come from the warm tier.
func testTierDeltaOnly(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 12})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}
	flush := mustFlush(ctx, t, env)
	if got := countTier(flush.Manifests[wide.ID], "base"); got != 0 {
		t.Fatalf("manifest holds %d base files, want 0 (delta-only scenario)", got)
	}
	if got := countTier(flush.Manifests[wide.ID], "delta"); got == 0 {
		t.Fatal("manifest holds no delta files after flush")
	}

	result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	if result != nil {
		if len(result.Records) != 12 {
			t.Fatalf("delta-only result has %d rows, want 12", len(result.Records))
		}
		if !result.Plan.Routing.UseDuckDB {
			t.Errorf("delta-only query did not route to duckdb: %+v", result.Plan.Routing)
		}
	}
	env.AssertQueryMatches(ctx, Query{
		Schema: wide,
		Sorts:  []Sort{{Attr: "count", Desc: true}},
		Limit:  5,
	})
}

// testTierHotOnly queries unflushed rows before any parquet exists. PreferHot
// routes to Postgres — the production shape for the pre-init window.
func testTierHotOnly(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 12})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}

	result := env.AssertQueryMatches(ctx, Query{Schema: wide, PreferHot: true, Limit: 100})
	if result != nil {
		if len(result.Records) != 12 {
			t.Fatalf("hot-only result has %d rows, want 12", len(result.Records))
		}
		if result.Plan.Routing.UseDuckDB {
			t.Errorf("hot-only PreferHot query unexpectedly routed to duckdb: %+v", result.Plan.Routing)
		}
	}
	env.AssertQueryMatches(ctx, Query{
		Schema:    wide,
		PreferHot: true,
		Sorts:     []Sort{{Attr: "count", Desc: true}},
		Limit:     5,
	})
}
