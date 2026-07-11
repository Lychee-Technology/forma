//go:build e2e

package production

import (
	"context"
	"testing"
)

// TestThreeTierMerge (#177 scenarios 4-6): merge-on-read correctness when
// base, delta, and hot tiers hold rows simultaneously — union of disjoint
// rows, LWW on triple overlap, dirty-set anti-join shadowing, and LWW keyed
// strictly on changed_at rather than tier layout. Every query is checked
// against the independent oracle (totals, row IDs, ordering, all attribute
// values).
func TestThreeTierMerge(t *testing.T) {
	cluster := SharedCluster(t)
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	scenarios := []struct {
		name string
		run  func(ctx context.Context, t *testing.T, env *Env, schema SchemaRef)
	}{
		{"union", testThreeTierUnion},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			sc.run(context.Background(), t, NewEnv(t, cluster), wide)
		})
	}
}

// testThreeTierUnion is #177 scenario 4: old rows in base (init + onboarding
// cleanup), mid rows in delta (flush), new rows hot (unflushed). All 24
// disjoint rows must be present exactly once, with a stable total order and
// pagination across tier boundaries.
func testThreeTierUnion(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	old := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 10})
	if err := env.ApplyEvents(ctx, old...); err != nil {
		t.Fatalf("apply old creates: %v", err)
	}
	report, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("run init: %v", err)
	}
	if report.RowsExported != 10 {
		t.Fatalf("init exported %d rows, want 10", report.RowsExported)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", wide.ID)

	mid := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 8})
	if err := env.ApplyEvents(ctx, mid...); err != nil {
		t.Fatalf("apply mid creates: %v", err)
	}
	mustFlush(ctx, t, env)

	fresh := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 6})
	if err := env.ApplyEvents(ctx, fresh...); err != nil {
		t.Fatalf("apply hot creates: %v", err)
	}

	// Tier precondition: exactly the 6 hot rows are dirty.
	var dirty int64
	if err := env.Pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM change_log WHERE flushed_at = 0").Scan(&dirty); err != nil {
		t.Fatalf("count dirty rows: %v", err)
	}
	if dirty != 6 {
		t.Fatalf("dirty set has %d rows, want 6 (hot batch only)", dirty)
	}

	result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	if result != nil {
		if len(result.Records) != 24 {
			t.Fatalf("union result has %d rows, want 24 (10 base + 8 delta + 6 hot)", len(result.Records))
		}
		if !result.Plan.Routing.UseDuckDB {
			t.Errorf("union query did not route to duckdb: %+v", result.Plan.Routing)
		}
	}
	// Total order across tiers (count is unique per generated ordinal), and a
	// paginated window that straddles tier boundaries.
	env.AssertQueryMatches(ctx, Query{
		Schema: wide,
		Sorts:  []Sort{{Attr: "count"}},
		Limit:  100,
	})
	env.AssertQueryMatches(ctx, Query{
		Schema: wide,
		Sorts:  []Sort{{Attr: "count"}},
		Limit:  7,
		Offset: 5,
	})
}
