//go:build e2e

package production

import (
	"context"
	"testing"
)

// TestFullTypeRoundTripAcrossTiers proves issue #174's success criteria for
// typical values: every supported scalar type — bound and EAV — survives
// write → CDC export → parquet → federated merge-on-read intact, across all
// three tiers. Physical layer: exact parquet column set/types + row values
// vs event-derived truth. Logical layer: AssertQueryMatches compares every
// attribute of every row against the independent oracle, hot and federated.
func TestFullTypeRoundTripAcrossTiers(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 12})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}

	// Hot baseline: full-attribute oracle comparison from Postgres.
	env.AssertQueryMatches(ctx, Query{Schema: wide, PreferHot: true, Limit: 100})

	// Cold tier: base parquet becomes the only source for creates[0:6].
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx,
		"DELETE FROM change_log WHERE schema_id = $1 AND row_id = ANY($2)",
		wide.ID, rowIDs(creates[0:6]))

	// Warm tier: flush creates[6:12] into a delta file.
	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Hot tier: two more creates that stay unflushed.
	hotCreates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 2})
	if err := env.ApplyEvents(ctx, hotCreates...); err != nil {
		t.Fatalf("apply hot creates: %v", err)
	}

	// Physical layer: schema + values of every parquet file.
	truth := buildWideTruth(t, creates)
	manifests, err := env.loadManifests(ctx)
	if err != nil {
		t.Fatalf("load manifests: %v", err)
	}
	m := manifests[wide.ID]
	if m == nil {
		t.Fatal("no manifest for e2e_wide")
	}
	tierRows := map[string]int{}
	for _, f := range m.Files {
		assertWideParquetSchema(ctx, t, env, f.Path, f.Tier)
		tierRows[f.Tier] += assertWideParquetValues(ctx, t, env, f.Path, f.Tier, truth)
	}
	if tierRows["base"] != 12 {
		t.Errorf("base parquet rows = %d, want 12", tierRows["base"])
	}
	if tierRows["delta"] != 6 {
		t.Errorf("delta parquet rows = %d, want 6", tierRows["delta"])
	}

	// Logical layer: federated merge read equals the oracle, row for row,
	// attribute for attribute, and actually routes through DuckDB.
	fed := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	if fed == nil {
		return // AssertQueryMatches already failed the test
	}
	if !fed.Plan.Routing.UseDuckDB {
		t.Errorf("federated query did not route to duckdb: %+v", fed.Plan.Routing)
	}
	if len(fed.Records) != len(creates)+len(hotCreates) {
		t.Errorf("federated rows = %d, want %d", len(fed.Records), len(creates)+len(hotCreates))
	}
}
