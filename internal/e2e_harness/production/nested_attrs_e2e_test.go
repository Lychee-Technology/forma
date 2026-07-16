//go:build e2e

package production

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
)

// TestFederatedReadNestedDottedAttributes pins #260: a federated DuckDB read
// of a schema whose attribute names contain dots (nested objects flattened
// to "contact.annualIncome") must return the same records the oracle
// expects, across all tiers. Before the fix the DuckDB projection emitted
// the dotted name bare, so DuckDB parsed it as table.column and the read
// failed with `Binder Error: Referenced table "contact" not found` — and
// even a quoted dotted name would miss, because the CDC exporter writes the
// parquet column as the folded alias (contact_annualIncome).
//
// The warm+cold assertions run before the hot rows are seeded because
// warm+cold reads exclude unflushed rows by design (#184 physical tier
// pruning), so the oracle — which does not model tier visibility — only
// reaches parity once every asserted row is flushed.
func TestFederatedReadNestedDottedAttributes(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	nested := SchemaRef{ID: SchemaIDNested, Name: "e2e_nested"}

	// Base tier: 5 creates exported to base parquet; clearing change_log
	// afterwards makes the parquet copy the only source (three-tier recipe).
	base := env.GenerateScript(ScriptSpec{Schema: nested, Creates: 5})
	if err := env.ApplyEvents(ctx, base...); err != nil {
		t.Fatalf("apply base events: %v", err)
	}
	if _, err := env.RunInit(ctx, nested); err != nil {
		t.Fatalf("run init (base export): %v", err)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", nested.ID)

	// Warm tier: 4 creates flushed into a delta file. Everything is now
	// flushed, so warm+cold reads must serve the full data set.
	delta := env.GenerateScript(ScriptSpec{Schema: nested, Creates: 4})
	if err := env.ApplyEvents(ctx, delta...); err != nil {
		t.Fatalf("apply delta events: %v", err)
	}
	mustFlush(ctx, t, env)

	// Warm+cold preferred tiers force the DuckDB path — the #260 repro shape.
	fed := env.AssertQueryMatches(ctx, Query{
		Schema:         nested,
		PreferredTiers: []model.DataTier{model.DataTierWarm, model.DataTierCold},
		Limit:          100,
	})
	if fed == nil || fed.Plan == nil || !fed.Plan.Routing.UseDuckDB {
		t.Fatalf("warm+cold query did not route to duckdb: %+v", fed)
	}

	// Dotted attribute in WHERE (column-bound) and ORDER BY, on the DuckDB path.
	env.AssertQueryMatches(ctx, Query{
		Schema:         nested,
		PreferredTiers: []model.DataTier{model.DataTierWarm, model.DataTierCold},
		Filters:        []Filter{{Attr: "contact.annualIncome", Op: "gte", Value: "200"}},
		Sorts:          []Sort{{Attr: "contact.annualIncome", Desc: true}},
		Limit:          100,
	})

	// Dotted EAV-only attribute in WHERE, on the DuckDB path.
	env.AssertQueryMatches(ctx, Query{
		Schema:         nested,
		PreferredTiers: []model.DataTier{model.DataTierWarm, model.DataTierCold},
		Filters:        []Filter{{Attr: "contact.note", Op: "starts_with", Value: "note"}},
		Limit:          100,
	})

	// Hot tier: 3 unflushed creates; an all-tier read merges them back in.
	hot := env.GenerateScript(ScriptSpec{Schema: nested, Creates: 3})
	if err := env.ApplyEvents(ctx, hot...); err != nil {
		t.Fatalf("apply hot events: %v", err)
	}
	env.AssertQueryMatches(ctx, Query{Schema: nested, Limit: 100})
}
