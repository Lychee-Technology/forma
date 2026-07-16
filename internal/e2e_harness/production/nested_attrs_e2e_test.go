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
func TestFederatedReadNestedDottedAttributes(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	nested := SchemaRef{ID: SchemaIDNested, Name: "e2e_nested"}

	seedAllTiers(ctx, t, env, nested)

	// Warm+cold preferred tiers force the DuckDB path (hybrid routing would
	// otherwise serve this small page from Postgres) — the #260 repro shape.
	fed := env.AssertQueryMatches(ctx, Query{
		Schema:         nested,
		PreferredTiers: []model.DataTier{model.DataTierWarm, model.DataTierCold},
		Limit:          100,
	})
	if fed == nil || fed.Plan == nil || !fed.Plan.Routing.UseDuckDB {
		t.Fatalf("warm+cold query did not route to duckdb: %+v", fed)
	}

	// All-tier merge with the dotted attribute in WHERE (column-bound), in a
	// second WHERE leaf (EAV-only), and in ORDER BY — every reader-side
	// injection surface for attribute names.
	env.AssertQueryMatches(ctx, Query{
		Schema:  nested,
		Filters: []Filter{{Attr: "contact.annualIncome", Op: "gte", Value: "200"}},
		Sorts:   []Sort{{Attr: "contact.annualIncome", Desc: true}},
		Limit:   100,
	})
	env.AssertQueryMatches(ctx, Query{
		Schema:  nested,
		Filters: []Filter{{Attr: "contact.note", Op: "starts_with", Value: "note"}},
		Limit:   100,
	})
}
