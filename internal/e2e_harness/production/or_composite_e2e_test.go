//go:build e2e

package production

import (
	"context"
	"testing"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// TestORCompositeSchemaScoping (#269): a top-level multi-branch OR condition
// on the PostgreSQL optimized-query path must stay scoped to the requested
// schema. The template joins the anchor condition with
// `schema_id = $1 AND <condition>`; if the OR renders unparenthesized, every
// branch after the first admits rows from OTHER schemas into the anchor.
// Final records survive (main_data re-filters by schema), but
// `COUNT(*) OVER()` counts the polluted anchor — total_records inflates and
// LIMIT/OFFSET paginates over foreign row_ids.
//
// Fixture geometry: e2e_second.code and e2e_wide.count are both bound to
// entity_main.integer_01, so a wide row with count=777 is exactly the foreign
// row an unscoped `integer_01 = 777` branch would admit.
func TestORCompositeSchemaScoping(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()

	fixtures := DefaultSchemaFixtures()
	wide, second := fixtures[1], fixtures[2]

	seedCrossSchemaIntegerCollision(ctx, t, env, wide, second)

	// Positive control: the pollution rows are really present and matchable
	// in their own schema — a broken seed cannot fake the probe green.
	control := env.AssertQueryMatches(ctx, Query{
		Schema:    wide,
		Filters:   []Filter{{Attr: "count", Value: "777"}},
		Limit:     10,
		PreferHot: true,
	})
	if control == nil {
		return
	}
	if control.Total != 5 {
		t.Fatalf("positive control: e2e_wide count=777 total %d, want 5", control.Total)
	}

	page := queryHotTopLevelOR(ctx, t, env, second, "code", "111", "777")

	// Exactly one e2e_second row matches (code=111, since 777 exists only in
	// e2e_wide). A polluted anchor reports total_records=6 (1 + 5 foreign).
	if len(page.Records) != 1 {
		t.Errorf("OR query returned %d records, want 1", len(page.Records))
	}
	for _, rec := range page.Records {
		if rec.SchemaID != second.ID {
			t.Errorf("OR query returned record from schema %d, want only %d", rec.SchemaID, second.ID)
		}
	}
	if page.TotalRecords != 1 {
		t.Errorf("OR query total_records = %d, want 1 (anchor polluted by foreign-schema rows, #269)", page.TotalRecords)
	}
}

// seedCrossSchemaIntegerCollision seeds the #269 pollution geometry: three
// target-schema rows (exactly one matching the probe's first OR branch, none
// matching the second) plus five e2e_wide rows whose integer_01 (count)
// equals the second branch's value — the rows an unscoped branch would admit
// into the target schema's anchor.
func seedCrossSchemaIntegerCollision(ctx context.Context, t *testing.T, env *Env, wide, second SchemaRef) {
	t.Helper()

	mustApplyEvents(ctx, t, env, "seed e2e_second",
		CreateEvent(second, map[string]any{"label": "match", "code": 111}),
		CreateEvent(second, map[string]any{"label": "other-a", "code": 222}),
		CreateEvent(second, map[string]any{"label": "other-b", "code": 333}),
	)

	wideCreates := make([]*Event, 0, 5)
	for i := 0; i < 5; i++ {
		wideCreates = append(wideCreates, CreateEvent(wide, map[string]any{
			"title": "pollution",
			"count": 777,
		}))
	}
	mustApplyEvents(ctx, t, env, "seed e2e_wide pollution rows", wideCreates...)
}

// queryHotTopLevelOR runs `attr = v1 OR attr = v2` as a top-level OR against
// the real engine and returns the page. The harness Query spec only builds
// AND composites, so the federated query is constructed directly (same shape
// Env.buildFederatedQuery emits). PreferHot pins the hot-only OLTP route
// through the optimized PG template; a DuckDB-routing guard keeps the probe
// on the #269 surface.
func queryHotTopLevelOR(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, attr, v1, v2 string) *model.PersistentRecordPage {
	t.Helper()

	fq := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			SchemaID: schema.ID,
			Condition: &forma.CompositeCondition{
				Logic: forma.LogicOr,
				Conditions: []forma.Condition{
					&forma.KvCondition{Attr: attr, Value: "equals:" + v1},
					&forma.KvCondition{Attr: attr, Value: "equals:" + v2},
				},
			},
			Limit: 10,
		},
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
		PreferHot:      true,
	}
	opts := &model.FederatedQueryOptions{
		IncludeExecutionPlan: true,
		ExecutionPlan:        &model.ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}},
	}
	page, err := env.Engine().Query(ctx, env.Tables, fq, opts)
	if err != nil {
		t.Fatalf("top-level OR query on %s: %v", schema.Name, err)
	}
	if opts.ExecutionPlan.Routing.UseDuckDB {
		t.Fatalf("OR probe unexpectedly routed to DuckDB; the #269 surface is the PG optimized path: %+v", opts.ExecutionPlan.Routing)
	}
	return page
}
