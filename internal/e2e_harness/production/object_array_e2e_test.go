//go:build e2e

package production

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
)

// objectArrayProps declares e2e_simple with items, an array of objects.
const objectArrayProps = `{
    "name": { "type": "string" },
    "value": { "type": "number" },
    "items": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "sku": { "type": "string" },
          "qty": { "type": "number" }
        }
      }
    }
  }`

const objectArrayAttrs = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "value": { "attributeID": 2, "valueType": "numeric" },
  "items.sku": { "attributeID": 3, "valueType": "text" },
  "items.qty": { "attributeID": 4, "valueType": "numeric" }
}
`

// TestObjectArraySchemaReadsEveryElementOnTheFederatedRoute pins #704 on
// PostgreSQL + DuckDB. The parquet tiers hold one value per member of an
// array of objects, so a flush of such a schema used to collapse a row's
// two items into one, and a federated read answered that collapsed shape and
// missed a filter on the smaller element. The flush now leaves the schema's
// rows unflushed, and every federated read of it, an explicit warm/cold
// declaration included, is answered from PostgreSQL with both elements and
// the hot_tier_only marker.
func TestObjectArraySchemaReadsEveryElementOnTheFederatedRoute(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(writeSimpleSchemaDir(t, objectArrayProps, objectArrayAttrs)))
	simple := DefaultSchemaFixtures()[0]
	row := CreateEvent(simple, map[string]any{"name": "two-items", "value": float64(1), "items": []any{
		map[string]any{"sku": "a", "qty": float64(1)},
		map[string]any{"sku": "b", "qty": float64(2)},
	}})
	mustApplyEvents(ctx, t, env, "row with two items", row)

	flush, err := env.RunFlush(ctx)
	if err != nil {
		t.Fatalf("a flush must skip a schema it cannot export, not fail: %v", err)
	}

	stored, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: simple.Name, RowID: &row.RowID})
	if err != nil {
		t.Fatalf("OLTP get: %v", err)
	}
	if items, _ := stored.Attributes["items"].([]any); len(items) != 2 {
		t.Fatalf("OLTP get reads items %v, want both elements", stored.Attributes["items"])
	}
	for _, tiers := range [][]string{nil, {"hot", "warm", "cold"}, {"warm", "cold"}} {
		res, err := env.EntityManager().Query(ctx, &forma.QueryRequest{
			SchemaName: simple.Name, Page: 1, ItemsPerPage: 10,
			Condition: &forma.KvCondition{Attr: "items.sku", Value: "equals:a"},
			Federated: &forma.FederatedQueryRequest{Enabled: true, PreferredTiers: tiers, IncludeExecutionPlan: true},
		})
		if err != nil {
			t.Fatalf("federated query with preferred_tiers %v: %v", tiers, err)
		}
		answered := make([]map[string]any, 0, len(res.Data))
		for _, rec := range res.Data {
			answered = append(answered, rec.Attributes)
		}
		if len(res.Data) != 1 || !reflect.DeepEqual(res.Data[0].Attributes["items"], stored.Attributes["items"]) {
			t.Fatalf("federated query with preferred_tiers %v answered %v, want the row with items %v",
				tiers, answered, stored.Attributes["items"])
		}
		assertObjectArrayRoute(t, tiers, res)
	}

	if flush.UnflushedAfter != flush.UnflushedBefore || len(flush.NewObjects) != 0 {
		t.Fatalf("flush marked %d of %d rows flushed and wrote %v, want the schema left unflushed and nothing written",
			flush.UnflushedBefore-flush.UnflushedAfter, flush.UnflushedBefore, flush.NewObjects)
	}
}

// assertObjectArrayRoute: res was answered from PostgreSQL alone, says why
// in its plan, and marks the tiers it did not read.
func assertObjectArrayRoute(t *testing.T, tiers []string, res *forma.QueryResult) {
	t.Helper()
	plan := res.ExecutionPlan
	if plan == nil || plan.Routing.UsedDuckDB || !strings.Contains(plan.Routing.Reason, `array of objects "items"`) {
		t.Fatalf("preferred_tiers %v: plan %+v, want a Postgres-only route naming the array of objects", tiers, plan)
	}
	want := []string{"warm", "cold"}
	if res.Partial == nil || res.Partial.Reason != forma.PartialReasonHotTierOnly ||
		!reflect.DeepEqual(res.Partial.UnconsultedTiers, want) {
		t.Fatalf("preferred_tiers %v: partial %+v, want hot_tier_only with unconsulted %v", tiers, res.Partial, want)
	}
}
