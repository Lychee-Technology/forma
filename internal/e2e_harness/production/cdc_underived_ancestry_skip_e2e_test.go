//go:build e2e

package production

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/cdc"
)

// underivedAncestryCase is an e2e_simple generation that declares the array
// of objects items beside a construct whose ancestors the schema registry
// cannot place, so it derives no array ancestry for the schema (#937).
type underivedAncestryCase struct {
	construct string // the property beside items
	unplaced  string // the attribute the registry cannot place
	value     any    // what the row stores under construct
}

var underivedAncestryCases = map[string]underivedAncestryCase{
	"oneOf union": {
		construct: `{"oneOf": [
        {"type": "object", "properties": {"kind": {"type": "string"}}},
        {"type": "array", "items": {"type": "object", "properties": {"kind": {"type": "string"}}}}
      ]}`,
		unplaced: "extra.kind",
		value:    map[string]any{"kind": "k"},
	},
	"nested array": {
		construct: `{"type": "array", "items": {"type": "array", "items": {"type": "object", "properties": {"kind": {"type": "string"}}}}}`,
		unplaced:  "extra.kind",
		value:     []any{[]any{map[string]any{"kind": "k"}}},
	},
	"cross-file $ref": {
		construct: `{"$ref": "e2e_nested.json#/properties/contact"}`,
		unplaced:  "extra.note",
		value:     map[string]any{"note": "n"},
	},
}

// schemaDir writes the generation: name, items[] of {sku} and extra.
func (c underivedAncestryCase) schemaDir(t *testing.T) string {
	t.Helper()
	props := fmt.Sprintf(`{
    "name": { "type": "string" },
    "items": { "type": "array", "items": { "type": "object", "properties": { "sku": { "type": "string" } } } },
    "extra": %s
  }`, c.construct)
	attrs := fmt.Sprintf(`{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "items.sku": { "attributeID": 2, "valueType": "text" },
  %q: { "attributeID": 3, "valueType": "text" }
}
`, c.unplaced)
	return writeSimpleSchemaDir(t, props, attrs)
}

// seed builds the case's Env with one e2e_simple row holding two items and
// one ordinary e2e_second row, and returns the e2e_simple row.
func (c underivedAncestryCase) seed(ctx context.Context, t *testing.T) (*Env, *Event) {
	t.Helper()
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(c.schemaDir(t)))
	fixtures := DefaultSchemaFixtures()
	row := CreateEvent(fixtures[0], map[string]any{"name": "two-items", "extra": c.value, "items": []any{
		map[string]any{"sku": "a"},
		map[string]any{"sku": "b"},
	}})
	mustApplyEvents(ctx, t, env, "underived-ancestry rows", row,
		CreateEvent(fixtures[2], map[string]any{"label": "flat", "code": 1}))
	return env, row
}

// assertUnderivedSkipped: skipped names schema alone, for the attribute the
// registry could not place.
func (c underivedAncestryCase) assertSkipped(t *testing.T, skipped []cdc.SkippedSchema, schema SchemaRef) {
	t.Helper()
	want := `could not place attribute "` + c.unplaced + `"`
	if len(skipped) != 1 || skipped[0].SchemaID != schema.ID || !strings.Contains(skipped[0].Reason, want) {
		t.Fatalf("skipped schemas %+v, want schema %d skipped with %s", skipped, schema.ID, want)
	}
}

// TestFlushSkipsASchemaWhoseArrayAncestryWasNotDerived pins #937 on a real
// flush pass. Before it, each construct left the schema's ancestry
// underived, the guard read that as "no array of objects", and the flush
// exported items collapsed to one value per row and marked the row flushed.
// Now the flush skips the schema and reports the unplaced attribute, the row
// stays unflushed with nothing written for it, and its neighbour flushes.
// Get and Query, on the deployments whose routing would otherwise use
// DuckDB included, answer both elements from PostgreSQL alone.
func TestFlushSkipsASchemaWhoseArrayAncestryWasNotDerived(t *testing.T) {
	ctx := context.Background()
	for name, c := range underivedAncestryCases {
		t.Run(name, func(t *testing.T) {
			env, row := c.seed(ctx, t)
			fixtures := DefaultSchemaFixtures()
			simple, flat := fixtures[0], fixtures[2]

			flush, err := env.RunFlush(ctx)
			if err != nil {
				t.Fatalf("a flush must skip a schema it cannot export, not fail: %v", err)
			}
			c.assertSkipped(t, flush.Skipped, simple)
			if n := unflushedRows(ctx, t, env, simple); n != 1 {
				t.Fatalf("skipped schema has %d unflushed rows, want its 1 row left unflushed", n)
			}
			if n := unflushedRows(ctx, t, env, flat); n != 0 {
				t.Fatalf("ordinary schema has %d unflushed rows, want it flushed", n)
			}
			finals, _ := splitKeys(flush.NewObjects)
			manifests, err := env.loadManifests(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if m := manifests[simple.ID]; m != nil && len(m.Files) != 0 {
				t.Fatalf("skipped schema's manifest lists %+v, want nothing exported", m.Files)
			}
			assertManifestDeltaPaths(t, manifests, flat, finals)

			c.assertReadsEveryElement(ctx, t, env, simple, row)
		})
	}
}

// assertReadsEveryElement: Get answers both items, and every federated
// deployment answers the row whole from PostgreSQL alone, saying why.
func (c underivedAncestryCase) assertReadsEveryElement(ctx context.Context, t *testing.T, env *Env, simple SchemaRef, row *Event) {
	t.Helper()
	stored, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: simple.Name, RowID: &row.RowID})
	if err != nil {
		t.Fatalf("OLTP get: %v", err)
	}
	if items, _ := stored.Attributes["items"].([]any); len(items) != 2 {
		t.Fatalf("OLTP get reads items %v, want both elements", stored.Attributes["items"])
	}
	managers := map[string]forma.EntityManager{
		"hybrid routing": env.RoutedEntityManager(forma.RoutingStrategyHybrid),
		"duckdb-routed":  env.DuckDBRoutedEntityManager(),
	}
	for name, manager := range managers {
		res, err := manager.Query(ctx, &forma.QueryRequest{
			SchemaName: simple.Name, Page: 1, ItemsPerPage: 10,
			Condition:            &forma.KvCondition{Attr: "items.sku", Value: "equals:a"},
			IncludeExecutionPlan: true,
		})
		if err != nil {
			t.Fatalf("federated query on the %s deployment: %v", name, err)
		}
		if len(res.Data) != 1 || !reflect.DeepEqual(res.Data[0].Attributes, stored.Attributes) {
			t.Fatalf("federated query on the %s deployment answered %d rows %+v, want the row as Get reads it: %v",
				name, len(res.Data), res.Data, stored.Attributes)
		}
		plan := res.ExecutionPlan
		if plan == nil || plan.Routing.UsedDuckDB || !strings.Contains(plan.Routing.Reason, `could not place attribute "`+c.unplaced+`"`) {
			t.Fatalf("%s deployment: plan %+v, want a Postgres-only route naming %s", name, plan, c.unplaced)
		}
	}
}

// TestCDCInitSkipsOrRefusesASchemaWhoseArrayAncestryWasNotDerived is the
// cdc-init leg: a run whose --schema-id names the schema fails with
// ErrSchemaNotExportable and writes nothing, and a run over every schema
// skips and reports it, initializes the others, and leaves its row
// unflushed.
func TestCDCInitSkipsOrRefusesASchemaWhoseArrayAncestryWasNotDerived(t *testing.T) {
	ctx := context.Background()
	for name, c := range underivedAncestryCases {
		t.Run(name, func(t *testing.T) {
			env, _ := c.seed(ctx, t)
			fixtures := DefaultSchemaFixtures()
			simple, flat := fixtures[0], fixtures[2]

			keysBefore, err := env.listS3Keys(ctx)
			if err != nil {
				t.Fatal(err)
			}
			_, err = env.RunInit(ctx, simple)
			if !errors.Is(err, cdc.ErrSchemaNotExportable) || !strings.Contains(err.Error(), c.unplaced) {
				t.Fatalf("cdc-init naming the schema: err %v, want ErrSchemaNotExportable naming %s", err, c.unplaced)
			}
			keysAfter, err := env.listS3Keys(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if added := diffKeys(keysBefore, keysAfter); len(added) != 0 {
				t.Fatalf("a refused cdc-init wrote %v, want nothing", added)
			}

			report, err := env.RunInitWith(ctx, flat, InitOverrides{AllSchemas: true})
			if err != nil {
				t.Fatalf("cdc-init over every schema must skip, not fail: %v", err)
			}
			c.assertSkipped(t, report.Skipped, simple)
			manifests, err := env.loadManifests(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if m := manifests[simple.ID]; m != nil && len(m.Files) != 0 {
				t.Fatalf("skipped schema's manifest lists %+v, want nothing exported", m.Files)
			}
			if m := manifests[flat.ID]; m == nil || len(m.Files) == 0 {
				t.Fatalf("ordinary schema %d was not initialized: manifest %+v", flat.ID, m)
			}
			if n := unflushedRows(ctx, t, env, simple); n != 1 {
				t.Fatalf("skipped schema has %d unflushed rows, want its 1 row left unflushed", n)
			}
		})
	}
}
