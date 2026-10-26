//go:build e2e

package production

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"github.com/lychee-technology/forma/internal/schemameta"
)

// #733 acceptance on the PostgreSQL + DuckDB harness: Env.EvolveSchema
// records the new declaration the way apply-schema does, so a change the D2
// rules refuse fails there and the Env keeps running on the old one.

// ledgerQtyAttrs declares e2e_simple with qty (id 3) of valueType, bound to
// column, or EAV-only when column is empty.
func ledgerQtyAttrs(valueType, column string) string {
	binding := ""
	if column != "" {
		binding = fmt.Sprintf(`, "column_binding": { "col_name": "%s" }`, column)
	}
	return fmt.Sprintf(`{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "value": { "attributeID": 2, "valueType": "numeric" },
  "qty": { "attributeID": 3, "valueType": "%s"%s }
}
`, valueType, binding)
}

func qtyDir(t *testing.T, valueType, column string) string {
	t.Helper()
	return writeSimpleSchemaDir(t, qtyProps, ledgerQtyAttrs(valueType, column))
}

func qtyCreate(schema SchemaRef, ordinal int, qty any) *Event {
	return CreateEvent(schema, map[string]any{"name": fmt.Sprintf("row-%04d", ordinal), "value": float64(ordinal), "qty": qty})
}

func activeGeneration(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) int64 {
	t.Helper()
	n, ok, err := schemaledger.ReadPointer(ctx, env.Pool, env.Tables.SchemaRegistry, schema.ID)
	if err != nil || !ok {
		t.Fatalf("read the active generation of %s: ok=%t err=%v", schema.Name, ok, err)
	}
	return n
}

// readQtyBothRoutes reads rowID's qty through the public EntityManager on
// the OLTP route (Get) and on the federated route (a DuckDB-routed query).
func readQtyBothRoutes(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, rowID uuid.UUID) (oltp, federated any) {
	t.Helper()
	got, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: schema.Name, RowID: &rowID})
	if err != nil {
		t.Fatalf("get %s: %v", rowID, err)
	}
	page, err := env.EntityManager().Query(ctx, &forma.QueryRequest{
		SchemaName: schema.Name, Page: 1, ItemsPerPage: 100,
		Federated: &forma.FederatedQueryRequest{Enabled: true, PreferredTiers: []string{"hot", "warm", "cold"}},
	})
	if err != nil {
		t.Fatalf("federated query: %v", err)
	}
	for _, rec := range page.Data {
		if rec.RowID == rowID {
			return got.Attributes["qty"], rec.Attributes["qty"]
		}
	}
	t.Fatalf("federated query did not return row %s", rowID)
	return nil, nil
}

// TestApplyRefusesNumericToBigint: a narrowing is refused whatever the
// stored values (every qty here is whole), the active generation stays, and
// reads keep working under the old declaration.
func TestApplyRefusesNumericToBigint(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(qtyDir(t, "numeric", "double_01")))
	simple := DefaultSchemaFixtures()[0]
	mustApplyEvents(ctx, t, env, "whole qtys", qtyCreate(simple, 0, float64(1000)), qtyCreate(simple, 1, float64(2000)))
	mustFlush(ctx, t, env)

	err := env.EvolveSchema(ctx, qtyDir(t, "bigint", "double_01"))
	if err == nil {
		t.Fatal("numeric to bigint must be refused")
	}
	for _, want := range []string{"e2e_simple", "qty (id 3)", "numeric bound to double_01", "bigint bound to double_01", "narrows", "#730"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not mention %q", err, want)
		}
	}
	if n := activeGeneration(ctx, t, env, simple); n != 1 {
		t.Fatalf("a refused apply moved the active generation to %d", n)
	}
	full := env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 10})
	assertUsesDuckDB(t, full)
	if full != nil && full.Total != 2 {
		t.Fatalf("read under the old declaration returned %d rows, want 2", full.Total)
	}
}

// TestApplyAllowsWideningAndRefusesTheOldDirectory: smallint to integer and
// bigint to numeric (double_*) apply, and reads answer unchanged on every
// tier; a process restarted on the old SCHEMA_DIR then refuses to start.
func TestApplyAllowsWideningAndRefusesTheOldDirectory(t *testing.T) {
	ctx := context.Background()
	const attrs = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "value": { "attributeID": 2, "valueType": "numeric" },
  "qty": { "attributeID": 3, "valueType": "%s", "column_binding": { "col_name": "smallint_01" } },
  "amount": { "attributeID": 4, "valueType": "%s", "column_binding": { "col_name": "double_01" } }
}
`
	const props = `{
    "name": { "type": "string" },
    "value": { "type": "number" },
    "qty": { "type": "number" },
    "amount": { "type": "number" }
  }`
	v1 := writeSimpleSchemaDir(t, props, fmt.Sprintf(attrs, "smallint", "bigint"))
	v2 := writeSimpleSchemaDir(t, props, fmt.Sprintf(attrs, "integer", "numeric"))
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(v1))
	simple := DefaultSchemaFixtures()[0]
	row := func(i int) *Event {
		return CreateEvent(simple, map[string]any{"name": fmt.Sprintf("row-%04d", i), "value": float64(i),
			"qty": float64(i * 100), "amount": float64(i) * 1e12})
	}
	mustApplyEvents(ctx, t, env, "v1 rows", row(1), row(2), row(3))
	mustFlush(ctx, t, env)
	mustApplyEvents(ctx, t, env, "v1 hot row", row(4))

	if err := env.EvolveSchema(ctx, v2); err != nil {
		t.Fatalf("smallint to integer and bigint to numeric (double_01) must apply: %v", err)
	}
	if n := activeGeneration(ctx, t, env, simple); n != 2 {
		t.Fatalf("active generation = %d, want 2", n)
	}
	for _, q := range []Query{{Schema: simple, Limit: 10}, {Schema: simple, Limit: 10, PreferHot: true},
		{Schema: simple, Filters: []Filter{{Attr: "amount", Op: "gte", Value: "2e12"}}, Limit: 10}} {
		env.AssertQueryMatches(ctx, q)
	}

	_, err := schemameta.NewFileSchemaRegistryContext(ctx, env.Pool, env.Tables.SchemaRegistry, v1)
	if !errors.Is(err, schemaledger.ErrNotActive) || !strings.Contains(err.Error(), "qty (id 3) is declared as smallint") {
		t.Fatalf("a registry on the old SCHEMA_DIR = %v, want ErrNotActive naming qty", err)
	}
	_, err = schemameta.NewMetadataLoader(env.Pool, env.Tables.SchemaRegistry, v1).LoadMetadata(ctx)
	if !errors.Is(err, schemaledger.ErrNotActive) {
		t.Fatalf("a metadata loader on the old SCHEMA_DIR = %v, want ErrNotActive", err)
	}
}

// TestApplyWarnsOnBigintToNumericInABigintColumn: the change applies with
// the warning, stored on the generation, and both routes read 2^53+1 as the
// same rounded float64, 2^53, and MaxInt64 as 2^63, which no int64 holds.
func TestApplyWarnsOnBigintToNumericInABigintColumn(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(qtyDir(t, "bigint", "bigint_01")))
	simple := DefaultSchemaFixtures()[0]
	past := qtyCreate(simple, 0, int64(1)<<53+1)
	top := qtyCreate(simple, 1, int64(math.MaxInt64))
	mustApplyEvents(ctx, t, env, "qty past 2^53", past, top)
	mustFlush(ctx, t, env) // the federated route now reads the delta object

	if err := env.EvolveSchema(ctx, qtyDir(t, "numeric", "bigint_01")); err != nil {
		t.Fatalf("bigint to numeric (bigint_01) must apply: %v", err)
	}
	var warnings string
	query := fmt.Sprintf("SELECT warnings FROM %s WHERE schema_id = $1 AND generation = 2",
		schemaledger.TablesFor(env.Tables.SchemaRegistry).Generation)
	if err := env.Pool.QueryRow(ctx, query, simple.ID).Scan(&warnings); err != nil {
		t.Fatalf("read generation 2's warnings: %v", err)
	}
	if !strings.Contains(warnings, "bigint_01") || !strings.Contains(warnings, "rounded") {
		t.Fatalf("generation 2 stores warnings %q, want the bigint_01 rounding warning", warnings)
	}

	for _, tc := range []struct {
		event *Event
		want  float64
	}{
		{past, float64(int64(1) << 53)},
		{top, 0x1p63},
	} {
		oltp, federated := readQtyBothRoutes(ctx, t, env, simple, tc.event.RowID)
		if fmt.Sprint(oltp) != fmt.Sprint(tc.want) || fmt.Sprint(federated) != fmt.Sprint(tc.want) {
			t.Fatalf("qty reads %v (OLTP) and %v (federated), want both rounded to %v", oltp, federated, tc.want)
		}
	}
}

// TestApplyKeepsSafeEvolutionsWorking: a rename recorded as a removal plus
// an addition under a new id applies, and reads match across generations.
// Adding, removing and EAV-to-bound promotion go through the same gated
// EvolveSchema in TestSchemaEvolutionAddedColumn, RemovedColumn and
// EAVToBoundPromotion.
func TestApplyKeepsSafeEvolutionsWorking(t *testing.T) {
	ctx := context.Background()
	const renamed = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "value": { "attributeID": 2, "valueType": "numeric" },
  "label": { "attributeID": 5, "valueType": "text" },
  "points": { "attributeID": 6, "valueType": "integer" }
}
`
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(writeSimpleSchemaDir(t, scoreIntProps, scoreIntAttrs)))
	simple := DefaultSchemaFixtures()[0]
	seedEvolutionTiers(ctx, t, env, simple, writeSimpleSchemaDir(t, pointsIntProps, renamed),
		buildEvolutionProfile(buildLabeledExtras(func(ordinal int) map[string]any {
			return map[string]any{"score": float64(ordinal * 10)}
		})),
		buildEvolutionProfile(buildLabeledExtras(func(ordinal int) map[string]any {
			return map[string]any{"points": float64(ordinal * 10)}
		})))
	gen, ok, err := schemaledger.ReadActive(ctx, env.Pool, env.Tables.SchemaRegistry, simple.ID)
	if err != nil || !ok || gen.Number != 2 {
		t.Fatalf("active generation = %+v (%v), want 2", gen, err)
	}
	for _, e := range gen.Entries {
		if e.AttrID == 3 && !e.Retired {
			t.Fatalf("score (id 3) is recorded as %+v, want it kept as retired", e)
		}
	}
	full := env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 20})
	if full != nil && full.Total != 12 {
		t.Fatalf("full scan total = %d, want 12", full.Total)
	}
	env.AssertQueryMatches(ctx, Query{Schema: simple, Filters: []Filter{{Attr: "points", Op: "gte", Value: "50"}}, Limit: 20})
}
