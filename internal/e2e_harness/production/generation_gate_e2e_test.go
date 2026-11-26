//go:build e2e

package production

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
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
	page, err := env.DuckDBRoutedEntityManager().Query(ctx, &forma.QueryRequest{
		SchemaName: schema.Name, Page: 1, ItemsPerPage: 100,
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
// stored values (whole values within ±2^53, 1000.5, 1e30, 2^53+2), the active
// generation stays unchanged, no partial activation occurs, and OLTP/federated
// reads continue returning the original stored values.
func TestApplyRefusesNumericToBigint(t *testing.T) {
	cluster := SharedCluster(t)
	for _, tc := range []struct {
		name   string
		values []float64
	}{
		{name: "whole_values_within_exact_int_range", values: []float64{1000, 2000}},
		{name: "fraction_1000_5", values: []float64{1000.5}},
		{name: "overflow_1e30", values: []float64{1e30}},
		{name: "past_2_53_plus_2", values: []float64{float64(int64(1)<<53 + 2)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			verifyNumericToBigintRefusal(t, cluster, tc.name, tc.values)
		})
	}
}

func verifyNumericToBigintRefusal(t *testing.T, cluster *Cluster, caseName string, values []float64) {
	t.Helper()
	ctx := context.Background()
	v1 := qtyDir(t, "numeric", "double_01")
	v2 := qtyDir(t, "bigint", "double_01")
	env := NewEnv(t, cluster, WithSchemaDir(v1))
	simple := DefaultSchemaFixtures()[0]

	beforeGen, ok, err := schemaledger.ReadActive(ctx, env.Pool, env.Tables.SchemaRegistry, simple.ID)
	if err != nil || !ok || beforeGen.Number != 1 {
		t.Fatalf("read initial generation of %s: ok=%t gen=%+v err=%v", simple.Name, ok, beforeGen, err)
	}

	events := make([]*Event, len(values))
	for i, v := range values {
		events[i] = qtyCreate(simple, i, v)
	}
	mustApplyEvents(ctx, t, env, caseName, events...)

	flushRes := mustFlush(ctx, t, env)
	key := requireSoleParquet(t, "flush", flushRes.NewObjects)
	requireParquetCols(t, "numeric delta", describeParquetCols(ctx, t, env, key), map[string]string{"qty": "DOUBLE"})

	err = env.EvolveSchema(ctx, v2)
	if err == nil {
		t.Fatal("numeric to bigint must be refused")
	}
	for _, want := range []string{"e2e_simple", "qty (id 3)", "numeric bound to double_01", "bigint bound to double_01", "narrows", "#730"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not mention %q", err, want)
		}
	}

	assertGenerationUnchangedAndGated(ctx, t, env, simple, beforeGen, v1, v2)
	assertStoredAndReadValues(ctx, t, env, simple, events, values)
}

func assertGenerationUnchangedAndGated(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, beforeGen schemaledger.Generation, v1, v2 string) {
	t.Helper()
	if n := activeGeneration(ctx, t, env, schema); n != 1 {
		t.Fatalf("a refused apply moved the active generation to %d", n)
	}
	afterGen, ok, err := schemaledger.ReadActive(ctx, env.Pool, env.Tables.SchemaRegistry, schema.ID)
	if err != nil || !ok || afterGen.Number != beforeGen.Number || afterGen.Hash != beforeGen.Hash ||
		!slices.Equal(afterGen.Entries, beforeGen.Entries) {
		t.Fatalf("active generation mutated on refusal: before=%+v after=%+v (ok=%t err=%v)", beforeGen, afterGen, ok, err)
	}
	var genCount int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE schema_id = $1", schemaledger.TablesFor(env.Tables.SchemaRegistry).Generation)
	if err := env.Pool.QueryRow(ctx, countQuery, schema.ID).Scan(&genCount); err != nil {
		t.Fatalf("count generations: %v", err)
	}
	if genCount != 1 {
		t.Fatalf("generation table has %d rows after refused apply, want 1", genCount)
	}
	if _, err := schemameta.NewFileSchemaRegistryContext(ctx, env.Pool, env.Tables.SchemaRegistry, v2); !errors.Is(err, schemaledger.ErrNotActive) {
		t.Fatalf("registry on refused declaration v2 = %v, want ErrNotActive", err)
	}
	if _, err := schemameta.NewMetadataLoader(env.Pool, env.Tables.SchemaRegistry, v2).LoadMetadata(ctx); !errors.Is(err, schemaledger.ErrNotActive) {
		t.Fatalf("metadata loader on refused declaration v2 = %v, want ErrNotActive", err)
	}
	if _, err := schemameta.NewFileSchemaRegistryContext(ctx, env.Pool, env.Tables.SchemaRegistry, v1); err != nil {
		t.Fatalf("registry on original declaration v1 failed after refusal: %v", err)
	}
}

func assertStoredAndReadValues(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, events []*Event, values []float64) {
	t.Helper()
	full := env.AssertQueryMatches(ctx, Query{Schema: schema, Limit: 10})
	assertUsesDuckDB(t, full)
	if full != nil && full.Total != int64(len(values)) {
		t.Fatalf("read under the old declaration returned %d rows, want %d", full.Total, len(values))
	}
	colQuery := fmt.Sprintf("SELECT double_01 FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2", env.Tables.EntityMain)
	for i, ev := range events {
		want := values[i]
		oltp, federated := readQtyBothRoutes(ctx, t, env, schema, ev.RowID)
		oltpF, okOLTP := oltp.(float64)
		fedF, okFed := federated.(float64)
		if !okOLTP || !okFed || oltpF != want || fedF != want {
			t.Fatalf("row %s qty reads %v (%T, OLTP) and %v (%T, federated), want exact float64 %v", ev.RowID, oltp, oltp, federated, federated, want)
		}
		var stored float64
		if err := env.Pool.QueryRow(ctx, colQuery, schema.ID, ev.RowID).Scan(&stored); err != nil {
			t.Fatalf("query stored double_01 for row %s: %v", ev.RowID, err)
		}
		if stored != want {
			t.Fatalf("stored double_01 for row %s = %v, want %v", ev.RowID, stored, want)
		}
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
