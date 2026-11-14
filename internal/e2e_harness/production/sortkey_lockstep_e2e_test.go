//go:build e2e

package production

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sortkey"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// lockstepEdgeRows reach the edges of the derivations e2e_wide's attributes
// take through the write funnel: the bound smallint and integer limits, a
// bound bigint past 2^53, bytes that order upper case first, a uuid with
// its high bit set, fractional and negative doubles. Absent attributes are
// NULL, which the comparator sorts last.
func lockstepEdgeRows(wide SchemaRef) []*Event {
	return []*Event{
		CreateEvent(wide, map[string]any{"title": "B", "rank": int64(math.MinInt16), "count": int64(math.MaxInt32),
			"amount": int64(1<<53 + 1), "score": -1.5, "ref": "ffffffff-0000-4000-8000-000000000000", "note": "é",
			"active": true, "level": int64(math.MaxInt16), "qty": int64(math.MinInt32), "total": int64(1<<53 - 1),
			"ratio": 0.1, "token": "00000000-0000-4000-8000-000000000001", "born": "1969-12-31", "seen": "2024-01-02T03:04:05.678Z"}),
		CreateEvent(wide, map[string]any{"title": "a", "rank": int64(math.MaxInt16), "count": int64(math.MinInt32),
			"amount": int64(math.MaxInt64), "score": 9.6, "ref": "7fffffff-0000-4000-8000-000000000000", "note": "",
			"active": false, "level": int64(-1), "qty": int64(0), "total": int64(-(1 << 53) + 1), "ratio": -2.5}),
		CreateEvent(wide, map[string]any{"title": "", "amount": int64(math.MinInt64), "score": 1e300}),
	}
}

// TestSortKeyDerivationLockstep pins design.md §6.1's derivation table end
// to end: rows written through the EntityManager, their hot keys derived in
// Go from the stored values pgx reads (sortkey.DeriveAttribute), then
// flushed. Every parquet value must equal its row's hot key, and a page
// sorted on each attribute, now read from parquet, must come back in the
// order the Go comparator gives the hot keys, both directions.
func TestSortKeyDerivationLockstep(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1]
	events := append(env.GenerateScript(ScriptSpec{Schema: wide, Creates: 9}), lockstepEdgeRows(wide)...)
	mustApplyEvents(ctx, t, env, "lockstep creates", events...)

	cache, ok := env.Metadata.GetSchemaCacheByID(wide.ID)
	if !ok {
		t.Fatalf("no metadata cache for schema %d", wide.ID)
	}
	var attrs []string
	for name, meta := range cache {
		if meta.ValueType != forma.ValueTypeList {
			attrs = append(attrs, name)
		}
	}
	sort.Strings(attrs)

	hot := map[string]map[uuid.UUID]any{}
	for _, attr := range attrs {
		hot[attr] = readHotKeys(ctx, t, env, wide, cache[attr])
	}
	mustFlush(ctx, t, env)

	for _, attr := range attrs {
		typ, err := sortkey.AttributeType(cache[attr])
		if err != nil {
			t.Fatalf("%s: %v", attr, err)
		}
		exported := readParquetKeys(ctx, t, env, wide, attr, typ)
		for _, id := range rowIDs(events) {
			if !sameKey(hot[attr][id], exported[id]) {
				t.Fatalf("%s of %s: hot key %#v, exported %#v", attr, id, hot[attr][id], exported[id])
			}
		}
		for _, desc := range []bool{false, true} {
			assertLockstepOrder(ctx, t, env, wide, attr, desc, hot[attr], rowIDs(events))
		}
	}
}

// readHotKeys derives every row's hot key for one attribute from its storage:
// the bound entity_main column, or the eav_data value column the write
// funnel fills (value_numeric for the numeric family, bool and dates,
// value_text for text and uuid).
func readHotKeys(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, meta forma.AttributeMetadata) map[uuid.UUID]any {
	t.Helper()
	var query string
	var args []any
	if meta.ColumnBinding != nil {
		query = fmt.Sprintf("SELECT ltbase_row_id, %s FROM %s WHERE ltbase_schema_id = $1", meta.ColumnBinding.ColumnName, env.Tables.EntityMain)
		args = []any{schema.ID}
	} else {
		column := "value_numeric"
		if meta.ValueType == forma.ValueTypeText || meta.ValueType == forma.ValueTypeUUID {
			column = "value_text"
		}
		query = fmt.Sprintf("SELECT row_id, %s FROM %s WHERE schema_id = $1 AND attr_id = $2", column, env.Tables.EAVData)
		args = []any{schema.ID, meta.AttributeID}
	}
	rows, err := env.Pool.Query(ctx, query, args...)
	if err != nil {
		t.Fatalf("read hot storage of %s: %v", meta.AttributeName, err)
	}
	defer rows.Close()
	keys := map[uuid.UUID]any{}
	for rows.Next() {
		var id uuid.UUID
		var stored any
		if err := rows.Scan(&id, &stored); err != nil {
			t.Fatalf("scan hot storage of %s: %v", meta.AttributeName, err)
		}
		if n, ok := stored.(pgtype.Numeric); ok && !n.Valid {
			stored = nil
		}
		key, err := sortkey.DeriveAttribute(meta, stored)
		if err != nil {
			t.Fatalf("derive %s of %s from %T: %v", meta.AttributeName, id, stored, err)
		}
		keys[id] = key
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read hot storage of %s: %v", meta.AttributeName, err)
	}
	return keys
}

// readParquetKeys reads one attribute of every exported row at its export
// type, as the parquet leg reads the column.
func readParquetKeys(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, attr string, typ sortkey.ExportType) map[uuid.UUID]any {
	t.Helper()
	keys := schemaParquetKeys(ctx, t, env, schema)
	uris := make([]string, len(keys))
	for i, k := range keys {
		uris[i] = "'" + env.ParquetURI(k) + "'"
	}
	q := fmt.Sprintf("SELECT CAST(row_id AS VARCHAR), CAST(%s AS %s) FROM read_parquet([%s], union_by_name=true)",
		sqlgen.ParquetAttrColumn(attr), typ, strings.Join(uris, ", "))
	rows, err := env.Duck.DB.QueryContext(ctx, q)
	if err != nil {
		t.Fatalf("read parquet %s: %v", attr, err)
	}
	defer func() { _ = rows.Close() }()
	out := map[uuid.UUID]any{}
	for rows.Next() {
		var id string
		var v any
		if err := rows.Scan(&id, &v); err != nil {
			t.Fatalf("scan parquet %s: %v", attr, err)
		}
		switch x := v.(type) {
		case int16:
			v = int64(x)
		case int32:
			v = int64(x)
		}
		out[uuid.MustParse(id)] = v
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read parquet %s: %v", attr, err)
	}
	return out
}

func sameKey(a, b any) bool {
	af, aok := a.(float64)
	bf, bok := b.(float64)
	if aok && bok {
		return math.IsNaN(af) && math.IsNaN(bf) || af == bf && math.Signbit(af) == math.Signbit(bf)
	}
	return a == b
}

// assertLockstepOrder sorts the rows on attr through the engine, with every
// row in parquet, and requires the comparator's order of the hot keys.
func assertLockstepOrder(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, attr string, desc bool, hot map[uuid.UUID]any, ids []uuid.UUID) {
	t.Helper()
	q := Query{Schema: wide, Sorts: []Sort{{Attr: attr, Desc: desc}}, Limit: 100, PreferredTiers: allTiers}
	orders, err := env.buildAttributeOrders(q)
	if err != nil {
		t.Fatalf("%s: %v", attr, err)
	}
	order, err := sortkey.OffsetOrder(orders)
	if err != nil {
		t.Fatalf("%s: %v", attr, err)
	}
	keys := make([][]any, len(ids))
	for i, id := range ids {
		keys[i] = []any{hot[id], id}
	}
	sort.SliceStable(keys, func(a, b int) bool { return order.Compare(keys[a], keys[b]) < 0 })
	want := make([]string, len(keys))
	for i, k := range keys {
		want[i] = k[1].(uuid.UUID).String()
	}
	res := mustQuery(ctx, t, env, q)
	if !res.Plan.Routing.UseDuckDB {
		t.Fatalf("%s: the sorted page must route to DuckDB, got %+v", attr, res.Plan.Routing)
	}
	if got := pageRowIDs(res); !equalStrings(got, want) {
		t.Fatalf("%s desc=%v: engine order %v, comparator order %v", attr, desc, got, want)
	}
}
