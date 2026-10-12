package internal

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/testdb"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL truth table for the #646 operators on the OLTP route,
// over both storage kinds (entity_main columns and eav_data rows) and both
// value_numeric types (the temp tables' DOUBLE PRECISION and production's
// NUMERIC).

const dslSchemaID int16 = 1

func dslSchemaCache() forma.SchemaAttributeCache {
	bind := func(col string, enc forma.MainColumnEncoding) *forma.MainColumnBinding {
		return &forma.MainColumnBinding{ColumnName: forma.MainColumn(col), Encoding: enc}
	}
	return forma.SchemaAttributeCache{
		"code":      {AttributeID: 1, ValueType: forma.ValueTypeText, ColumnBinding: bind("text_01", "")},
		"note":      {AttributeID: 2, ValueType: forma.ValueTypeText},
		"rank":      {AttributeID: 3, ValueType: forma.ValueTypeInteger, ColumnBinding: bind("integer_01", "")},
		"weight":    {AttributeID: 4, ValueType: forma.ValueTypeNumeric},
		"big":       {AttributeID: 5, ValueType: forma.ValueTypeBigInt},
		"flag":      {AttributeID: 6, ValueType: forma.ValueTypeBool, ColumnBinding: bind("smallint_01", forma.MainColumnEncodingBoolInt)},
		"flag_text": {AttributeID: 7, ValueType: forma.ValueTypeBool, ColumnBinding: bind("text_02", forma.MainColumnEncodingBoolText)},
		"soft":      {AttributeID: 8, ValueType: forma.ValueTypeBool},
		"due":       {AttributeID: 9, ValueType: forma.ValueTypeDateTime, ColumnBinding: bind("bigint_01", forma.MainColumnEncodingUnixMs)},
		"due_iso":   {AttributeID: 10, ValueType: forma.ValueTypeDateTime, ColumnBinding: bind("text_03", forma.MainColumnEncodingISO8601)},
		"seen":      {AttributeID: 11, ValueType: forma.ValueTypeDateTime},
		"day":       {AttributeID: 12, ValueType: forma.ValueTypeDate},
		"ref":       {AttributeID: 13, ValueType: forma.ValueTypeUUID},
		"owner":     {AttributeID: 14, ValueType: forma.ValueTypeUUID, ColumnBinding: bind("uuid_01", "")},
		"labels":    {AttributeID: 15, ValueType: forma.ValueTypeList},
		"other":     {AttributeID: 16, ValueType: forma.ValueTypeText},
		"day_main":  {AttributeID: 17, ValueType: forma.ValueTypeDate, ColumnBinding: bind("bigint_02", forma.MainColumnEncodingUnixMs)},
		"day_iso":   {AttributeID: 18, ValueType: forma.ValueTypeDate, ColumnBinding: bind("text_04", forma.MainColumnEncodingISO8601)},
	}
}

type dslFixture struct {
	ctx    context.Context
	pool   *pgxpool.Pool
	repo   *DBPersistentRecordRepository
	tables model.StorageTables
	cache  forma.SchemaAttributeCache
}

func newDSLFixture(t *testing.T, numericEAV bool) *dslFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	t.Cleanup(cancel)
	pool := testdb.Connect(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)
	if numericEAV {
		_, err := pool.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN value_numeric TYPE NUMERIC", sanitizeIdentifier(tables.EAVData)))
		require.NoError(t, err)
	}
	cache := dslSchemaCache()
	metadata := schemameta.NewMetadataCache()
	require.NoError(t, metadata.RegisterSchema("dsl646", dslSchemaID, cache))
	return &dslFixture{ctx: ctx, pool: pool, repo: NewDBPersistentRecordRepository(pool, metadata), tables: tables, cache: cache}
}

// seed inserts one entity: main holds entity_main column values, eav holds
// (attribute, value_text, value_numeric, array_indices) rows.
func (f *dslFixture) seed(t *testing.T, main map[string]any, eav ...dslEAVRow) uuid.UUID {
	t.Helper()
	rowID := uuid.New()
	cols := []string{"ltbase_schema_id", "ltbase_row_id", "ltbase_created_at", "ltbase_updated_at"}
	args := []any{dslSchemaID, rowID, int64(1), int64(1)}
	for col, v := range main {
		cols = append(cols, col)
		args = append(args, v)
	}
	phs := make([]string, len(args))
	for i := range args {
		phs[i] = fmt.Sprintf("$%d", i+1)
	}
	_, err := f.pool.Exec(f.ctx, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		sanitizeIdentifier(f.tables.EntityMain), strings.Join(cols, ", "), strings.Join(phs, ", ")), args...)
	require.NoError(t, err)
	for _, r := range eav {
		_, err := f.pool.Exec(f.ctx, fmt.Sprintf(
			"INSERT INTO %s (schema_id, row_id, attr_id, array_indices, value_text, value_numeric) VALUES ($1, $2, $3, $4, $5, $6)",
			sanitizeIdentifier(f.tables.EAVData)),
			dslSchemaID, rowID, f.cache[r.attr].AttributeID, r.index, r.text, r.numeric)
		require.NoError(t, err)
	}
	return rowID
}

type dslEAVRow struct {
	attr    string
	index   string
	text    any
	numeric any
}

func eavText(attr, v string) dslEAVRow        { return dslEAVRow{attr: attr, text: v} }
func eavNum(attr string, v float64) dslEAVRow { return dslEAVRow{attr: attr, numeric: v} }

// eavInt seeds an exact integer: a float64 seed would store the 2^53
// neighbour of a large bigint in a NUMERIC column.
func eavInt(attr string, v int64) dslEAVRow { return dslEAVRow{attr: attr, numeric: v} }

// match runs the condition through the repository and returns the matching
// row ids, sorted.
func (f *dslFixture) match(t *testing.T, cond forma.Condition) []uuid.UUID {
	t.Helper()
	page, err := f.repo.QueryPersistentRecords(f.ctx, &model.PersistentRecordQuery{
		Tables: f.tables, SchemaID: dslSchemaID, Condition: cond, Limit: 100,
	})
	require.NoError(t, err, "%+v", cond)
	ids := make([]uuid.UUID, 0, len(page.Records))
	for _, r := range page.Records {
		ids = append(ids, r.RowID)
	}
	return sortedIDs(ids...)
}

func sortedIDs(ids ...uuid.UUID) []uuid.UUID {
	out := append([]uuid.UUID{}, ids...)
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

func kv(attr, value string) *forma.KvCondition { return &forma.KvCondition{Attr: attr, Value: value} }

func TestConditionDSLExtendedTruthTable(t *testing.T) {
	for _, numeric := range []bool{false, true} {
		t.Run(fmt.Sprintf("numeric_eav=%v", numeric), func(t *testing.T) {
			runDSLTruthTable(t, newDSLFixture(t, numeric))
		})
	}
}

func runDSLTruthTable(t *testing.T, f *dslFixture) {
	u1, u2 := "6f1c3c3e-1111-4111-8111-111111111111", "6f1c3c3e-2222-4222-8222-222222222222"
	r1 := f.seed(t, map[string]any{
		"text_01": "a", "integer_01": 1, "smallint_01": 1, "text_02": "1",
		"bigint_01": int64(1704164645000), "text_03": "2024-01-02T03:04:05Z", "uuid_01": u1,
	}, eavText("note", "x"), eavNum("weight", 1.5), eavInt("big", 9007199254740993), eavNum("soft", 1),
		eavNum("seen", 1704164645000), eavNum("day", 1704153600000), eavText("ref", u1),
		dslEAVRow{attr: "labels", index: "0", text: "p"}, dslEAVRow{attr: "labels", index: "1", text: "q"})
	r2 := f.seed(t, map[string]any{
		"text_01": "b", "integer_01": 2, "smallint_01": 0, "text_02": "0",
		"bigint_01": int64(1704164646000), "text_03": "2024-01-02T03:04:06Z", "uuid_01": u2,
	}, eavText("note", "y"), eavNum("weight", 2.5), eavInt("big", 2), eavNum("soft", 0),
		eavNum("seen", 1704164646000), eavNum("day", 1704240000000), eavText("ref", u2),
		dslEAVRow{attr: "labels", index: ""}) // the empty-list marker row
	// r3 has no eav_data row at all: only the entity_main anchor visits it.
	r3 := f.seed(t, map[string]any{})
	// r4 is visible to an EAV anchor through an unrelated attribute.
	r4 := f.seed(t, map[string]any{}, eavText("other", "z"))

	all, present, absent := sortedIDs(r1, r2, r3, r4), sortedIDs(r1, r2), sortedIDs(r3, r4)
	attrs := []struct{ attr, v1, v2 string }{
		{"code", `"a"`, `"b"`},
		{"note", `"x"`, `"y"`},
		{"rank", "1", "2"},
		{"weight", "1.5", "2.5"},
		{"big", "9007199254740993", "2"},
		{"flag", "true", "false"},
		{"flag_text", "true", "false"},
		{"soft", "1", "0"},
		{"due", `"2024-01-02T03:04:05Z"`, "1704164646000"},
		{"due_iso", "1704164645000", `"2024-01-02T03:04:06Z"`},
		{"seen", `"2024-01-02T03:04:05Z"`, "1704164646000"},
		{"day", `"2024-01-02T00:00:00Z"`, "1704240000000"},
		{"ref", `"` + strings.ToUpper(u1) + `"`, `"` + u2 + `"`},
		{"owner", `"` + strings.ToUpper(u1) + `"`, `"` + u2 + `"`},
	}
	for _, a := range attrs {
		t.Run(a.attr, func(t *testing.T) {
			require.Equal(t, sortedIDs(r1), f.match(t, kv(a.attr, "in:["+a.v1+"]")))
			require.Equal(t, present, f.match(t, kv(a.attr, "in:["+a.v1+","+a.v2+","+a.v1+"]")), "duplicates are allowed")
			require.Equal(t, sortedIDs(r2), f.match(t, kv(a.attr, "not_in:["+a.v1+"]")), "not_in never matches an absent value")
			require.Equal(t, absent, f.match(t, kv(a.attr, "is_null:true")))
			require.Equal(t, present, f.match(t, kv(a.attr, "is_not_null:true")))
			require.Equal(t, all, f.match(t, &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: []forma.Condition{
				kv(a.attr, "in:["+a.v1+"]"), kv(a.attr, "not_in:["+a.v1+"]"), kv(a.attr, "is_null:true"),
			}}), "in, not_in and is_null partition every entity")
		})
	}

	t.Run("list attribute presence counts the empty-list marker", func(t *testing.T) {
		require.Equal(t, absent, f.match(t, kv("labels", "is_null:true")))
		require.Equal(t, present, f.match(t, kv("labels", "is_not_null:true")))
	})
	t.Run("integer list drops fractional elements", func(t *testing.T) {
		require.Equal(t, sortedIDs(r2), f.match(t, kv("rank", "in:[1.5, 2]")))
		require.Equal(t, sortedIDs(r2), f.match(t, kv("big", "not_in:[9007199254740993, 2.5]")))
		require.Empty(t, f.match(t, kv("rank", "in:[1.5]")))
		require.Equal(t, present, f.match(t, kv("rank", "not_in:[1.5]")))
	})
	t.Run("composites mix the new operators with the old", func(t *testing.T) {
		require.Equal(t, sortedIDs(r1, r3, r4), f.match(t, &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: []forma.Condition{
			kv("code", "equals:a"), kv("note", "is_null:true"),
		}}))
		require.Equal(t, sortedIDs(r2), f.match(t, &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
			kv("note", `in:["x","y"]`), kv("weight", "gt:2"),
		}}))
	})
}

func TestConditionDSLExtendedRefusals(t *testing.T) {
	f := newDSLFixture(t, true)
	f.seed(t, map[string]any{"text_01": "a"})
	oversized := "in:[" + strings.Repeat(`"a",`, 1000) + `"a"]`
	cases := map[string]string{
		"in:[]":                    "operator 'in' on attribute 'code' takes a JSON array of 1 to 1000 values; the array is empty",
		oversized:                  "operator 'in' on attribute 'code' takes a JSON array of 1 to 1000 values; the array has more than 1000 values",
		"not_in:[null]":            "element at index 0 of the 'not_in' list for attribute 'code' is null",
		"is_null:false":            "operator 'is_null' on attribute 'code' takes only the value true",
		`in:["a", {"b": 1}]`:       "element at index 1 of the 'in' list for attribute 'code' is a JSON object",
		`in:"a"`:                   "the value is not a JSON array",
		`in:["a"] trailing`:        "the value has data after the JSON array",
		`not_in:["a"],["b"]`:       "the value has data after the JSON array",
		`is_not_null:TRUE`:         `takes only the value true, got "TRUE"`,
		`in:["2024-01-02", "a"x]`:  "the value is not a well-formed JSON array",
		`not_in:[1, "two", false]`: "", // text elements accept any literal
	}
	for value, want := range cases {
		_, err := f.repo.QueryPersistentRecords(f.ctx, &model.PersistentRecordQuery{
			Tables: f.tables, SchemaID: dslSchemaID, Condition: kv("code", value), Limit: 10,
		})
		if want == "" {
			require.NoError(t, err, value)
			continue
		}
		require.ErrorIs(t, err, forma.ErrInvalidInput, value)
		msg, ok := forma.ResolvePublicMessage(err)
		require.True(t, ok, value)
		require.Contains(t, msg, want, value)
	}

	_, err := f.repo.QueryPersistentRecords(f.ctx, &model.PersistentRecordQuery{
		Tables: f.tables, SchemaID: dslSchemaID, Condition: kv("labels", `in:["p"]`), Limit: 10,
	})
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.Contains(t, err.Error(), "operator 'in' is not supported on list attribute 'labels'")
}
