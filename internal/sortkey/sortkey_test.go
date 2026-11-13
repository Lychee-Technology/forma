package sortkey

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

var (
	idLow  = uuid.MustParse("00000000-0000-0000-0000-000000000001")
	idMid  = uuid.MustParse("7fffffff-0000-0000-0000-000000000000")
	idHigh = uuid.MustParse("ffffffff-0000-0000-0000-000000000000")
)

// The comparator puts NULL last in both directions, and reverses only the
// non-NULL order for a descending key.
func TestCompareNullsLastBothDirections(t *testing.T) {
	for _, desc := range []bool{false, true} {
		o := Order{Columns: []Column{{Name: "n", Type: BigInt, Desc: desc}, {Name: ColumnRowID, Type: UUID}}}
		require.Equal(t, 1, o.Compare([]any{nil, idLow}, []any{int64(5), idLow}), "desc=%v: NULL sorts after a value", desc)
		require.Equal(t, -1, o.Compare([]any{int64(5), idLow}, []any{nil, idLow}), "desc=%v", desc)
		require.Equal(t, -1, o.Compare([]any{nil, idLow}, []any{nil, idHigh}), "desc=%v: NULLs tie, row_id decides", desc)
	}
	asc := Order{Columns: []Column{{Name: "n", Type: BigInt}}}
	desc := Order{Columns: []Column{{Name: "n", Type: BigInt, Desc: true}}}
	require.Equal(t, -1, asc.Compare([]any{int64(1)}, []any{int64(2)}))
	require.Equal(t, 1, desc.Compare([]any{int64(1)}, []any{int64(2)}))
}

// CompareDouble is DuckDB's order: NaN equal to itself and above +Inf, -0
// equal to 0. TestDuckDBDoubleOrder checks the same against the engine.
func TestCompareDouble(t *testing.T) {
	nan, inf := math.NaN(), math.Inf(1)
	require.Equal(t, 0, CompareDouble(nan, nan))
	require.Equal(t, 1, CompareDouble(nan, inf))
	require.Equal(t, -1, CompareDouble(inf, nan))
	require.Equal(t, 0, CompareDouble(math.Copysign(0, -1), 0))
	require.Equal(t, -1, CompareDouble(math.Inf(-1), -math.MaxFloat64))
}

// Strings compare by bytes, uuids by their 16 bytes, booleans false first.
func TestCompareValueOrders(t *testing.T) {
	require.Equal(t, -1, compareValues(Varchar, "B", "a"), "byte order puts upper case first")
	require.Equal(t, -1, compareValues(Varchar, "z", "é"))
	require.Equal(t, -1, compareValues(UUID, idMid, idHigh), "the high bit is not a sign")
	require.Equal(t, -1, compareValues(Boolean, false, true))
}

// The predicate is three-valued: a NULL key never satisfies its arm, even
// though the comparator sorts it after every boundary.
func TestAdmitsIsThreeValued(t *testing.T) {
	o := Order{Columns: []Column{{Name: "n", Type: BigInt}, {Name: ColumnRowID, Type: UUID}}}
	bounds := []any{int64(5), idMid}
	require.True(t, o.Admits([]any{int64(6), idLow}, bounds), "past the boundary on the first key")
	require.True(t, o.Admits([]any{int64(5), idHigh}, bounds), "tied, past it on row_id")
	require.False(t, o.Admits([]any{int64(5), idMid}, bounds), "the boundary row itself")
	require.False(t, o.Admits([]any{int64(4), idHigh}, bounds))
	require.False(t, o.Admits([]any{nil, idHigh}, bounds), "NULL is unknown, not after")
	require.Equal(t, 1, o.Compare([]any{nil, idHigh}, []any{int64(5), idMid}), "yet the comparator sorts it last")

	desc := Order{Columns: []Column{{Name: "n", Type: BigInt, Desc: true}, {Name: ColumnRowID, Type: UUID, Desc: true}}}
	require.True(t, desc.Admits([]any{int64(5), idLow}, bounds), "row_id DESC admits the lower row_ids of the tie")
	require.False(t, desc.Admits([]any{int64(5), idHigh}, bounds))
	require.True(t, desc.Admits([]any{int64(4), idHigh}, bounds))
}

// deleted_ts reads NULL as 0 in the predicate and in the order, as the
// renderer's COALESCE does.
func TestDeletedTSReadsZero(t *testing.T) {
	cursor := &model.KeysetCursor{
		Columns: []model.KeysetColumn{
			{Attribute: "deleted_ts", Direction: forma.SortOrderAsc, ExportType: "BIGINT"},
			{Attribute: "row_id", Direction: forma.SortOrderAsc, ExportType: "UUID"},
		},
		Values: []any{int64(0), idMid},
		Mode:   model.KeysetCursorModeAfter,
	}
	o, err := KeysetOrder(cursor)
	require.NoError(t, err)
	require.True(t, o.Admits([]any{nil, idHigh}, cursor.Values), "a NULL row after the cursor's row_id")
	require.False(t, o.Admits([]any{nil, idLow}, cursor.Values))
	require.True(t, o.Admits([]any{nil, idLow}, []any{int64(-1), idHigh}), "ascending below 0 admits every row")
	require.False(t, o.Admits([]any{nil, idHigh}, []any{int64(1), idLow}), "ascending above 0 admits none")
	require.Equal(t, -1, o.Compare([]any{nil, idLow}, []any{int64(0), idMid}), "row_id orders the NULL and 0 rows together")

	key, err := DeriveSystem("deleted_ts", int64(17))
	require.NoError(t, err)
	require.Equal(t, int64(0), key, "the hot key is the constant 0")
}

// The offset order mirrors buildNonKeysetOrderBy, the keyset order the
// cursor, row_id direction included.
func TestPageOrders(t *testing.T) {
	def, err := OffsetOrder(nil)
	require.NoError(t, err)
	require.Equal(t, []Column{{Name: "created_at", Type: BigInt, Desc: true}, {Name: "row_id", Type: UUID}}, def.Columns)

	orders := []model.AttributeOrder{
		{AttrName: "count", ValueType: forma.ValueTypeInteger, SortOrder: forma.SortOrderDesc,
			StorageLocation: forma.AttributeStorageLocationMain, ColumnName: "integer_01"},
		{AttrName: "qty", ValueType: forma.ValueTypeInteger, StorageLocation: forma.AttributeStorageLocationEAV},
	}
	o, err := OffsetOrder(orders)
	require.NoError(t, err)
	require.Equal(t, []Column{
		{Name: "count", Type: Integer, Desc: true},
		{Name: "qty", Type: Double},
		{Name: "row_id", Type: UUID},
	}, o.Columns)

	_, err = OffsetOrder([]model.AttributeOrder{{AttrName: "tags", ValueType: forma.ValueTypeList}})
	require.ErrorContains(t, err, "is a list")

	_, err = KeysetOrder(&model.KeysetCursor{Columns: []model.KeysetColumn{{Attribute: "row_id"}}, Values: []any{idLow}})
	require.ErrorContains(t, err, "not normalized")
}

// normalizeCache holds one attribute of every export type.
func normalizeCache() forma.SchemaAttributeCache {
	bound := func(col string) *forma.MainColumnBinding {
		return &forma.MainColumnBinding{ColumnName: forma.MainColumn(col)}
	}
	return forma.SchemaAttributeCache{
		"big":   {AttributeName: "big", ValueType: forma.ValueTypeBigInt},
		"int":   {AttributeName: "int", ValueType: forma.ValueTypeInteger, ColumnBinding: bound("integer_01")},
		"small": {AttributeName: "small", ValueType: forma.ValueTypeSmallInt, ColumnBinding: bound("smallint_01")},
		"num":   {AttributeName: "num", ValueType: forma.ValueTypeNumeric},
		"text":  {AttributeName: "text", ValueType: forma.ValueTypeText},
		"ref":   {AttributeName: "ref", ValueType: forma.ValueTypeUUID},
		"flag":  {AttributeName: "flag", ValueType: forma.ValueTypeBool},
		"day":   {AttributeName: "day", ValueType: forma.ValueTypeDate},
		"tags":  {AttributeName: "tags", ValueType: forma.ValueTypeList},
	}
}

func cursorOn(attr string, v any) *model.KeysetCursor {
	return &model.KeysetCursor{
		Columns: []model.KeysetColumn{{Attribute: attr}, {Attribute: "row_id"}},
		Values:  []any{v, idMid.String()},
		Mode:    model.KeysetCursorModeAfter,
	}
}

// One boundary type per cursor column: the accepted boundaries normalize to
// the export type's Go type, everything else is refused, never converted.
func TestNormalizeCursorBoundaries(t *testing.T) {
	text := "x"
	cases := []struct {
		attr string
		in   any
		want any // nil: refused
	}{
		{"big", int64(9007199254740993), int64(9007199254740993)},
		{"big", int32(7), int64(7)},
		{"big", uint8(7), int64(7)},
		{"big", json.Number("9223372036854775807"), int64(math.MaxInt64)},
		{"big", uint64(math.MaxUint64), nil},
		{"big", 9.6, nil},
		{"big", 10.0, nil},
		{"big", json.Number("10.0"), nil},
		{"big", "10", nil},
		{"big", time.UnixMilli(5), nil},
		{"day", time.UnixMilli(5), nil},
		{"day", int64(5), int64(5)},
		{"created_at", int64(5), int64(5)},
		{"deleted_ts", 0, int64(0)},
		{"int", int64(math.MaxInt32), int64(math.MaxInt32)},
		{"int", int64(math.MaxInt32) + 1, nil},
		{"small", int64(-32769), nil},
		{"small", int16(-32768), int64(-32768)},
		{"num", 9.6, 9.6},
		{"num", float32(0.5), 0.5},
		{"num", int64(1 << 53), float64(1 << 53)},
		{"num", int64(1<<53 + 1), nil},
		{"num", int64(math.MaxInt64), nil},
		{"num", int64(math.MinInt64), float64(math.MinInt64)},
		{"num", json.Number("9.6"), 9.6},
		{"num", json.Number("1e3"), 1000.0},
		{"num", json.Number("9007199254740993"), nil},
		{"num", json.Number("9223372036854775808"), math.Ldexp(1, 63)},
		{"num", json.Number("9223372036854775809"), nil},
		{"num", json.Number("-9223372036854775809"), nil},
		{"num", json.Number("18446744073709551615"), nil},
		{"num", json.Number("18446744073709551616"), math.Ldexp(1, 64)},
		{"num", uint64(1) << 63, math.Ldexp(1, 63)},
		{"num", uint64(1<<63 + 1), nil},
		{"num", uint64(math.MaxUint64), nil},
		{"big", json.Number("9223372036854775808"), nil},
		{"big", json.Number("-9223372036854775808"), int64(math.MinInt64)},
		{"num", "9.6", nil},
		{"text", "abc", "abc"},
		{"text", &text, "x"},
		{"text", 5, nil},
		{"ref", "6BA7B810-9DAD-11D1-80B4-00C04FD430C8", "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		{"ref", "not-a-uuid", nil},
		{"flag", true, true},
		{"flag", 1, nil},
		{"row_id", "FFFFFFFF-0000-0000-0000-000000000000", idHigh},
		{"row_id", [16]byte(idLow), idLow},
		{"row_id", 5, nil},
	}
	for _, tc := range cases {
		var cursor *model.KeysetCursor
		if tc.attr == "row_id" {
			cursor = &model.KeysetCursor{Columns: []model.KeysetColumn{{Attribute: "row_id"}}, Values: []any{tc.in}, Mode: model.KeysetCursorModeAfter}
		} else {
			cursor = cursorOn(tc.attr, tc.in)
		}
		got, err := NormalizeCursor(cursor, nil, normalizeCache())
		if tc.want == nil {
			require.Error(t, err, "%s <- %T(%v) must be refused", tc.attr, tc.in, tc.in)
			continue
		}
		require.NoError(t, err, "%s <- %T(%v)", tc.attr, tc.in, tc.in)
		require.Equal(t, tc.want, got.Values[0], "%s <- %T(%v)", tc.attr, tc.in, tc.in)
		again, err := NormalizeCursor(got, nil, normalizeCache())
		require.NoError(t, err, "normalizing twice")
		require.Equal(t, got, again, "normalization is idempotent")
	}
	nan, err := NormalizeCursor(cursorOn("num", math.NaN()), nil, normalizeCache())
	require.NoError(t, err)
	require.True(t, math.IsNaN(nan.Values[0].(float64)), "a NaN boundary stays NaN, which DuckDB orders")
}

// The column's export type is recorded on the cursor; a column the schema
// does not know, and a list, are refused; the request's sort key resolves a
// column the metadata lacks.
func TestNormalizeCursorTypes(t *testing.T) {
	got, err := NormalizeCursor(cursorOn("int", int64(3)), nil, normalizeCache())
	require.NoError(t, err)
	require.Equal(t, "INTEGER", got.Columns[0].ExportType)
	require.Equal(t, "UUID", got.Columns[1].ExportType)
	require.Equal(t, idMid, got.Values[1])

	_, err = NormalizeCursor(cursorOn("missing", int64(3)), nil, normalizeCache())
	require.ErrorContains(t, err, "neither a system column")
	_, err = NormalizeCursor(cursorOn("tags", "a"), nil, normalizeCache())
	require.ErrorContains(t, err, "is a list")

	orders := []model.AttributeOrder{{AttrName: "missing", ValueType: forma.ValueTypeBigInt, StorageLocation: forma.AttributeStorageLocationEAV}}
	got, err = NormalizeCursor(cursorOn("missing", int64(3)), orders, nil)
	require.NoError(t, err)
	require.Equal(t, "BIGINT", got.Columns[0].ExportType)

	stale := cursorOn("int", int64(3))
	stale.Columns[0].ExportType = "BIGINT"
	_, err = NormalizeCursor(stale, nil, normalizeCache())
	require.ErrorContains(t, err, "carries export type BIGINT")

	inactive := &model.KeysetCursor{}
	same, err := NormalizeCursor(inactive, nil, nil)
	require.NoError(t, err)
	require.Same(t, inactive, same)
}

// A cursor-only attribute column resolves as DuckDB binds it: the attribute
// whose parquet column it reaches, ASCII case aside. The caller's spelling
// is kept on the cursor, which the renderer emits.
func TestNormalizeCursorFoldsAttributeCase(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"count":        {AttributeName: "count", ValueType: forma.ValueTypeInteger, ColumnBinding: &forma.MainColumnBinding{ColumnName: "integer_01"}},
		"contact.name": {AttributeName: "contact.name", ValueType: forma.ValueTypeText},
		"Count":        {AttributeName: "Count", ValueType: forma.ValueTypeNumeric},
	}
	got, err := NormalizeCursor(cursorOn("Count", 1.5), nil, cache)
	require.NoError(t, err, "an exact name wins over a case variant")
	require.Equal(t, "DOUBLE", got.Columns[0].ExportType)

	delete(cache, "Count")
	got, err = NormalizeCursor(cursorOn("COUNT", int64(3)), nil, cache)
	require.NoError(t, err)
	require.Equal(t, "COUNT", got.Columns[0].Attribute)
	require.Equal(t, "INTEGER", got.Columns[0].ExportType)
	require.Equal(t, int64(3), got.Values[0])
	_, err = NormalizeCursor(cursorOn("COUNT", 9.6), nil, cache)
	require.ErrorContains(t, err, "exported as INTEGER", "the case variant is held to the attribute's type")

	got, err = NormalizeCursor(cursorOn("Contact.NAME", "x"), nil, cache)
	require.NoError(t, err)
	require.Equal(t, "VARCHAR", got.Columns[0].ExportType)

	_, err = NormalizeCursor(cursorOn("coünt", int64(3)), nil, cache)
	require.ErrorContains(t, err, "neither a system column", "the fold is ASCII only")

	cache["A_B"] = forma.AttributeMetadata{ValueType: forma.ValueTypeText}
	cache["a.b"] = forma.AttributeMetadata{ValueType: forma.ValueTypeBigInt}
	_, err = NormalizeCursor(cursorOn("a_b", int64(3)), nil, cache)
	require.ErrorContains(t, err, "no single export type")
}

// System keys derive from their stored values; deleted_ts is always 0.
func TestDeriveSystem(t *testing.T) {
	got, err := DeriveSystem("row_id", pgtype.UUID{Bytes: idHigh, Valid: true})
	require.NoError(t, err)
	require.Equal(t, idHigh, got)
	got, err = DeriveSystem("ROW_ID", pgtype.UUID{})
	require.NoError(t, err)
	require.Nil(t, got)
	got, err = DeriveSystem("created_at", int64(42))
	require.NoError(t, err)
	require.Equal(t, int64(42), got)
	got, err = DeriveSystem("deleted_ts", nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), got)
	_, err = DeriveSystem("updated_at", int64(1))
	require.Error(t, err)
	_, err = DeriveSystem("ver_ts", "1")
	require.Error(t, err)
}

// An input of a kind the attribute's storage cannot hold is an error, and a
// list attribute has no derivation.
func TestDeriveAttributeRefusesForeignInputs(t *testing.T) {
	_, err := DeriveAttribute(forma.AttributeMetadata{AttributeName: "n", ValueType: forma.ValueTypeBigInt}, "12")
	require.Error(t, err)
	_, err = DeriveAttribute(forma.AttributeMetadata{AttributeName: "t", ValueType: forma.ValueTypeText}, int64(1))
	require.Error(t, err)
	_, err = DeriveAttribute(forma.AttributeMetadata{AttributeName: "l", ValueType: forma.ValueTypeList}, "a")
	require.ErrorContains(t, err, "is a list")
	got, err := DeriveAttribute(forma.AttributeMetadata{AttributeName: "n", ValueType: forma.ValueTypeBigInt}, pgtype.Numeric{})
	require.NoError(t, err)
	require.Nil(t, got, "a NULL value_numeric derives NULL")
}

// CheckKey and CheckBounds hold a tuple to its order's Go types.
func TestCheckKey(t *testing.T) {
	o := Order{Columns: []Column{{Name: "n", Type: Integer}, {Name: ColumnRowID, Type: UUID}}}
	require.NoError(t, o.CheckKey([]any{nil, idLow}))
	require.Error(t, o.CheckKey([]any{int32(1), idLow}))
	require.Error(t, o.CheckKey([]any{int64(1)}))
	require.Error(t, o.CheckBounds([]any{nil, idLow}))
	require.NoError(t, o.CheckBounds([]any{int64(1), idLow}))
}
