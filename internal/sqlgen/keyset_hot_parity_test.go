package sqlgen_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sortkey"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// The hot leg's Go copy of the keyset predicate and the order (sortkey) must
// admit and order rows as the SQL the shared keyset renderer emits does on
// DuckDB (design.md §6.1, pgx-hot-leg-plan.md step 2, #853). These tests
// run the rendered SQL over an in-memory table standing in for the visible
// CTE, whose columns carry the export types.

func openParityDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "?lambda_syntax=DISABLE_SINGLE_ARROW")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// parityRowID gives row i a uuid whose leading byte spreads across the
// signed boundary, so byte order and a signed reading disagree.
func parityRowID(i int) uuid.UUID {
	var u uuid.UUID
	u[0] = byte((i * 83) % 256)
	u[15] = byte(i)
	return u
}

func bindValue(v any) any {
	if u, ok := v.(uuid.UUID); ok {
		return u.String()
	}
	return v
}

// parityColumn is one export type under test: the attribute's metadata, its
// table column, and the key values of its rows (nil is NULL).
type parityColumn struct {
	attr string
	meta forma.AttributeMetadata
	typ  string
	keys []any
}

func parityColumns() []parityColumn {
	bound := func(col string) *forma.MainColumnBinding {
		return &forma.MainColumnBinding{ColumnName: forma.MainColumn(col)}
	}
	ids := []any{parityRowID(1).String(), parityRowID(40).String(), parityRowID(99).String()}
	return []parityColumn{
		{"k_big", forma.AttributeMetadata{ValueType: forma.ValueTypeBigInt}, "BIGINT", []any{
			int64(math.MinInt64), int64(-1), int64(0), int64(9), int64(10), int64(10), int64(1<<53 - 1),
			int64(1 << 53), int64(1<<53 + 1), int64(math.MaxInt64), nil}},
		{"k_int", forma.AttributeMetadata{ValueType: forma.ValueTypeInteger, ColumnBinding: bound("integer_01")}, "INTEGER", []any{
			int64(math.MinInt32), int64(-1), int64(0), int64(10), int64(math.MaxInt32), nil}},
		{"k_small", forma.AttributeMetadata{ValueType: forma.ValueTypeSmallInt, ColumnBinding: bound("smallint_01")}, "SMALLINT", []any{
			int64(math.MinInt16), int64(0), int64(10), int64(math.MaxInt16), nil}},
		{"k_num", forma.AttributeMetadata{ValueType: forma.ValueTypeNumeric}, "DOUBLE", []any{
			math.Inf(-1), -1.5, math.Copysign(0, -1), 0.0, 9.6, 10.0, float64(1 << 53), float64(1<<53 + 2),
			math.Ldexp(1, 63), math.Nextafter(math.Ldexp(1, 63), math.Inf(1)), math.Ldexp(1, 64),
			math.Inf(1), math.NaN(), nil}},
		{"k_text", forma.AttributeMetadata{ValueType: forma.ValueTypeText}, "VARCHAR", []any{
			"", "10", "B", "a", "abc", "é", nil}},
		{"k_ref", forma.AttributeMetadata{ValueType: forma.ValueTypeUUID}, "VARCHAR", append(ids, nil)},
		{"k_flag", forma.AttributeMetadata{ValueType: forma.ValueTypeBool}, "BOOLEAN", []any{false, true, nil}},
		{"deleted_ts", forma.AttributeMetadata{}, "BIGINT", []any{int64(0), nil, int64(0), nil}},
		{"created_at", forma.AttributeMetadata{}, "BIGINT", []any{int64(-1), int64(0), int64(1700000000000)}},
	}
}

// parityBoundaries is a boundary of every Go type a caller can put in a
// cursor, at the edges the plan names: a float 9.6 and whole floats,
// integers at 2^53 - 1, 2^53 and 2^53 + 1 and at the BIGINT limits, a NaN,
// and upper-case uuid text. Integers past int64, as a uint64 and as an
// integral json.Number, are held to the same exactness rule: 2^63 and 2^64
// have a double image, 2^63 + 1 and 2^64 - 1 do not.
func parityBoundaries() []any {
	return []any{
		int(10), int8(-1), int16(math.MaxInt16), int32(math.MinInt32), int64(1<<53 - 1), int64(1 << 53),
		int64(1<<53 + 1), int64(math.MinInt64), int64(math.MaxInt64), uint(10), uint8(1), uint16(math.MaxUint16),
		uint32(math.MaxUint32), uint64(math.MaxUint64), float32(9.5), 9.6, 10.0, -1.0, float64(1 << 53),
		math.NaN(), math.Inf(1), math.Copysign(0, -1), json.Number("10"), json.Number("9.6"),
		uint64(1) << 63, uint64(1<<63 + 1), json.Number("9223372036854775808"), json.Number("9223372036854775809"),
		json.Number("18446744073709551615"), json.Number("18446744073709551616"), json.Number("-9223372036854775809"),
		"10", "B", "abc", strings.ToUpper(parityRowID(40).String()), parityRowID(40).String(), true, false,
		parityRowID(40), [16]byte(parityRowID(40)), time.UnixMilli(10), []byte("10"),
	}
}

// TestKeysetCursorParity: every boundary is either refused before either leg
// runs, or the rendered DuckDB predicate and the Go predicate admit the same
// rows, for every export type, both directions, with the boundary row_id in
// the middle of the table.
func TestKeysetCursorParity(t *testing.T) {
	db := openParityDB(t)
	cache := forma.SchemaAttributeCache{}
	for _, c := range parityColumns() {
		if c.meta.ValueType != "" {
			c.meta.AttributeName = c.attr
			cache[c.attr] = c.meta
		}
	}
	for _, c := range parityColumns() {
		table := loadParityTable(t, db, c)
		accepted := 0
		for _, dir := range []forma.SortOrder{forma.SortOrderAsc, forma.SortOrderDesc} {
			for _, b := range parityBoundaries() {
				cursor := &model.KeysetCursor{
					Columns: []model.KeysetColumn{{Attribute: c.attr, Direction: dir}, {Attribute: "row_id", Direction: dir}},
					Values:  []any{b, parityRowID(len(c.keys) / 2).String()},
					Mode:    model.KeysetCursorModeAfter,
				}
				normalized, err := sortkey.NormalizeCursor(cursor, nil, cache)
				if err != nil {
					continue
				}
				accepted++
				label := fmt.Sprintf("%s %s at %T(%v)", c.attr, dir, b, b)
				require.Equal(t, goAdmitted(t, normalized, table), duckAdmitted(t, db, normalized), label)
			}
		}
		require.NotZero(t, accepted, "%s: some boundary must be accepted", c.attr)
	}
	rowIDOnly(t, db)
}

// rowIDOnly runs the parity over a cursor on row_id alone.
func rowIDOnly(t *testing.T, db *sql.DB) {
	t.Helper()
	table := loadParityTable(t, db, parityColumn{attr: "created_at", typ: "BIGINT", keys: make([]any, 12)})
	for _, dir := range []forma.SortOrder{forma.SortOrderAsc, forma.SortOrderDesc} {
		for _, b := range parityBoundaries() {
			cursor := &model.KeysetCursor{Columns: []model.KeysetColumn{{Attribute: "row_id", Direction: dir}}, Values: []any{b}, Mode: model.KeysetCursorModeAfter}
			normalized, err := sortkey.NormalizeCursor(cursor, nil, nil)
			if err != nil {
				continue
			}
			require.Equal(t, goAdmitted(t, normalized, table), duckAdmitted(t, db, normalized), "row_id %s at %T(%v)", dir, b, b)
		}
	}
}

// parityTable is the rows loaded for one column: each row's key tuple (the
// column, then row_id) by row_id text.
type parityTable map[string][]any

// loadParityTable replaces the table v with one row per key: row_id and
// the column under test, at its export type. Every other column the
// renderer could name is absent, so a cursor on it fails at the binder.
func loadParityTable(t *testing.T, db *sql.DB, c parityColumn) parityTable {
	t.Helper()
	_, err := db.Exec(fmt.Sprintf("CREATE OR REPLACE TABLE v (row_id UUID, %s %s)", c.attr, c.typ))
	require.NoError(t, err)
	table := parityTable{}
	for i, k := range c.keys {
		id := parityRowID(i)
		_, err := db.Exec(fmt.Sprintf("INSERT INTO v VALUES (CAST(? AS UUID), CAST(? AS %s))", c.typ), id.String(), bindValue(k))
		require.NoError(t, err)
		table[id.String()] = []any{k, id}
	}
	return table
}

func goAdmitted(t *testing.T, cursor *model.KeysetCursor, table parityTable) []string {
	t.Helper()
	order, err := sortkey.KeysetOrder(cursor)
	require.NoError(t, err)
	require.NoError(t, order.CheckBounds(cursor.Values))
	var ids []string
	for id, key := range table {
		if len(cursor.Columns) == 1 {
			key = key[1:]
		}
		if order.Admits(key, cursor.Values) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func duckAdmitted(t *testing.T, db *sql.DB, cursor *model.KeysetCursor) []string {
	t.Helper()
	clause, args, err := sqlgen.GenerateKeysetWhereClause(cursor)
	require.NoError(t, err)
	return queryIDs(t, db, "SELECT CAST(row_id AS VARCHAR) FROM v WHERE "+clause+" ORDER BY 1", args...)
}

func queryIDs(t *testing.T, db *sql.DB, query string, args ...any) []string {
	t.Helper()
	rows, err := db.Query(query, args...)
	require.NoError(t, err, query)
	defer func() { _ = rows.Close() }()
	ids := []string{}
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	if len(ids) == 0 {
		return nil
	}
	return ids
}

// The renderer refuses a cursor that has not been normalized, and a type it
// would have to interpolate that is not an export type.
func TestKeysetRendererRefusesAnUnnormalizedCursor(t *testing.T) {
	cursor := &model.KeysetCursor{Columns: []model.KeysetColumn{{Attribute: "row_id"}}, Values: []any{"x"}, Mode: model.KeysetCursorModeAfter}
	_, _, err := sqlgen.GenerateKeysetWhereClause(cursor)
	require.ErrorContains(t, err, "the cursor was not normalized")

	cursor.Columns[0].ExportType = "UUID) OR (1=1"
	_, _, err = sqlgen.GenerateKeysetWhereClause(cursor)
	require.ErrorContains(t, err, "the cursor was not normalized")

	cursor.Columns[0].ExportType = "UUID"
	clause, args, err := sqlgen.GenerateKeysetWhereClause(cursor)
	require.NoError(t, err)
	require.Equal(t, "(row_id > CAST(? AS UUID))", clause)
	require.Equal(t, []any{"x"}, args)
}
