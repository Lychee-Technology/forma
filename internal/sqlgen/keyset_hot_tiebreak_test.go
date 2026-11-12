package sqlgen_test

import (
	"database/sql"
	"fmt"
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sortkey"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// tieRow is one row of the tiebreak fixture: created_at, an INTEGER
// attribute k_int (EAV-only, so DOUBLE on DuckDB), deleted_ts in either live
// encoding, and row_id.
type tieRow struct {
	created any
	kInt    any
	deleted any
	id      uuid.UUID
}

// tieRows is a run of rows that tie on created_at, k_int and deleted_ts,
// with row_ids on both sides of the signed boundary, beside rows that sort
// before and after the run and a run tied on a NULL k_int.
func tieRows() []tieRow {
	rows := []tieRow{
		{int64(99), 1.0, int64(0), parityRowID(3)},
		{int64(101), 1.0, nil, parityRowID(4)},
	}
	for i := 10; i < 22; i++ {
		var deleted any
		if i%2 == 0 {
			deleted = int64(0)
		}
		rows = append(rows, tieRow{int64(100), 5.0, deleted, parityRowID(i)})
	}
	for i := 30; i < 34; i++ {
		rows = append(rows, tieRow{int64(100), nil, nil, parityRowID(i)})
	}
	return rows
}

func loadTieRows(t *testing.T, db *sql.DB) []tieRow {
	t.Helper()
	_, err := db.Exec("CREATE OR REPLACE TABLE v (row_id UUID, created_at BIGINT, k_int DOUBLE, deleted_ts BIGINT)")
	require.NoError(t, err)
	rows := tieRows()
	for _, r := range rows {
		_, err := db.Exec("INSERT INTO v VALUES (CAST(? AS UUID), ?, ?, ?)", r.id.String(), r.created, r.kInt, r.deleted)
		require.NoError(t, err)
	}
	return rows
}

// keyOf reads a row's key tuple for an order's columns.
func keyOf(r tieRow, order sortkey.Order) []any {
	key := make([]any, len(order.Columns))
	for i, c := range order.Columns {
		switch c.Name {
		case "created_at":
			key[i] = r.created
		case "k_int":
			key[i] = r.kInt
		case "deleted_ts":
			key[i] = r.deleted
		case "row_id":
			key[i] = r.id
		}
	}
	return key
}

// goOrder sorts the rows with the comparator and returns their row_ids.
func goOrder(rows []tieRow, order sortkey.Order) []string {
	sorted := append([]tieRow(nil), rows...)
	sort.SliceStable(sorted, func(a, b int) bool {
		return order.Compare(keyOf(sorted[a], order), keyOf(sorted[b], order)) < 0
	})
	ids := make([]string, len(sorted))
	for i, r := range sorted {
		ids[i] = r.id.String()
	}
	return ids
}

func tieCursor(t *testing.T, cols []model.KeysetColumn, values []any, orders []model.AttributeOrder) *model.KeysetCursor {
	t.Helper()
	cursor, err := sortkey.NormalizeCursor(&model.KeysetCursor{Columns: cols, Values: values, Mode: model.KeysetCursorModeAfter}, orders, nil)
	require.NoError(t, err)
	return cursor
}

var kIntOrder = []model.AttributeOrder{{AttrName: "k_int", ValueType: forma.ValueTypeInteger, StorageLocation: forma.AttributeStorageLocationEAV}}

// TestKeysetRowIDTiebreakParity: over rows that tie on every column before
// row_id, the comparator orders them as buildKeysetOrderBy orders them under
// row_id ASC and row_id DESC, and as buildNonKeysetOrderBy orders them; and
// the Go predicate at a row_id boundary inside the run admits the rows
// DuckDB admits.
func TestKeysetRowIDTiebreakParity(t *testing.T) {
	db := openParityDB(t)
	rows := loadTieRows(t, db)
	run := rows[2:14]

	type form struct {
		name string
		cols []model.KeysetColumn
		vals func(tieRow) []any
	}
	forms := []form{}
	for _, dir := range []forma.SortOrder{forma.SortOrderAsc, forma.SortOrderDesc} {
		forms = append(forms,
			form{"created_at DESC, row_id " + string(dir),
				[]model.KeysetColumn{{Attribute: "created_at", Direction: forma.SortOrderDesc}, {Attribute: "row_id", Direction: dir}},
				func(r tieRow) []any { return []any{r.created, r.id} }},
			form{"created_at, deleted_ts, row_id " + string(dir),
				[]model.KeysetColumn{{Attribute: "created_at", Direction: forma.SortOrderAsc}, {Attribute: "deleted_ts", Direction: dir}, {Attribute: "row_id", Direction: dir}},
				func(r tieRow) []any { return []any{r.created, int64(0), r.id} }},
		)
	}
	forms = append(forms, form{"k_int ASC, row_id ASC",
		[]model.KeysetColumn{{Attribute: "k_int", Direction: forma.SortOrderAsc}, {Attribute: "row_id", Direction: forma.SortOrderAsc}},
		func(r tieRow) []any { return []any{r.kInt, r.id} }})

	for _, f := range forms {
		for _, boundary := range run {
			cursor := tieCursor(t, f.cols, f.vals(boundary), kIntOrder)
			order, err := sortkey.KeysetOrder(cursor)
			require.NoError(t, err)
			label := fmt.Sprintf("%s at %s", f.name, boundary.id)

			byDuck := queryIDs(t, db, "SELECT CAST(row_id AS VARCHAR) FROM v ORDER BY "+sqlgen.BuildKeysetOrderBy(cursor))
			require.Equal(t, byDuck, goOrder(rows, order), "%s: order", label)

			clause, args, err := sqlgen.GenerateKeysetWhereClause(cursor)
			require.NoError(t, err)
			admitted := queryIDs(t, db, "SELECT CAST(row_id AS VARCHAR) FROM v WHERE "+clause+" ORDER BY "+sqlgen.BuildKeysetOrderBy(cursor), args...)
			var goAdmits []string
			for _, id := range goOrder(rows, order) {
				r := rowByID(rows, id)
				if order.Admits(keyOf(r, order), cursor.Values) {
					goAdmits = append(goAdmits, id)
				}
			}
			require.Equal(t, admitted, goAdmits, "%s: admitted rows", label)
		}
	}

	for _, orders := range [][]model.AttributeOrder{nil, kIntOrder, {{AttrName: "k_int", ValueType: forma.ValueTypeInteger, SortOrder: forma.SortOrderDesc, StorageLocation: forma.AttributeStorageLocationEAV}}} {
		q := &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{AttributeOrders: orders}}
		order, err := sortkey.OffsetOrder(orders)
		require.NoError(t, err)
		byDuck := queryIDs(t, db, "SELECT CAST(row_id AS VARCHAR) FROM v ORDER BY "+sqlgen.BuildNonKeysetOrderBy(q))
		require.Equal(t, byDuck, goOrder(rows, order), "offset order %v", orders)
	}
}

// TestKeysetDeletedTSOrdersByLaterColumns pins the order change the
// deleted_ts rule makes on today's path: among rows sharing created_at, a
// cursor on created_at, deleted_ts, row_id returns the NULL and 0 rows in
// row_id order, where the raw column put the NULL rows last.
func TestKeysetDeletedTSOrdersByLaterColumns(t *testing.T) {
	db := openParityDB(t)
	rows := loadTieRows(t, db)
	cursor := tieCursor(t, []model.KeysetColumn{
		{Attribute: "created_at", Direction: forma.SortOrderAsc},
		{Attribute: "deleted_ts", Direction: forma.SortOrderAsc},
		{Attribute: "row_id", Direction: forma.SortOrderAsc},
	}, []any{int64(99), int64(0), parityRowID(3)}, nil)
	require.Contains(t, sqlgen.BuildKeysetOrderBy(cursor), "COALESCE(deleted_ts, 0) ASC")

	clause, args, err := sqlgen.GenerateKeysetWhereClause(cursor)
	require.NoError(t, err)
	require.Contains(t, clause, "COALESCE(deleted_ts, 0) > CAST(? AS BIGINT)")
	got := queryIDs(t, db, "SELECT CAST(row_id AS VARCHAR) FROM v WHERE "+clause+" ORDER BY "+sqlgen.BuildKeysetOrderBy(cursor), args...)

	var tied []string
	for _, r := range rows {
		if r.created == int64(100) {
			tied = append(tied, r.id.String())
		}
	}
	sort.Strings(tied)
	require.Equal(t, append(tied, parityRowID(4).String()), got,
		"every row of created_at 100 in row_id order, NULL and 0 alike, then the next created_at")
}

func rowByID(rows []tieRow, id string) tieRow {
	for _, r := range rows {
		if r.id.String() == id {
			return r
		}
	}
	return tieRow{}
}
