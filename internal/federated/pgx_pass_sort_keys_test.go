package federated

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/sortkey"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

// The parquet leg's sort keys over attributes whose order the pgx pass must
// read at the export type, and under names that collide with the
// statement's own output columns (#856 review R1-F1, R1-F2).

// pgxSortMetadata is parquetOnlyMetadata plus a bool_smallint bool
// ("active"), a bool_text bool ("flag"), and an EAV integer named as the
// parquet-only statement names its first sort-key column.
func pgxSortMetadata(t *testing.T) *schemameta.MetadataCache {
	t.Helper()
	mc := schemameta.NewMetadataCache()
	require.NoError(t, mc.RegisterSchema("parquet_only", parquetOnlySchemaID, forma.SchemaAttributeCache{
		"name": {AttributeID: 1, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_01")}},
		"age": {AttributeID: 2, ValueType: forma.ValueTypeInteger,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("integer_01")}},
		"tag": {AttributeID: 205, ValueType: forma.ValueTypeText},
		"active": {AttributeID: 3, ValueType: forma.ValueTypeBool,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("smallint_01"), Encoding: forma.MainColumnEncodingBoolInt}},
		"flag": {AttributeID: 4, ValueType: forma.ValueTypeBool,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_02"), Encoding: forma.MainColumnEncodingBoolText}},
		sqlgen.SortKeyColumn(0): {AttributeID: 206, ValueType: forma.ValueTypeInteger},
	}))
	return mc
}

func boolOf(v bool) *bool { return &v }

func floatOf(v float64) *float64 { return &v }

// boolSortRows holds hot and parquet rows interleaved on both bools, with
// ties on each value, a row updated after its flush (2, whose parquet copy
// holds the opposite bools) and a row deleted after it (13). withNulls
// leaves active and flag NULL on two rows.
func boolSortRows(withNulls bool) []pgxRow {
	hot := map[int]bool{2: true, 3: true, 6: true, 9: true, 10: true}
	var rows []pgxRow
	for i := 1; i <= 12; i++ {
		r := pgxRow{i: i, createdAt: int64(10 * i), age: ageOf(30),
			active: boolOf(i%2 == 0), flag: boolOf(i%3 == 0),
			hot: hot[i], parquet: !hot[i] || i == 2}
		if withNulls && (i == 5 || i == 10) {
			r.active, r.flag = nil, nil
		}
		rows = append(rows, r)
	}
	return append(rows, pgxRow{i: 13, createdAt: 0, age: ageOf(30), active: boolOf(false), flag: boolOf(false), parquet: true, deleted: true})
}

// boolOrders orders by one bound bool: active (bool_smallint, smallint_01)
// or flag (bool_text, text_02).
func boolOrders(attr string, desc bool) []model.AttributeOrder {
	column := map[string]string{"active": "smallint_01", "flag": "text_02"}[attr]
	dir := forma.SortOrderAsc
	if desc {
		dir = forma.SortOrderDesc
	}
	return []model.AttributeOrder{{AttrID: 3, AttrName: attr, ValueType: forma.ValueTypeBool,
		StorageLocation: forma.AttributeStorageLocationMain, ColumnName: column, SortOrder: dir}}
}

// A bound bool sorts at its export type on both legs: offset pages of every
// size tile the merged order, whichever encoding stores it. Before the fix
// the parquet leg returned the outer select's SMALLINT (bool_smallint) or
// VARCHAR (bool_text) image as the key, and every page with a parquet row
// failed.
func TestPgxPassOffsetPagesOrderByColumnBoundBooleans(t *testing.T) {
	ctx := context.Background()
	for _, attr := range []string{"active", "flag"} {
		for _, desc := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/desc=%t", attr, desc), func(t *testing.T) {
				f := newPgxFixture(t, boolSortRows(true))
				f.meta = pgxSortMetadata(t)
				e := f.engine(t, []string{f.dir + "/base.parquet"})
				orders := boolOrders(attr, desc)
				order, err := sortkey.OffsetOrder(orders)
				require.NoError(t, err)
				require.Equal(t, sortkey.Boolean, order.Columns[0].Type)
				want := f.visibleOrder(t, order)
				require.Len(t, want, 12)
				for limit := 1; limit <= 3; limit++ {
					for offset := 0; offset <= len(want); offset++ {
						records, total, err := e.ExecuteDuckDBFederatedQuery(ctx, tieRunTables, pgxQuery(), limit, offset, orders, nil)
						require.NoError(t, err)
						require.Equal(t, want[min(offset, len(want)):min(offset+limit, len(want))], idsOrEmpty(records),
							"offset %d, limit %d", offset, limit)
						require.Equal(t, int64(len(want)), total)
					}
				}
			})
		}
	}
}

// A keyset walk over a bound bool returns every row once, in the merged
// order, with page boundaries between hot and parquet rows of one tie run.
func TestPgxPassKeysetWalksColumnBoundBooleans(t *testing.T) {
	for _, attr := range []string{"active", "flag"} {
		for _, limit := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s/limit=%d", attr, limit), func(t *testing.T) {
				f := newPgxFixture(t, boolSortRows(false))
				f.meta = pgxSortMetadata(t)
				e := f.engine(t, []string{f.dir + "/base.parquet"})
				columns := []model.KeysetColumn{{Attribute: attr, Direction: forma.SortOrderAsc}, {Attribute: "row_id", Direction: forma.SortOrderAsc}}
				order := sortkey.Order{Columns: []sortkey.Column{{Name: attr, Type: sortkey.Boolean}, {Name: sortkey.ColumnRowID, Type: sortkey.UUID}}}
				want := f.visibleOrder(t, order)
				require.Len(t, want, 12)
				require.Equal(t, want, walkPgxKeyset(t, e, f, columns, boolOrders(attr, false), []any{false, uuid.Nil}, limit))
			})
		}
	}
}

// collidingRows tie on age in pairs, and forma_sort_key_0 rises with
// row_id, so its DESC order breaks each tie against row_id: an order that
// read the statement's sort-key alias for age in its place would fall
// through to row_id and answer each pair reversed.
func collidingRows() []pgxRow {
	hot := map[int]bool{2: true, 3: true, 6: true}
	var rows []pgxRow
	for i := 1; i <= 8; i++ {
		rows = append(rows, pgxRow{i: i, createdAt: int64(i), age: ageOf(int64(30 + (i-1)/2)),
			key0: floatOf(float64(i)), hot: hot[i], parquet: !hot[i] || i == 3})
	}
	return rows
}

// collidingOrders orders by age ASC, then forma_sort_key_0 DESC.
func collidingOrders() []model.AttributeOrder {
	return []model.AttributeOrder{
		{AttrID: 2, AttrName: "age", ValueType: forma.ValueTypeInteger,
			StorageLocation: forma.AttributeStorageLocationMain, ColumnName: "integer_01", SortOrder: forma.SortOrderAsc},
		{AttrID: 206, AttrName: sqlgen.SortKeyColumn(0), ValueType: forma.ValueTypeInteger,
			StorageLocation: forma.AttributeStorageLocationEAV, SortOrder: forma.SortOrderDesc},
	}
}

// An attribute named like a sort-key column of the parquet-only result
// orders as itself, on offset pages: the statement reads it as
// visible.forma_sort_key_0, never as the generated alias of the age key.
func TestPgxPassOffsetOrderReadsAnAttributeNamedLikeASortKeyColumn(t *testing.T) {
	ctx := context.Background()
	f := newPgxFixture(t, collidingRows())
	f.meta = pgxSortMetadata(t)
	e := f.engine(t, []string{f.dir + "/base.parquet"})
	orders := collidingOrders()
	order, err := sortkey.OffsetOrder(orders)
	require.NoError(t, err)
	want := f.visibleOrder(t, order)
	require.Equal(t, []uuid.UUID{parquetOnlyRowID(2), parquetOnlyRowID(1)}, want[:2], "the second key breaks the age tie")
	for limit := 1; limit <= 3; limit++ {
		for offset := 0; offset <= len(want); offset++ {
			records, total, err := e.ExecuteDuckDBFederatedQuery(ctx, tieRunTables, pgxQuery(), limit, offset, orders, nil)
			require.NoError(t, err)
			require.Equal(t, want[min(offset, len(want)):min(offset+limit, len(want))], idsOrEmpty(records),
				"offset %d, limit %d", offset, limit)
			require.Equal(t, int64(len(want)), total)
		}
	}
}

// The same collision under a cursor: a keyset walk on age ASC,
// forma_sort_key_0 DESC returns every row once, in that order.
func TestPgxPassKeysetOrderReadsAnAttributeNamedLikeASortKeyColumn(t *testing.T) {
	for _, limit := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			f := newPgxFixture(t, collidingRows())
			f.meta = pgxSortMetadata(t)
			e := f.engine(t, []string{f.dir + "/base.parquet"})
			key0 := sqlgen.SortKeyColumn(0)
			columns := []model.KeysetColumn{
				{Attribute: "age", Direction: forma.SortOrderAsc},
				{Attribute: key0, Direction: forma.SortOrderDesc},
				{Attribute: "row_id", Direction: forma.SortOrderAsc},
			}
			orders := collidingOrders()
			order := sortkey.Order{Columns: []sortkey.Column{
				{Name: "age", Type: sortkey.Integer},
				{Name: key0, Type: sortkey.Double, Desc: true},
				{Name: sortkey.ColumnRowID, Type: sortkey.UUID},
			}}
			want := f.visibleOrder(t, order)
			require.Equal(t, []uuid.UUID{parquetOnlyRowID(2), parquetOnlyRowID(1)}, want[:2])
			require.Equal(t, want, walkPgxKeyset(t, e, f, columns, orders, []any{int64(-1), float64(0), uuid.Nil}, limit))
		})
	}
}
