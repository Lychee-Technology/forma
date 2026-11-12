package federated

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/queryplan"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

// The parquet-only template (plan step 3, #854) run through the engine's
// compile path and executed on DuckDB over a real parquet file, with the
// dirty set staged through the Appender as the pgx pass will stage it.

const parquetOnlySchemaID int16 = 7

var parquetOnlyTables = model.StorageTables{EntityMain: "entity_main", EAVData: "eav_data", ChangeLog: "change_log"}

func parquetOnlyRowID(i int) uuid.UUID {
	return uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0000-%012d", i))
}

// parquetOnlyFixture is one DuckDB connection and one parquet file of 24
// rows, i = 0..23: created_at 1000+i, age 20+i, tag "developer". Row 5 has a
// newer version with age 10, row 6 a newer tombstone, and rows 22 and 23 a
// NULL age.
type parquetOnlyFixture struct {
	conn *sql.Conn
	path string
}

func newParquetOnlyFixture(t *testing.T) *parquetOnlyFixture {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "base.parquet")
	db, err := sql.Open("duckdb", strictLambdaDSN)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	var values []string
	row := func(i int, changedAt, deletedAt int64, age string) {
		values = append(values, fmt.Sprintf("('%s'::UUID, %d::BIGINT, %d::BIGINT, %d::BIGINT, 'John %d', %s::INTEGER, 'developer')",
			parquetOnlyRowID(i), 1000+i, changedAt, deletedAt, i, age))
	}
	for i := 0; i < 24; i++ {
		age := fmt.Sprint(20 + i)
		if i >= 22 {
			age = "NULL"
		}
		row(i, 100, 0, age)
	}
	row(5, 200, 0, "10")
	row(6, 200, 200, "26")
	_, err = conn.ExecContext(ctx, `COPY (SELECT * FROM (VALUES `+strings.Join(values, ", ")+
		`) AS t(row_id, ltbase_created_at, changed_at, deleted_at, name, age, tag)) TO '`+path+`' (FORMAT PARQUET)`)
	require.NoError(t, err)
	return &parquetOnlyFixture{conn: conn, path: path}
}

func parquetOnlyMetadata(t *testing.T) *schemameta.MetadataCache {
	t.Helper()
	mc := schemameta.NewMetadataCache()
	require.NoError(t, mc.RegisterSchema("parquet_only", parquetOnlySchemaID, forma.SchemaAttributeCache{
		"name": {AttributeID: 1, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_01")}},
		"age": {AttributeID: 2, ValueType: forma.ValueTypeInteger,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("integer_01")}},
		"tag": {AttributeID: 205, ValueType: forma.ValueTypeText},
	}))
	return mc
}

func newParquetOnlyEngine(t *testing.T, cache *queryplan.Cache, connString string) *DBFederatedQueryEngine {
	t.Helper()
	return NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, &fakeDirtyIDFetcher{}, &fakeDuckDBExecutor{}, nil,
		hybridDuckConfig(), parquetOnlyMetadata(t), connString, WithPlanCache(cache))
}

func parquetOnlyQuery(cond forma.Condition, cursor *model.KeysetCursor) *model.FederatedAttributeQuery {
	q := &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: parquetOnlySchemaID, Condition: cond}}
	q.KeysetCursor = cursor
	return q
}

// run stages dirty, executes sqlStr on the fixture's connection, and returns
// the row IDs in order with the parquet count (-1 when no row came back).
func (f *parquetOnlyFixture) run(t *testing.T, dirty []uuid.UUID, sqlStr string, args []any) ([]uuid.UUID, int64) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, StageDirtySet(ctx, f.conn, dirty))
	defer func() { require.NoError(t, DropStagedDirtySet(ctx, f.conn)) }()

	rows, err := f.conn.QueryContext(ctx, sqlStr, args...)
	require.NoError(t, err, "query=%s", sqlStr)
	defer func() { require.NoError(t, rows.Close()) }()
	cols, err := rows.Columns()
	require.NoError(t, err)
	idCol, totalCol := -1, -1
	for i, c := range cols {
		switch c {
		case "ltbase_row_id":
			idCol = i
		case "total_records":
			totalCol = i
		}
	}
	require.True(t, idCol >= 0 && totalCol >= 0, "result columns %v", cols)

	var ids []uuid.UUID
	total := int64(-1)
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		require.NoError(t, rows.Scan(ptrs...))
		ids = append(ids, scannedUUID(t, vals[idCol]))
		total = vals[totalCol].(int64)
	}
	require.NoError(t, rows.Err())
	return ids, total
}

func scannedUUID(t *testing.T, v any) uuid.UUID {
	t.Helper()
	switch id := v.(type) {
	case duckdb.UUID:
		return uuid.UUID(id)
	case []byte:
		u, err := uuid.FromBytes(id)
		require.NoError(t, err)
		return u
	case string:
		return uuid.MustParse(id)
	default:
		t.Fatalf("unexpected row_id type %T", v)
		return uuid.Nil
	}
}

// parquetOnlyExpected is the parquet leg's full answer to age > 18 in its
// order (created_at DESC): rows 21 down to 0 without the superseded row 5,
// the tombstoned row 6 and the staged dirty rows.
func parquetOnlyExpected(dirty []uuid.UUID) []uuid.UUID {
	skip := map[uuid.UUID]bool{parquetOnlyRowID(5): true, parquetOnlyRowID(6): true}
	for _, id := range dirty {
		skip[id] = true
	}
	var out []uuid.UUID
	for i := 21; i >= 0; i-- {
		if !skip[parquetOnlyRowID(i)] {
			out = append(out, parquetOnlyRowID(i))
		}
	}
	return out
}

func parquetOnlyDirtySet(n int) []uuid.UUID {
	if n == 0 {
		return nil
	}
	ids := []uuid.UUID{parquetOnlyRowID(1), parquetOnlyRowID(2)}
	for len(ids) < n {
		ids = append(ids, uuid.New())
	}
	return ids
}

// The compiled-plan cache keeps one entry per shape, whatever the dirty set's
// size, the hot match count, the page and the engine's connection string:
// none of them renders into the parquet-only skeleton. Each pass also runs,
// and answers exactly the parquet window it was bound to.
func TestParquetOnlyPlanCacheKeepsOneEntryPerShape(t *testing.T) {
	ctx := context.Background()
	f := newParquetOnlyFixture(t)
	cache := queryplan.NewCache(64)
	engines := []*DBFederatedQueryEngine{
		newParquetOnlyEngine(t, cache, "host=a user=forma password=first-secret"),
		newParquetOnlyEngine(t, cache, "host=b user=forma password=second-secret"),
	}
	cond := &forma.KvCondition{Attr: "age", Value: "gt:18"}
	paths := []string{f.path}
	const limit = 5

	var skeleton string
	for _, e := range engines {
		for _, dirtySize := range []int{0, 2, 1000} {
			dirty := parquetOnlyDirtySet(dirtySize)
			expected := parquetOnlyExpected(dirty)
			for _, page := range []struct{ offset, hot int }{{0, 0}, {4, 3}, {8, 40}} {
				window, err := sqlgen.OffsetPageParquetWindow(page.offset, limit, page.hot)
				require.NoError(t, err)
				sqlStr, args, err := e.buildParquetOnlyQuery(ctx, parquetOnlyTables, parquetOnlyQuery(cond, nil),
					nil, limit, page.offset, window, paths, coldScanSet{})
				require.NoError(t, err)
				if skeleton == "" {
					skeleton = sqlStr
				}
				require.Equal(t, skeleton, sqlStr, "one statement text for the shape")
				require.NotContains(t, sqlStr, "secret")

				ids, total := f.run(t, dirty, sqlStr, args)
				end := min(int(window.Offset+window.Limit), len(expected))
				require.Equal(t, expected[window.Offset:end], ids,
					"dirty %d, offset %d, hot %d: window %+v", dirtySize, page.offset, page.hot, window)
				require.Equal(t, int64(len(expected)), total, "total_records is the parquet leg's full count")
			}
		}
	}
	_, misses := cache.Stats()
	require.Equal(t, int64(1), misses, "one compiled plan for the offset shape")

	// A keyset page is another shape: one more entry, whatever the set.
	for _, dirtySize := range []int{0, 1000} {
		dirty := parquetOnlyDirtySet(dirtySize)
		expected := parquetOnlyExpected(dirty)
		// expected[2] is row 19 either way: the dirty rows 1 and 2 sort last.
		require.Equal(t, parquetOnlyRowID(19), expected[2])
		boundary := int64(1000 + 19)
		cursor := &model.KeysetCursor{
			Mode: model.KeysetCursorModeAfter,
			Columns: []model.KeysetColumn{
				{Attribute: "created_at", Direction: forma.SortOrderDesc},
				{Attribute: "row_id", Direction: forma.SortOrderDesc},
			},
			Values: []any{boundary, expected[2].String()},
		}
		window, err := sqlgen.KeysetPageParquetWindow(3)
		require.NoError(t, err)
		sqlStr, args, err := engines[dirtySize%2].buildParquetOnlyQuery(ctx, parquetOnlyTables, parquetOnlyQuery(cond, cursor),
			nil, 3, 0, window, paths, coldScanSet{})
		require.NoError(t, err)
		ids, _ := f.run(t, dirty, sqlStr, args)
		require.Equal(t, expected[3:6], ids, "dirty %d: the three parquet rows after the cursor", dirtySize)
	}
	_, misses = cache.Stats()
	require.Equal(t, int64(2), misses, "one compiled plan per shape: offset and keyset")
}

// The parquet leg orders NULL last in both directions even when DuckDB's
// default_null_order says otherwise, because every key spells NULLS LAST
// out: the Go comparator puts NULL last, and the merge needs the two orders
// to agree.
func TestParquetOnlyOrderKeepsNullsLastUnderNullsFirstDefault(t *testing.T) {
	ctx := context.Background()
	f := newParquetOnlyFixture(t)
	_, err := f.conn.ExecContext(ctx, "SET default_null_order = 'nulls_first'")
	require.NoError(t, err)
	var first sql.NullInt64
	require.NoError(t, f.conn.QueryRowContext(ctx, "SELECT x FROM (VALUES (1), (NULL)) t(x) ORDER BY x LIMIT 1").Scan(&first))
	require.False(t, first.Valid, "the setting must be in force for this test to mean anything")

	e := newParquetOnlyEngine(t, nil, "")
	cond := &forma.KvCondition{Attr: "tag", Value: "equals:developer"}
	window, err := sqlgen.OffsetPageParquetWindow(0, 100, 0)
	require.NoError(t, err)
	for _, dir := range []forma.SortOrder{forma.SortOrderAsc, forma.SortOrderDesc} {
		orders := []model.AttributeOrder{{AttrID: 2, AttrName: "age", ColumnName: "integer_01", ValueType: forma.ValueTypeInteger,
			StorageLocation: forma.AttributeStorageLocationMain, SortOrder: dir}}
		sqlStr, args, err := e.buildParquetOnlyQuery(ctx, parquetOnlyTables, parquetOnlyQuery(cond, nil),
			orders, 100, 0, window, []string{f.path}, coldScanSet{})
		require.NoError(t, err)
		ids, _ := f.run(t, nil, sqlStr, args)
		require.Len(t, ids, 23, "every live row: 24 less the tombstoned row 6")
		require.Equal(t, []uuid.UUID{parquetOnlyRowID(22), parquetOnlyRowID(23)}, ids[21:],
			"%s: the NULL ages come last, tied rows by row_id ASC", dir)
	}
}

// Staging replaces any set an earlier pass on the connection left behind,
// so a pass that failed before its drop cannot leak its set into the next.
func TestStageDirtySetReplacesAnEarlierSet(t *testing.T) {
	ctx := context.Background()
	f := newParquetOnlyFixture(t)
	count := func() int64 {
		var n int64
		require.NoError(t, f.conn.QueryRowContext(ctx, "SELECT count(*) FROM temp.main."+sqlgen.StagedDirtyIDsTable).Scan(&n))
		return n
	}
	require.NoError(t, StageDirtySet(ctx, f.conn, parquetOnlyDirtySet(1000)))
	require.Equal(t, int64(1000), count())
	require.NoError(t, StageDirtySet(ctx, f.conn, parquetOnlyDirtySet(2)))
	require.Equal(t, int64(2), count())
	require.NoError(t, StageDirtySet(ctx, f.conn, nil))
	require.Equal(t, int64(0), count())
	require.NoError(t, DropStagedDirtySet(ctx, f.conn))
	require.NoError(t, DropStagedDirtySet(ctx, f.conn), "dropping twice is harmless")
}
