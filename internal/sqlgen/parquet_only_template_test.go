package sqlgen

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

func parquetOnlyTestParams(t *testing.T) map[string]any {
	t.Helper()
	return injectTestRenderParams(t, map[string]any{
		// A connection string the parquet-only render must never carry.
		"DuckDBPGConnString": "host=pg password=hunter2",
	}, 7)
}

func parquetOnlyTestQuery(cursor *model.KeysetCursor) *model.FederatedAttributeQuery {
	q := &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{
		SchemaID: 7,
		Condition: &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
			&forma.KvCondition{Attr: "age", Value: "gt:10"},
			&forma.KvCondition{Attr: "tag", Value: "equals:x"},
		}},
		Limit: 25,
	}}
	q.KeysetCursor = cursor
	return q
}

func parquetOnlyTestCursor() *model.KeysetCursor {
	return &model.KeysetCursor{
		Mode: model.KeysetCursorModeAfter,
		Columns: []model.KeysetColumn{
			{Attribute: "created_at", Direction: forma.SortOrderDesc, ExportType: "BIGINT"},
			{Attribute: "row_id", Direction: forma.SortOrderDesc, ExportType: "UUID"},
		},
		Values: []any{int64(1700000000000), "11111111-1111-1111-1111-111111111111"},
	}
}

// The parquet-only render reads no PostgreSQL at all: no pg_source, no
// catalog reference, no change_log scan and no flush-grace cutoff. The dirty
// set comes from the staged temp table, and every placeholder has a bind.
func TestParquetOnlyTemplateRendersNoPostgres(t *testing.T) {
	cache := dualPlanTestCache()
	for name, cursor := range map[string]*model.KeysetCursor{
		"offset": nil,
		"keyset": parquetOnlyTestCursor(),
	} {
		t.Run(name, func(t *testing.T) {
			q := parquetOnlyTestQuery(cursor)
			dual := parityDual(t, q.Condition, cache)
			sql, args, err := BuildParquetOnlyQuery(parquetOnlyTestParams(t), q, dual, ParquetWindow{Limit: 30})
			require.NoError(t, err)

			for _, absent := range []string{"pg_source", "forma_pg_", "change_log", "flushed_at", "hunter2", "ATTACH", "UNION ALL\n"} {
				require.NotContains(t, sql, absent)
			}
			require.Contains(t, sql, "NOT IN (SELECT row_id FROM temp.main.forma_dirty_ids)")
			require.Equal(t, 2, strings.Count(sql, BuildParquetScanSource("'"+testRenderParquetPath+"'", nil, nil)),
				"both scan sites read the runtime scan source")
			require.Equal(t, strings.Count(sql, "?"), len(args), "every placeholder has exactly one bind")
			require.Equal(t, int64(30), args[len(args)-map[bool]int{true: 1, false: 2}[cursor != nil]],
				"the window's limit binds after every condition and keyset arg")
		})
	}
}

// Every ORDER BY term renders NULLS LAST, in the order today's template
// renders, for both page forms.
func TestParquetOnlyOrderByIsExplicitNullsLast(t *testing.T) {
	sortQ := parquetOnlyTestQuery(nil)
	sortQ.AttributeOrders = []model.AttributeOrder{
		{AttrID: 6, AttrName: "age", ColumnName: "integer_01", StorageLocation: forma.AttributeStorageLocationMain, SortOrder: forma.SortOrderDesc},
		{AttrID: 7, AttrName: "tag", StorageLocation: forma.AttributeStorageLocationEAV, SortOrder: forma.SortOrderAsc},
	}
	cases := map[string]struct {
		q    *model.FederatedAttributeQuery
		want string
	}{
		"default order": {parquetOnlyTestQuery(nil), "ORDER BY created_at DESC NULLS LAST, row_id ASC NULLS LAST\nLIMIT ? OFFSET ?;"},
		"attribute order": {sortQ,
			"ORDER BY integer_01 DESC NULLS LAST, tag ASC NULLS LAST, row_id ASC NULLS LAST\nLIMIT ? OFFSET ?;"},
		"keyset": {parquetOnlyTestQuery(parquetOnlyTestCursor()),
			"ORDER BY created_at DESC NULLS LAST, row_id DESC NULLS LAST\nLIMIT ?;"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			compiled, err := CompileParquetOnlyQuery(parquetOnlyTestParams(t), c.q, nil)
			require.NoError(t, err)
			require.True(t, strings.HasSuffix(strings.TrimSpace(compiled.Skeleton), c.want),
				"skeleton tail:\n%s", compiled.Skeleton[len(compiled.Skeleton)-200:])
		})
	}
}

// The window and the condition operands are binds, so requests that differ
// only in them share one skeleton byte for byte.
func TestParquetOnlySkeletonIgnoresWindowAndOperands(t *testing.T) {
	cache := dualPlanTestCache()
	q1 := parquetOnlyTestQuery(nil)
	q2 := parquetOnlyTestQuery(nil)
	q2.Condition = &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
		&forma.KvCondition{Attr: "age", Value: "gt:99"},
		&forma.KvCondition{Attr: "tag", Value: "equals:y"},
	}}
	q2.Offset, q2.Limit = 400, 7

	sql1, args1, err := BuildParquetOnlyQuery(parquetOnlyTestParams(t), q1, parityDual(t, q1.Condition, cache), ParquetWindow{Offset: 0, Limit: 25})
	require.NoError(t, err)
	sql2, args2, err := BuildParquetOnlyQuery(parquetOnlyTestParams(t), q2, parityDual(t, q2.Condition, cache), ParquetWindow{Offset: 350, Limit: 57})
	require.NoError(t, err)
	require.Equal(t, sql1, sql2)
	require.Equal(t, []any{int64(25), int64(0)}, args1[len(args1)-2:])
	require.Equal(t, []any{int64(57), int64(350)}, args2[len(args2)-2:])
}

func TestParquetOnlyBindRefusesMismatchedRequests(t *testing.T) {
	offsetQ := parquetOnlyTestQuery(nil)
	keysetQ := parquetOnlyTestQuery(parquetOnlyTestCursor())
	offsetSkel, err := CompileParquetOnlyQuery(parquetOnlyTestParams(t), offsetQ, nil)
	require.NoError(t, err)
	keysetSkel, err := CompileParquetOnlyQuery(parquetOnlyTestParams(t), keysetQ, nil)
	require.NoError(t, err)

	_, _, err = offsetSkel.Bind(keysetQ, DualClauses{}, ParquetWindow{Limit: 1})
	require.ErrorContains(t, err, "keyset page")
	_, _, err = keysetSkel.Bind(offsetQ, DualClauses{}, ParquetWindow{Limit: 1})
	require.ErrorContains(t, err, "keyset page")
	_, _, err = keysetSkel.Bind(keysetQ, DualClauses{}, ParquetWindow{Offset: 3, Limit: 1})
	require.ErrorContains(t, err, "starts at its cursor")
	_, _, err = offsetSkel.Bind(offsetQ, DualClauses{}, ParquetWindow{Offset: -1, Limit: 1})
	require.ErrorContains(t, err, "non-negative")
}

func TestParquetOnlyCompileRefusesMissingInputs(t *testing.T) {
	_, err := CompileParquetOnlyQuery(parquetOnlyTestParams(t), nil, nil)
	require.ErrorContains(t, err, "nil")

	params := parquetOnlyTestParams(t)
	delete(params, "OuterSelect")
	_, err = CompileParquetOnlyQuery(params, parquetOnlyTestQuery(nil), nil)
	require.ErrorContains(t, err, "[OuterSelect]")

	params = parquetOnlyTestParams(t)
	delete(params, "DuckDBS3Paths")
	_, err = CompileParquetOnlyQuery(params, parquetOnlyTestQuery(nil), nil)
	require.ErrorContains(t, err, "S3_SCAN_SOURCE")

	// The hot leg's projection params are not needed.
	params = parquetOnlyTestParams(t)
	for _, key := range []string{"PGSourceSelect", "PGGroupBy", "EAVPivotSelect", "EAVPivotAttrs", "HasEAVPivot"} {
		delete(params, key)
	}
	_, err = CompileParquetOnlyQuery(params, parquetOnlyTestQuery(nil), nil)
	require.NoError(t, err)
}

// The offset window holds every parquet rank that can land on the page: a
// rank p lands at p plus the hot keys before it, 0 to h of them.
func TestOffsetPageParquetWindow(t *testing.T) {
	cases := []struct {
		offset, limit, hot int
		want               ParquetWindow
	}{
		{0, 10, 0, ParquetWindow{Offset: 0, Limit: 10}},
		{0, 10, 5, ParquetWindow{Offset: 0, Limit: 10}},
		{20, 10, 0, ParquetWindow{Offset: 20, Limit: 10}},
		{20, 10, 5, ParquetWindow{Offset: 15, Limit: 15}},
		{20, 10, 50, ParquetWindow{Offset: 0, Limit: 30}},
		{20, 0, 5, ParquetWindow{Offset: 15, Limit: 5}},
	}
	for _, c := range cases {
		got, err := OffsetPageParquetWindow(c.offset, c.limit, c.hot)
		require.NoError(t, err)
		require.Equal(t, c.want, got, "offset %d limit %d hot %d", c.offset, c.limit, c.hot)
		require.LessOrEqual(t, got.Limit, int64(c.limit+c.hot), "never more than h+limit rows")
	}
	for _, bad := range [][3]int{{-1, 1, 0}, {0, -1, 0}, {0, 1, -1}} {
		_, err := OffsetPageParquetWindow(bad[0], bad[1], bad[2])
		require.Error(t, err)
	}

	got, err := KeysetPageParquetWindow(12)
	require.NoError(t, err)
	require.Equal(t, ParquetWindow{Limit: 12}, got)
	_, err = KeysetPageParquetWindow(-1)
	require.Error(t, err)
}
