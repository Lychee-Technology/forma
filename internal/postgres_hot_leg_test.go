package internal

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
)

func hotLegTestRepo(t *testing.T) *DBPersistentRecordRepository {
	t.Helper()
	mc := schemameta.NewMetadataCache()
	require.NoError(t, mc.RegisterSchema("orders", 7, forma.SchemaAttributeCache{
		"code":   {AttributeID: 1, ValueType: forma.ValueTypeText, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_01")}},
		"note":   {AttributeID: 2, ValueType: forma.ValueTypeText},
		"weight": {AttributeID: 3, ValueType: forma.ValueTypeNumeric},
		"tags":   {AttributeID: 4, ValueType: forma.ValueTypeList},
	}))
	return NewDBPersistentRecordRepository(nil, mc)
}

var hotLegTestTables = model.StorageTables{EntityMain: "entity_main", EAVData: "eav_data", ChangeLog: "change_log"}

// The hot-keys query reads the dirty window as two index ranges, joins the
// live entity_main rows, filters with the PostgreSQL route's main-anchored
// rendering after its own two parameters, and projects each sort column's
// stored value where its key derives from it.
func TestBuildHotKeysStatement(t *testing.T) {
	r := hotLegTestRepo(t)
	stmt, err := r.buildHotKeysStatement(model.HotKeysQuery{
		Tables:        hotLegTestTables,
		SchemaID:      7,
		GraceCutoffMs: 1234,
		Condition: &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
			&forma.KvCondition{Attr: "code", Value: "gt:B"},
			&forma.KvCondition{Attr: "note", Value: "equals:x"},
		}},
		SortColumns: []string{"weight", "deleted_ts", "CODE", "created_at", "note", "row_id", "ver_ts"},
	})
	require.NoError(t, err)

	for _, want := range []string{
		`SELECT row_id, changed_at FROM "change_log" WHERE schema_id = $1 AND flushed_at = 0`,
		`SELECT row_id, changed_at FROM "change_log" WHERE schema_id = $1 AND flushed_at >= $2`,
		`JOIN "entity_main" m ON m.ltbase_schema_id = $1 AND m.ltbase_row_id = w.row_id`,
		`m."text_01" COLLATE "C" > $3`,
		`m.ltbase_schema_id`,
		`(SELECT ARRAY_AGG(e.value_numeric) FROM "eav_data" e WHERE e.schema_id = m.ltbase_schema_id AND e.row_id = m.ltbase_row_id AND e.attr_id = 3)`,
		`(SELECT ARRAY_AGG(e.value_text) FROM "eav_data" e WHERE e.schema_id = m.ltbase_schema_id AND e.row_id = m.ltbase_row_id AND e.attr_id = 2)`,
		`m.ltbase_created_at`,
	} {
		require.Contains(t, stmt.sql, want)
	}
	require.NotContains(t, stmt.sql, "ORDER BY", "Go orders the hot keys")
	require.NotContains(t, stmt.sql, "LIMIT")
	require.Equal(t, []any{int16(7), int64(1234), "B", int16(2), "x"}, stmt.args)

	indexes := make([]int, len(stmt.columns))
	for i, c := range stmt.columns {
		indexes[i] = c.index
	}
	require.Equal(t, []int{2, -1, 3, 4, 5, hotKeysRowIDIndex, hotKeysVerTSIndex}, indexes,
		"deleted_ts reads nothing; row_id and ver_ts reuse the columns every key reads")
	require.True(t, stmt.columns[0].eav)
	require.Equal(t, "code", stmt.columns[2].meta.AttributeName, "CODE reaches code as DuckDB binds it")
	require.False(t, stmt.columns[2].eav)
}

func TestBuildHotKeysStatementWithoutCondition(t *testing.T) {
	stmt, err := hotLegTestRepo(t).buildHotKeysStatement(model.HotKeysQuery{Tables: hotLegTestTables, SchemaID: 7})
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(stmt.sql, "WHERE (1=1)"), stmt.sql)
	require.Equal(t, []any{int16(7), int64(0)}, stmt.args)
	require.Empty(t, stmt.columns)
}

func TestBuildHotKeysStatementRefusals(t *testing.T) {
	r := hotLegTestRepo(t)
	cases := map[string]model.HotKeysQuery{
		"change log table name cannot be empty": {Tables: model.StorageTables{EntityMain: "m", EAVData: "e"}, SchemaID: 7},
		"eav table name cannot be empty":        {Tables: model.StorageTables{EntityMain: "m", ChangeLog: "c"}, SchemaID: 7},
		"schema id must be positive":            {Tables: hotLegTestTables},
		"no cache for schema id 8":              {Tables: hotLegTestTables, SchemaID: 8},
		`attribute "tags" is a list`:            {Tables: hotLegTestTables, SchemaID: 7, SortColumns: []string{"tags"}},
		"neither a system column":               {Tables: hotLegTestTables, SchemaID: 7, SortColumns: []string{"nope"}},
		"render condition":                      {Tables: hotLegTestTables, SchemaID: 7, Condition: &forma.KvCondition{Attr: "code", Value: "in:[]"}},
	}
	for want, req := range cases {
		_, err := r.buildHotKeysStatement(req)
		require.ErrorContains(t, err, want)
	}
	_, err := NewDBPersistentRecordRepository(nil, nil).buildHotKeysStatement(model.HotKeysQuery{Tables: hotLegTestTables, SchemaID: 7})
	require.ErrorContains(t, err, "schema metadata cache not available")
}
