package internal

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/stretchr/testify/require"
)

func anchorCTESegment(t *testing.T, sql string) string {
	t.Helper()

	start := strings.Index(sql, "WITH anchor AS (")
	require.NotEqual(t, -1, start)

	end := strings.Index(sql, "),\n        keys AS (")
	require.NotEqual(t, -1, end)

	return sql[start:end]
}

func TestOptimizedQueryTemplate_ChangeLogUsesExistsForMainAnchor(t *testing.T) {
	sql, err := renderTemplate(optimizedQuerySQLTemplate, map[string]any{
		"EAVTable":             "eav_data",
		"MainTable":            "entity_main",
		"ChangeLogTable":       "change_log",
		"MainProjection":       "m.ltbase_schema_id, m.ltbase_row_id",
		"SchemaID":             "$1",
		"UseMainTableAsAnchor": true,
		"Anchor": map[string]any{
			"Condition": "m.\"text_01\" = $2",
		},
		"SortKeys": []model.AttributeOrder{},
		"Limit":    "$3",
		"Offset":   "$4",
		"PageSize": "$3",
	})
	require.NoError(t, err)
	anchor := anchorCTESegment(t, sql)

	require.Contains(t, anchor, "FROM change_log cl")
	require.Contains(t, anchor, "AND EXISTS (")
	require.Contains(t, anchor, "FROM entity_main m")
	require.NotContains(t, anchor, "INNER JOIN entity_main m")
	require.NotContains(t, anchor, "SELECT DISTINCT cl.row_id")
}

func TestOptimizedQueryTemplate_ChangeLogUsesExistsForEAVAnchor(t *testing.T) {
	sql, err := renderTemplate(optimizedQuerySQLTemplate, map[string]any{
		"EAVTable":             "eav_data",
		"MainTable":            "entity_main",
		"ChangeLogTable":       "change_log",
		"MainProjection":       "m.ltbase_schema_id, m.ltbase_row_id",
		"SchemaID":             "$1",
		"UseMainTableAsAnchor": false,
		"Anchor": map[string]any{
			"Condition": "t.attr_id = 42",
		},
		"SortKeys": []model.AttributeOrder{},
		"Limit":    "$3",
		"Offset":   "$4",
		"PageSize": "$3",
	})
	require.NoError(t, err)
	anchor := anchorCTESegment(t, sql)

	require.Contains(t, anchor, "FROM change_log cl")
	require.Contains(t, anchor, "AND EXISTS (")
	require.Contains(t, anchor, "FROM eav_data t")
	require.NotContains(t, anchor, "INNER JOIN eav_data t")
	require.NotContains(t, anchor, "SELECT DISTINCT cl.row_id")
}

func TestOptimizedQueryTemplate_WithoutChangeLogHasNoUnionBranch(t *testing.T) {
	sql, err := renderTemplate(optimizedQuerySQLTemplate, map[string]any{
		"EAVTable":             "eav_data",
		"MainTable":            "entity_main",
		"ChangeLogTable":       "",
		"MainProjection":       "m.ltbase_schema_id, m.ltbase_row_id",
		"SchemaID":             "$1",
		"UseMainTableAsAnchor": true,
		"Anchor": map[string]any{
			"Condition": "m.\"text_01\" = $2",
		},
		"SortKeys": []model.AttributeOrder{},
		"Limit":    "$3",
		"Offset":   "$4",
		"PageSize": "$3",
	})
	require.NoError(t, err)
	anchor := anchorCTESegment(t, sql)

	require.NotContains(t, anchor, "FROM change_log cl")
	require.NotContains(t, anchor, "-- Include real-time buffer rows from change_log")
	require.NotContains(t, anchor, "UNION")
}
