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

// TestOptimizedQueryTemplate_AnchorConditionParenthesized (#269): every
// {{.Anchor.Condition}} injection site joins the schema_id constraint with
// AND, so the injected condition must be wrapped in parentheses. Without
// them a top-level multi-branch OR parses as
// `(schema_id = $1 AND branch1) OR branch2 ...` by operator precedence, and
// every branch after the first scans unconstrained by the requested schema
// (polluted anchor: inflated total_records, pagination drift).
func TestOptimizedQueryTemplate_AnchorConditionParenthesized(t *testing.T) {
	cases := []struct {
		name          string
		useMainAnchor bool
		condition     string
	}{
		{
			name:          "main_anchor",
			useMainAnchor: true,
			condition:     `(m."integer_01" = $2) OR (m."integer_01" = $3)`,
		},
		{
			name:          "eav_anchor",
			useMainAnchor: false,
			condition:     `(EXISTS (SELECT 1 FROM x)) OR (EXISTS (SELECT 1 FROM y))`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sql, err := renderTemplate(optimizedQuerySQLTemplate, map[string]any{
				"EAVTable":             "eav_data",
				"MainTable":            "entity_main",
				"ChangeLogTable":       "change_log",
				"MainProjection":       "m.ltbase_schema_id, m.ltbase_row_id",
				"SchemaID":             "$1",
				"UseMainTableAsAnchor": tc.useMainAnchor,
				"Anchor": map[string]any{
					"Condition": tc.condition,
				},
				"SortKeys": []model.AttributeOrder{},
				"Limit":    "$4",
				"Offset":   "$5",
				"PageSize": "$4",
			})
			require.NoError(t, err)
			anchor := anchorCTESegment(t, sql)

			// Each anchor variant injects the condition twice: the anchor scan
			// and the change_log EXISTS branch. Both must parenthesize it.
			require.Equal(t, 2, strings.Count(anchor, "AND ("+tc.condition+")"),
				"anchor CTE must wrap the injected condition in parens at both sites:\n%s", anchor)
			require.NotContains(t, anchor, "AND "+tc.condition+"\n",
				"anchor CTE must not inject the condition bare:\n%s", anchor)
		})
	}
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
