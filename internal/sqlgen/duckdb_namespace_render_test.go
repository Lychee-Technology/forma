package sqlgen

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// nonBenchmarkNamespaceParams builds render params for a non-benchmark schema
// (id 7) with the real schema projection injected, so the rendered SQL contains
// the actual attribute-named CTE columns (`age`, `tag`).
func nonBenchmarkNamespaceParams(t *testing.T, cache forma.SchemaAttributeCache) map[string]any {
	t.Helper()
	return applyProjectionParams(t, map[string]any{"DuckDBS3Paths": []string{testRenderParquetPath}}, 7, cache)
}

// TestDuckDBNamespace_RenderPaths_AttributeAliasedClause is the #167 render-path
// coverage: it drives the changed column resolution through the parquet-only
// compile and bind for a NON-benchmark schema with a main-column + pure-EAV
// composite, with the real schema projection injected and a sort on the
// column-bound attribute present.
//
// It asserts (a) the DuckClause emitted into the s3_source/visible WHERE uses the
// attribute names that the projection aliases the CTE columns by (`age`, `tag`),
// never the physical entity_main column (`integer_01`); (b) only the outer
// SELECT maps the attribute back to that physical column; and (c) the ORDER BY
// on that attribute reads its visible column by the attribute name too.
func TestDuckDBNamespace_RenderPaths_AttributeAliasedClause(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"age": {AttributeID: 6, ValueType: forma.ValueTypeInteger,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("integer_01")}},
		"tag": {AttributeID: 7, ValueType: forma.ValueTypeText},
	}
	cond := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
		&forma.KvCondition{Attr: "age", Value: "gt:10"},
		&forma.KvCondition{Attr: "tag", Value: "equals:x"},
	}}
	q := &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{
		SchemaID:  7,
		Condition: cond,
		AttributeOrders: []model.AttributeOrder{{
			StorageLocation: forma.AttributeStorageLocationMain,
			ColumnName:      "integer_01",
			AttrName:        "age",
			SortOrder:       forma.SortOrderDesc,
		}},
	}}

	idx := 0
	dc, err := ToDualClauses(cond, "eav_t", 7, cache, &idx)
	require.NoError(t, err)

	// The clause references attribute names (matching the CTE aliases), not the
	// physical column the attribute is bound to.
	require.Equal(t, "(age > CAST(? AS DOUBLE)) AND (tag = ?)", dc.DuckClause)
	require.NotContains(t, dc.DuckClause, "integer_01")

	sql, _, err := BuildParquetOnlyQuery(nonBenchmarkNamespaceParams(t, cache), q, dc, ParquetWindow{Limit: 25})
	require.NoError(t, err)

	// The attribute-aliased clause flows into the DuckDB CTE WHERE clauses.
	require.Equal(t, 2, strings.Count(sql, dc.DuckClause), "the render must embed the attribute-named DuckClause at both sites")
	// The CTE carries the column-bound attribute under its attribute name, and
	// only the outer select maps it back to the physical column.
	require.Contains(t, sql, "deleted_at AS deleted_ts, age, tag,", "s3_source must project the attribute-named parquet column")
	require.Contains(t, sql, "CAST(age AS INTEGER) AS integer_01", "the outer select must alias the attribute to its physical column")
	// The sort on the column-bound attribute reads its visible column.
	require.Contains(t, sql, "ORDER BY visible.age DESC NULLS LAST", "ORDER BY must read the attribute's visible column")
}
