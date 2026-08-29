package sqlgen

import (
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// nonBenchmarkNamespaceParams builds render params for a non-benchmark schema
// (id 7) with the real schema projection injected, so the rendered SQL contains
// the actual attribute-aliased CTE columns (`... AS age`, `... AS tag`).
func nonBenchmarkNamespaceParams(t *testing.T, cache forma.SchemaAttributeCache) map[string]any {
	t.Helper()
	return applyProjectionParams(t, compiledParityParams(t), 7, cache)
}

// TestDuckDBNamespace_RenderPaths_AttributeAliasedClause is the #167 render-path
// coverage: it drives the changed column resolution through BOTH the direct
// BuildDuckDBQuery renderer and the compiled-plan CompileDuckDBQuery/Bind
// renderer for a NON-benchmark schema with a main-column + pure-EAV composite,
// with the real schema projection injected and a main-column ORDER BY present.
//
// It asserts (a) the DuckClause emitted into the s3_source/visible WHERE uses the
// attribute names that the projection aliases the CTE columns by (`age`, `tag`),
// never the physical entity_main column (`integer_01`); (b) ORDER BY on a
// main-column sort key is unaffected — it stays the physical descriptor name
// (`integer_01`), which the outer SELECT aliases; and (c) both render paths agree.
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

	dirty := []uuid.UUID{uuid.New()}

	// Direct renderer.
	builtSQL, builtArgs, err := BuildDuckDBQuery(AdvancedQueryTemplateDuckDB, nonBenchmarkNamespaceParams(t, cache), q, dirty, &dc)
	require.NoError(t, err)

	// Compiled-plan renderer.
	compiled, err := CompileDuckDBQuery(AdvancedQueryTemplateDuckDB, nonBenchmarkNamespaceParams(t, cache), q, &dc, true)
	require.NoError(t, err)
	require.NotNil(t, compiled, "non-benchmark column-bound composite must be cacheable")
	boundSQL, boundArgs := compiled.Bind(q, dc, dirty, FlushGraceCutoffDisabled)

	for name, sql := range map[string]string{"direct": builtSQL, "compiled": boundSQL} {
		// The attribute-aliased clause flows into the DuckDB CTE WHERE clauses.
		require.Contains(t, sql, dc.DuckClause, "%s render must embed the attribute-named DuckClause", name)
		// The projection aliases the column-bound attribute by its attribute name.
		require.Contains(t, sql, "AS age", "%s render must project the CTE column as the attribute name", name)
		// ORDER BY on the main-column sort key is unaffected by the DuckClause fix:
		// it references the physical descriptor name the outer SELECT aliases.
		require.Contains(t, sql, "ORDER BY integer_01 DESC", "%s render ORDER BY must stay the outer physical alias", name)
	}

	// Both render paths must agree for this non-benchmark column-bound shape.
	require.Equal(t, builtSQL, boundSQL, "compiled Bind must equal the direct build")
	require.Equal(t, builtArgs, boundArgs)
}
