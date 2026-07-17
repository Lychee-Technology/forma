package sqlgen

import (
	"testing"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// Shared projection fixtures for renderer/compiler tests (#222). The advanced
// template refuses to render without the seven schema-projection params
// (requireProjectionParams), so every test driving it must derive them from a
// real BuildSchemaProjection call — never hand-written SQL, which is exactly
// the drift the retired toy defaults caused.

// testProjectionCache is the canonical name/age/tag fixture: a text main
// column, an integer main column, and one EAV attribute (id 205) — the same
// shape the design-doc §5 guard uses (internal/federated/design_doc_sql_test.go).
func testProjectionCache() forma.SchemaAttributeCache {
	return forma.SchemaAttributeCache{
		"name": {AttributeID: 1, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_01")}},
		"age": {AttributeID: 2, ValueType: forma.ValueTypeInteger,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("integer_01")}},
		"tag": {AttributeID: 205, ValueType: forma.ValueTypeText},
	}
}

// applyProjectionParams derives the seven projection params from the given
// cache and copies them into params, returning it for chaining.
func applyProjectionParams(t *testing.T, params map[string]any, schemaID int16, cache forma.SchemaAttributeCache) map[string]any {
	t.Helper()
	sp, err := BuildSchemaProjection(schemaID, cache)
	require.NoError(t, err)
	params["S3SourceSelect"] = sp.S3SourceSelect
	params["PGSourceSelect"] = sp.PGSourceSelect
	params["PGGroupBy"] = sp.PGGroupBy
	params["EAVPivotSelect"] = sp.EAVPivotSelect
	params["EAVPivotAttrs"] = sp.EAVPivotAttrs
	params["HasEAVPivot"] = sp.EAVPivotAttrs != ""
	params["OuterSelect"] = sp.OuterSelect
	return params
}

// withTestProjection injects the canonical fixture projection into params.
func withTestProjection(t *testing.T, params map[string]any, schemaID int16) map[string]any {
	t.Helper()
	return applyProjectionParams(t, params, schemaID, testProjectionCache())
}

// TestBuildDuckDBQuery_MissingProjectionParamsRejected pins the retirement of
// the toy-schema defaults: an advanced-template render without the schema
// projection params must fail naming every missing key — never silently
// substitute a projection duckDBScanBuffers cannot scan.
func TestBuildDuckDBQuery_MissingProjectionParamsRejected(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 1, Limit: 10},
	}
	dual := &DualClauses{DuckClause: "1=1"}

	_, _, err := BuildDuckDBQuery(AdvancedQueryTemplateDuckDB, map[string]any{}, q, nil, dual)
	require.Error(t, err)
	for _, key := range projectionParamKeys {
		require.Contains(t, err.Error(), key)
	}
	require.Contains(t, err.Error(), "#222")

	// One key present, six missing: only the absent keys are named.
	_, _, err = BuildDuckDBQuery(AdvancedQueryTemplateDuckDB,
		map[string]any{"S3SourceSelect": "row_id"}, q, nil, dual)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "[S3SourceSelect")
	require.Contains(t, err.Error(), "OuterSelect")

	// The legacy dual=nil path guards the advanced template the same way.
	_, _, err = BuildDuckDBQuery(AdvancedQueryTemplateDuckDB, map[string]any{}, q, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "S3SourceSelect")
}

// TestCompileDuckDBQuery_MissingProjectionParamsRejected mirrors the guard on
// the compiled-plan entry point.
func TestCompileDuckDBQuery_MissingProjectionParamsRejected(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 1, Limit: 10},
	}
	dual := &DualClauses{DuckClause: "1=1"}

	compiled, err := CompileDuckDBQuery(AdvancedQueryTemplateDuckDB, map[string]any{}, q, dual, false)
	require.Error(t, err)
	require.Nil(t, compiled)
	require.Contains(t, err.Error(), "S3SourceSelect")
}
