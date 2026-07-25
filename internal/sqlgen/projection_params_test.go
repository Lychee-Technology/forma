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

// buildTestProjectionCache is the canonical name/age/tag fixture: a text main
// column, an integer main column, and one EAV attribute (id 205) — the same
// shape the design-doc §5 guard uses (internal/federated/design_doc_sql_test.go).
func buildTestProjectionCache() forma.SchemaAttributeCache {
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

// injectTestProjection injects the canonical fixture projection into params.
// Projection only — the result is NOT renderable on its own; see
// injectTestRenderParams.
func injectTestProjection(t *testing.T, params map[string]any, schemaID int16) map[string]any {
	t.Helper()
	return applyProjectionParams(t, params, schemaID, buildTestProjectionCache())
}

// testRenderParquetPath is the fixture parquet object every renderable
// advanced-template param map scans.
const testRenderParquetPath = "s3://test-bucket/parquet/data.parquet"

// injectTestRenderParams prepares a fully renderable advanced-template param
// map: the canonical projection plus a resolved parquet path set. Since #299
// the renderer rejects an unbound S3_PATHS (requireS3Paths) rather than letting
// read_parquet(<no value>) reach DuckDB, mirroring the engine's own refusal to
// scan an empty path set — so every test that renders the advanced template
// must author a path, exactly as production does.
func injectTestRenderParams(t *testing.T, params map[string]any, schemaID int16) map[string]any {
	t.Helper()
	injectTestProjection(t, params, schemaID)
	params["DuckDBS3Paths"] = []string{testRenderParquetPath}
	return params
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

// TestBuildDuckDBQuery_MissingS3PathsRejected pins #299's second layer: the
// advanced template renders read_parquet({{.S3_PATHS}}) unconditionally, so an
// unbound S3_PATHS used to reach DuckDB as the literal "<no value>" and die as
// a parser error that classified like a transient outage. The engine now fails
// an empty path set before rendering; this guard makes the bad render
// unreachable from any caller, not just from the engine.
func TestBuildDuckDBQuery_MissingS3PathsRejected(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 1, Limit: 10},
	}
	dual := &DualClauses{DuckClause: "1=1"}

	params := injectTestProjection(t, map[string]any{}, 1)
	sql, _, err := BuildDuckDBQuery(AdvancedQueryTemplateDuckDB, params, q, nil, dual)
	require.Error(t, err)
	require.Empty(t, sql)
	require.Contains(t, err.Error(), "S3_PATHS")
	require.Contains(t, err.Error(), "#299")

	// The legacy dual=nil path guards the advanced template the same way.
	_, _, err = BuildDuckDBQuery(AdvancedQueryTemplateDuckDB,
		injectTestProjection(t, map[string]any{}, 1), q, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "S3_PATHS")

	// With paths bound the render succeeds and the scan carries the real path.
	// Scoped to read_parquet deliberately: this minimal fixture also leaves the
	// Postgres-side params (PG_CONN, scan tables) unbound, so the rendered SQL
	// still shows <no value> in its postgres_scan calls. Production always binds
	// those from StorageTables — an adjacent hole to the one #299 closes here,
	// and not this test's subject.
	ok := injectTestProjection(t, map[string]any{}, 1)
	ok["DuckDBS3Paths"] = []string{"s3://b/1/a.parquet"}
	sql, _, err = BuildDuckDBQuery(AdvancedQueryTemplateDuckDB, ok, q, nil, dual)
	require.NoError(t, err)
	require.NotContains(t, sql, "read_parquet(<no value>")
	require.Contains(t, sql, "read_parquet('s3://b/1/a.parquet'")
}

// TestCompileDuckDBQuery_MissingS3PathsRejected mirrors the path guard on the
// compiled-plan entry point, which renders the same skeleton.
func TestCompileDuckDBQuery_MissingS3PathsRejected(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 1, Limit: 10},
	}
	dual := &DualClauses{DuckClause: "1=1"}

	compiled, err := CompileDuckDBQuery(AdvancedQueryTemplateDuckDB,
		injectTestProjection(t, map[string]any{}, 1), q, dual, false)
	require.Error(t, err)
	require.Nil(t, compiled)
	require.Contains(t, err.Error(), "S3_PATHS")
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
