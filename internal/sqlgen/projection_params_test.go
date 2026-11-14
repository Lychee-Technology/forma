package sqlgen

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// Shared projection fixtures for renderer/compiler tests (#222). The
// parquet-only template refuses to render without its schema-projection
// params (requireParquetOnlyProjection), so every test driving it must derive
// them from a real BuildSchemaProjection call — never hand-written SQL, which
// is exactly the drift the retired toy defaults caused.

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

// applyProjectionParams derives the projection params from the given cache
// and copies them into params, returning it for chaining.
func applyProjectionParams(t *testing.T, params map[string]any, schemaID int16, cache forma.SchemaAttributeCache) map[string]any {
	t.Helper()
	sp, err := BuildSchemaProjection(schemaID, cache)
	require.NoError(t, err)
	params["S3SourceSelect"] = sp.S3SourceSelect
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
// template param map scans.
const testRenderParquetPath = "s3://test-bucket/parquet/data.parquet"

// injectTestRenderParams prepares a fully renderable template param map: the
// canonical projection plus a resolved parquet path set. Since #299 the
// renderer rejects an unbound S3_PATHS (requireS3Paths) rather than letting
// read_parquet(<no value>) reach DuckDB, mirroring the engine's own refusal to
// scan an empty path set — so every test that renders the template must
// author a path, exactly as production does.
func injectTestRenderParams(t *testing.T, params map[string]any, schemaID int16) map[string]any {
	t.Helper()
	injectTestProjection(t, params, schemaID)
	params["DuckDBS3Paths"] = []string{testRenderParquetPath}
	return params
}
