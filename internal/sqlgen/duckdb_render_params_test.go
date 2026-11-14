package sqlgen

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// TestParquetOnlyTemplate_NoResourcePragmas locks the #104 contract: resource
// pragmas (threads / memory_limit) are connection-level configuration
// (DuckDBConfig via applyResourcePragmas), never per-query SQL — a template
// pragma executes on every query and silently overrides the configured
// connection-level values.
func TestParquetOnlyTemplate_NoResourcePragmas(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 1, Limit: 10, Offset: 0},
	}
	dual := DualClauses{DuckClause: "1=1"}

	sql, _, err := BuildParquetOnlyQuery(injectTestRenderParams(t, map[string]any{}, 1), q, dual, ParquetWindow{Limit: 10})
	require.NoError(t, err)
	require.NotContains(t, sql, "PRAGMA")
}

// Test RenderS3ParquetPath templating.
func TestRenderS3ParquetPath_Render(t *testing.T) {
	got, err := RenderS3ParquetPath("s3://bucket/schema_{{.SchemaID}}/data.parquet", 42)
	require.NoError(t, err)
	require.Equal(t, "s3://bucket/schema_42/data.parquet", got)

	_, err = RenderS3ParquetPath("", 1)
	require.Error(t, err)
}

// TestInjectDuckDBTemplateParams_KeysetPositionalPlaceholders pins the #212
// placeholder contract: the keyset clause uses positional "?" only (never
// "$n" — see the #161 regression test for why mixing mis-binds), and prefix
// columns that repeat across disjuncts contribute one arg per occurrence.
func TestInjectDuckDBTemplateParams_KeysetPositionalPlaceholders(t *testing.T) {
	cursor := &model.KeysetCursor{
		Columns: []model.KeysetColumn{
			{Attribute: "created_at", Direction: forma.SortOrderDesc, ExportType: "BIGINT"},
			{Attribute: "row_id", Direction: forma.SortOrderDesc, ExportType: "UUID"},
		},
		Values: []interface{}{int64(999), "11111111-1111-1111-1111-111111111111"},
		Mode:   model.KeysetCursorModeAfter,
	}
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 1, Limit: 10, Offset: 0},
		KeysetCursor:   cursor,
	}
	params := map[string]any{}
	require.NoError(t, injectDuckDBTemplateParams(params, q, nil))

	clause, ok := params["KEYSET_WHERE_CLAUSE"].(string)
	require.True(t, ok, "KEYSET_WHERE_CLAUSE should be a string")
	require.Equal(t, "(created_at < CAST(? AS BIGINT)) OR (created_at = CAST(? AS BIGINT) AND row_id < CAST(? AS UUID))", clause)
	require.NotContains(t, clause, "$")

	args, ok := params["KEYSET_ARGS"].([]interface{})
	require.True(t, ok, "KEYSET_ARGS should be a slice")
	require.Equal(t, []interface{}{
		int64(999), int64(999), "11111111-1111-1111-1111-111111111111",
	}, args, "prefix values repeat once per placeholder occurrence")
}

// TestBuildParquetOnlyQuery_KeysetArgsBindAfterConditions pins the end-to-end
// interleave for a keyset query: [DuckArgs, DuckArgs, keysetArgs, window
// limit], one "?" per arg in appearance order, no "$n" anywhere (#212).
func TestBuildParquetOnlyQuery_KeysetArgsBindAfterConditions(t *testing.T) {
	// The cursor carries the mandatory row_id tiebreak (#183): a cursor
	// ending on a non-unique key is refused by ValidateShape, which
	// generateKeysetWhereClause now enforces (#381 item 7).
	cursor := &model.KeysetCursor{
		Columns: []model.KeysetColumn{
			{Attribute: "created_at", Direction: forma.SortOrderDesc, ExportType: "BIGINT"},
			{Attribute: "row_id", Direction: forma.SortOrderDesc, ExportType: "UUID"},
		},
		Values: []interface{}{int64(1000), "11111111-1111-1111-1111-111111111111"},
		Mode:   model.KeysetCursorModeAfter,
	}
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 1, Limit: 10, Offset: 0},
		KeysetCursor:   cursor,
	}
	dual := DualClauses{
		DuckClause: "age > ? AND name = ?",
		DuckArgs:   []any{int64(25), "Alice"},
	}

	sql, args, err := BuildParquetOnlyQuery(injectTestRenderParams(t, map[string]any{}, 1), q, dual, ParquetWindow{Limit: 10})
	require.NoError(t, err)

	// [DuckArgs(2), DuckArgs(2), keyset(3), window(1)] — a 2-column cursor
	// contributes n(n+1)/2 args.
	require.Len(t, args, 8)
	require.Equal(t, []any{int64(25), "Alice", int64(25), "Alice"}, args[:4])
	require.Equal(t, []any{
		int64(1000), int64(1000), "11111111-1111-1111-1111-111111111111",
	}, args[4:7], "keyset values must follow the condition args, in placeholder order")
	require.Equal(t, int64(10), args[7], "the window limit binds last")
	require.NotContains(t, sql, "$", "keyset queries must not reintroduce $n placeholders")
	require.Equal(t, 8, strings.Count(sql, "?"),
		"placeholder count must equal arg count for positional binding")
}
