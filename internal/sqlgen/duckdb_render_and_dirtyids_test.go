package sqlgen

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// Test RenderS3ParquetPath templating.
func TestRenderS3ParquetPath_Render(t *testing.T) {
	got, err := RenderS3ParquetPath("s3://bucket/schema_{{.SchemaID}}/data.parquet", 42)
	require.NoError(t, err)
	require.Equal(t, "s3://bucket/schema_42/data.parquet", got)

	_, err = RenderS3ParquetPath("", 1)
	require.Error(t, err)
}

// Test RenderDirtyIDsValuesCSV produces correct VALUES list.
func TestRenderDirtyIDsValuesCSV(t *testing.T) {
	u1 := uuid.New()
	u2 := uuid.New()
	csv := RenderDirtyIDsValuesCSV([]uuid.UUID{u1, u2})
	require.Contains(t, csv, "('"+u1.String()+"')")
	require.Contains(t, csv, "('"+u2.String()+"')")
}

// Test AppendDirtyExclusion builds clause and args.
func TestAppendDirtyExclusion(t *testing.T) {
	base := "age > 18"
	u1 := uuid.New()
	u2 := uuid.New()
	clause, args := AppendDirtyExclusion(base, []uuid.UUID{u1, u2})
	require.Contains(t, clause, "age > 18")
	require.Contains(t, clause, "row_id NOT IN (")
	require.Len(t, args, 2)
	require.Equal(t, u1.String(), args[0])
}

// TestInjectDuckDBTemplateParams_KeysetParamOffset verifies that when a non-zero
// keysetParamOffset is passed, the keyset WHERE clause uses $offset+1, $offset+2, …
// placeholders so they align with keyset values appended after existing whereArgs.
func TestInjectDuckDBTemplateParams_KeysetParamOffset_NonZero(t *testing.T) {
	cursor := &model.KeysetCursor{
		Columns: []model.KeysetColumn{
			{Attribute: "created_at", Direction: forma.SortOrderAsc},
		},
		Values: []interface{}{int64(999)},
		Mode:   model.KeysetCursorModeAfter,
	}
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 1, Limit: 10, Offset: 0},
		KeysetCursor:   cursor,
	}
	params := map[string]any{}

	// Simulate 3 args already accumulated in whereArgs before keyset args are appended.
	injectDuckDBTemplateParams(params, q, nil, 3)

	// Keyset clause should start at $4 (offset=3 means first keyset param is $4).
	keysetClause, ok := params["KEYSET_WHERE_CLAUSE"].(string)
	require.True(t, ok, "KEYSET_WHERE_CLAUSE should be a string")
	require.Contains(t, keysetClause, "$4",
		"keyset placeholder must start at paramOffset+1 = $4")
	require.NotContains(t, keysetClause, "$1",
		"keyset placeholder must not collide with prior arg positions")
}

// TestBuildDuckDBQuery_KeysetArgPositionIsCorrect verifies that when a dual-clause
// query with 2 DuckDB filter args is combined with a keyset cursor, the keyset
// placeholder in the rendered SQL starts at the correct position and the keyset
// value appears at the matching position in the returned args slice.
func TestBuildDuckDBQuery_KeysetArgPositionIsCorrect(t *testing.T) {
	cursor := &model.KeysetCursor{
		Columns: []model.KeysetColumn{{Attribute: "created_at", Direction: forma.SortOrderDesc}},
		Values:  []interface{}{int64(1000)},
		Mode:    model.KeysetCursorModeAfter,
	}
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 1, Limit: 10, Offset: 0},
		KeysetCursor:   cursor,
	}
	// Two DuckDB filter args; no PG-main args.
	dual := &DualClauses{
		DuckClause: "age > ? AND name = ?",
		DuckArgs:   []any{int64(25), "Alice"},
	}
	params := map[string]any{}

	sql, args, err := BuildDuckDBQuery(AdvancedQueryTemplateDuckDB, params, q, nil, dual)
	require.NoError(t, err)

	// For the advanced dual path args are:
	//   [DuckArgs(2), PgMainArgs(0), DuckArgs(2), keysetArgs(1)]
	// So keyset is at position 4 (0-indexed) → $5.
	require.GreaterOrEqual(t, len(args), 5, "args must include keyset value at position 4")
	require.Equal(t, int64(1000), args[4], "keyset value must be at args[4]")
	require.True(t,
		strings.Contains(sql, "$5"),
		"rendered SQL must reference keyset value as $5, got SQL:\n%s", sql)
}
