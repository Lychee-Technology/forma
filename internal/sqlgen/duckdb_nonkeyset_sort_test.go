package sqlgen

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"

	"github.com/stretchr/testify/require"
)

// Non-keyset ORDER BY rendering for the advanced DuckDB template. Split out of
// duckdb_query_edge_cases_test.go, which was over the 500-line cap before this
// change and which #299 touched here (coding-standard.md: fix the immediate
// area when you touch non-compliant code).

func TestBuildDuckDBQuery_NonKeysetSort_RespectsAttributeOrders(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			SchemaID: 1,
			Limit:    10,
			Offset:   0,
			AttributeOrders: []model.AttributeOrder{
				{
					AttrID:          42,
					ValueType:       forma.ValueTypeNumeric,
					SortOrder:       forma.SortOrderAsc,
					StorageLocation: forma.AttributeStorageLocationMain,
					ColumnName:      "integer_01",
				},
			},
		},
		// No model.KeysetCursor → non-keyset path
	}

	dual := &DualClauses{
		DuckClause: "1=1",
		DuckArgs:   nil,
	}

	params := injectTestRenderParams(t, map[string]any{}, 1)
	sql, _, err := BuildDuckDBQuery(AdvancedQueryTemplateDuckDB, params, q, nil, dual)
	require.NoError(t, err)

	// The non-keyset ORDER BY must use the requested sort column, not "created_at DESC".
	require.Contains(t, sql, "integer_01 ASC",
		"non-keyset ORDER BY should reflect AttributeOrders[0]")
	require.NotContains(t, sql, "ORDER BY created_at DESC",
		"hardcoded fallback must not appear when AttributeOrders is populated")
}

// TestBuildDuckDBQuery_NonKeysetSort_FallbackWhenNoOrders verifies that the default
// "created_at DESC" fallback is still used when no AttributeOrders are specified.
func TestBuildDuckDBQuery_NonKeysetSort_FallbackWhenNoOrders(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			SchemaID: 1,
			Limit:    10,
			Offset:   0,
		},
	}

	dual := &DualClauses{
		DuckClause: "1=1",
		DuckArgs:   nil,
	}

	params := injectTestRenderParams(t, map[string]any{}, 1)
	sql, _, err := BuildDuckDBQuery(AdvancedQueryTemplateDuckDB, params, q, nil, dual)
	require.NoError(t, err)

	require.Contains(t, sql, "created_at DESC",
		"fallback ORDER BY should be 'created_at DESC' when no AttributeOrders supplied")
}
