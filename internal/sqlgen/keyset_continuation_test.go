package sqlgen

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"

	"github.com/stretchr/testify/require"
)

func continuationQuery(cursorAttr string, cursorDir forma.SortOrder) *model.FederatedAttributeQuery {
	return &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			SchemaID: 1,
			Limit:    10,
			AttributeOrders: []model.AttributeOrder{{
				AttrID: 42, AttrName: "count", ValueType: forma.ValueTypeNumeric,
				SortOrder: forma.SortOrderAsc, StorageLocation: forma.AttributeStorageLocationEAV,
			}},
		},
		KeysetCursor: &model.KeysetCursor{
			Columns: []model.KeysetColumn{
				{Attribute: cursorAttr, Direction: cursorDir},
				{Attribute: "row_id", Direction: forma.SortOrderAsc},
			},
			Values: []interface{}{int64(5), "r1"},
			Mode:   model.KeysetCursorModeAfter,
		},
	}
}

// TestBuildDuckDBQuery_KeysetMustContinueAttributeOrders pins the renderer's
// copy of the continuation rule (#381 review): the keyset ORDER BY renders
// from the cursor alone, so a cursor that does not continue q.AttributeOrders
// is refused here as well as at the federated seams, and a continuing cursor
// renders the very order page one was sorted by.
func TestBuildDuckDBQuery_KeysetMustContinueAttributeOrders(t *testing.T) {
	dual := &DualClauses{DuckClause: "1=1"}

	_, _, err := BuildDuckDBQuery(AdvancedQueryTemplateDuckDB, injectTestRenderParams(t, map[string]any{}, 1),
		continuationQuery("created_at", forma.SortOrderDesc), nil, dual)
	require.ErrorContains(t, err, `keyset cursor column 1 is "created_at" but the request sorts on "count"`)

	_, _, err = BuildDuckDBQuery(AdvancedQueryTemplateDuckDB, injectTestRenderParams(t, map[string]any{}, 1),
		continuationQuery("count", forma.SortOrderDesc), nil, dual)
	require.ErrorContains(t, err, `keyset cursor column 1 ("count") is descending but the request sorts "count" ascending`)

	sql, _, err := BuildDuckDBQuery(AdvancedQueryTemplateDuckDB, injectTestRenderParams(t, map[string]any{}, 1),
		continuationQuery("count", forma.SortOrderAsc), nil, dual)
	require.NoError(t, err)
	require.Contains(t, sql, "ORDER BY count ASC, row_id ASC", "a continuing cursor renders the request's order")
}
