package sqlgen

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// TestAdvancedTemplate_KeysetAppliedAfterDedup pins the #212 fix: the keyset
// cursor predicate must render in the visible CTE after rn = 1, never in the
// ranked CTE, where it would filter row versions before LWW dedup and let a
// superseded version resurrect (the keyset twin of #173).
func TestAdvancedTemplate_KeysetAppliedAfterDedup(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 1, Limit: 10},
		KeysetCursor: &model.KeysetCursor{
			Columns: []model.KeysetColumn{{Attribute: "count", Direction: forma.SortOrderAsc}},
			Values:  []any{float64(500)},
			Mode:    model.KeysetCursorModeAfter,
		},
	}
	dual := &DualClauses{DuckClause: "1=1"}

	sql, _, err := BuildDuckDBQuery(AdvancedQueryTemplateDuckDB, injectTestRenderParams(t, map[string]any{}, 1), q, nil, dual)
	require.NoError(t, err)

	rankedStart := strings.Index(sql, "ranked AS")
	visibleStart := strings.Index(sql, "visible AS")
	require.Greater(t, rankedStart, -1)
	require.Greater(t, visibleStart, rankedStart)

	rankedBody := sql[rankedStart:visibleStart]
	require.NotContains(t, rankedBody, "count >",
		"cursor must not filter row versions before ROW_NUMBER dedup (#212)")

	visibleBody := sql[visibleStart:]
	rnIdx := strings.Index(visibleBody, "rn = 1")
	keysetIdx := strings.Index(visibleBody, "count >")
	require.Greater(t, rnIdx, -1)
	require.Greater(t, keysetIdx, rnIdx, "cursor must apply after rn = 1 (#212)")
}
