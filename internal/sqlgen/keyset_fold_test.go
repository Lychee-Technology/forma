package sqlgen

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// TestRendererKeysetGateMatchesIsActive pins that the renderer's decision to
// emit the keyset clause is exactly model.KeysetCursor.IsActive (#381 item 9).
// A renderer that ignored an active cursor would answer an unfiltered page.
func TestRendererKeysetGateMatchesIsActive(t *testing.T) {
	cases := []struct {
		name   string
		cursor *model.KeysetCursor
	}{
		{"nil cursor", nil},
		{"empty columns", &model.KeysetCursor{Mode: model.KeysetCursorModeAfter}},
		{"active cursor", &model.KeysetCursor{
			Columns: []model.KeysetColumn{{Attribute: "row_id", Direction: forma.SortOrderAsc}},
			Values:  []interface{}{"r1"},
			Mode:    model.KeysetCursorModeAfter,
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			params := map[string]any{}
			q := &model.FederatedAttributeQuery{KeysetCursor: tc.cursor}
			injectDuckDBTemplateParams(params, q, nil)
			got, _ := params["HAS_KEYSET"].(bool)
			if want := tc.cursor.IsActive(); got != want {
				t.Errorf("HAS_KEYSET = %v, want IsActive() = %v", got, want)
			}
			if got && !strings.Contains(params["KEYSET_WHERE_CLAUSE"].(string), "row_id") {
				t.Errorf("active cursor rendered no row_id predicate: %v", params["KEYSET_WHERE_CLAUSE"])
			}
		})
	}
}
