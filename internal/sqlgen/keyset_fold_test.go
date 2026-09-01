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
			if err := injectDuckDBTemplateParams(params, q, nil); err != nil {
				t.Fatalf("injectDuckDBTemplateParams: %v", err)
			}
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

// TestKeysetColumnsAreFolded pins #381 item 1's live bug: cursor columns must
// be emitted through ParquetAttrColumn like every other column reference, or a
// dotted attribute renders "contact.annualIncome" against a visible CTE that
// exposes only "contact_annualIncome" — parsed by DuckDB as table "contact",
// column "annualIncome" (#260).
func TestKeysetColumnsAreFolded(t *testing.T) {
	cursor := &model.KeysetCursor{
		Columns: []model.KeysetColumn{
			{Attribute: "contact.annualIncome", Direction: forma.SortOrderDesc},
			{Attribute: "row_id", Direction: forma.SortOrderAsc},
		},
		Values: []interface{}{int64(90000), "r1"},
		Mode:   model.KeysetCursorModeAfter,
	}

	where, args, err := generateKeysetWhereClause(cursor, "")
	if err != nil {
		t.Fatalf("generateKeysetWhereClause: %v", err)
	}
	if strings.Contains(where, "contact.annualIncome") {
		t.Errorf("WHERE clause emits the unfolded dotted name: %s", where)
	}
	if !strings.Contains(where, "contact_annualIncome") {
		t.Errorf("WHERE clause is missing the folded column: %s", where)
	}
	if len(args) != 3 { // n(n+1)/2 for a 2-column cursor
		t.Errorf("args = %d, want 3", len(args))
	}

	order := buildKeysetOrderBy(cursor)
	if strings.Contains(order, "contact.annualIncome") {
		t.Errorf("ORDER BY emits the unfolded dotted name: %s", order)
	}
	if order != "contact_annualIncome DESC, row_id ASC" {
		t.Errorf("ORDER BY = %q, want %q", order, "contact_annualIncome DESC, row_id ASC")
	}
}

// TestKeysetSystemColumnsSurviveTheFold guards the no-op half: the fold is the
// identity on every visible-CTE system column, so this change must not alter
// any cursor that production already renders.
func TestKeysetSystemColumnsSurviveTheFold(t *testing.T) {
	for _, attr := range []string{"row_id", "created_at", "ver_ts", "deleted_ts"} {
		if got := ParquetAttrColumn(attr); got != attr {
			t.Errorf("ParquetAttrColumn(%q) = %q, want the identity", attr, got)
		}
	}
	cursor := &model.KeysetCursor{
		Columns: []model.KeysetColumn{
			{Attribute: "created_at", Direction: forma.SortOrderDesc},
			{Attribute: "row_id", Direction: forma.SortOrderAsc},
		},
		Values: []interface{}{int64(5), "r1"},
		Mode:   model.KeysetCursorModeAfter,
	}
	if got := buildKeysetOrderBy(cursor); got != "created_at DESC, row_id ASC" {
		t.Errorf("ORDER BY = %q, want %q", got, "created_at DESC, row_id ASC")
	}
}

// TestKeysetWhereClauseRejectsMisalignedValues pins #381 item 7 at the codegen
// seam. The retired valueAt fallback returned nil for an index past the end of
// Values, binding SQL NULL; `col > NULL` is NULL, WHERE treats that as
// not-true, and every disjunct carrying the unfilled arm dropped out — the
// caller received a silently empty page instead of an error.
func TestKeysetWhereClauseRejectsMisalignedValues(t *testing.T) {
	cursor := &model.KeysetCursor{
		Columns: []model.KeysetColumn{
			{Attribute: "created_at", Direction: forma.SortOrderDesc},
			{Attribute: "row_id", Direction: forma.SortOrderAsc},
		},
		Values: []interface{}{int64(5)}, // one short
		Mode:   model.KeysetCursorModeAfter,
	}

	_, _, err := generateKeysetWhereClause(cursor, "")
	if err == nil {
		t.Fatal("generateKeysetWhereClause() = nil error, want a misalignment error")
	}
	if !strings.Contains(err.Error(), "carries 2 column(s) but 1 value(s)") {
		t.Errorf("err = %q, want it to name the column/value counts", err.Error())
	}

	params := map[string]any{}
	q := &model.FederatedAttributeQuery{KeysetCursor: cursor}
	if err := injectDuckDBTemplateParams(params, q, nil); err == nil {
		t.Error("injectDuckDBTemplateParams() = nil error, want the misalignment to propagate")
	}
}
