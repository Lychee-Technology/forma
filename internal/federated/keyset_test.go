package federated

import (
	"context"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

func TestValidateKeysetColumns_SystemColumnsSupported(t *testing.T) {
	supportedColumns := []model.KeysetColumn{
		{Attribute: "row_id", Direction: forma.SortOrderAsc},
		{Attribute: "created_at", Direction: forma.SortOrderDesc},
		{Attribute: "updated_at", Direction: forma.SortOrderAsc},
		{Attribute: "deleted_at", Direction: forma.SortOrderAsc},
		{Attribute: "ver_ts", Direction: forma.SortOrderDesc},
		{Attribute: "deleted_ts", Direction: forma.SortOrderAsc},
		{Attribute: "schema_id", Direction: forma.SortOrderAsc},
	}

	err := validateKeysetColumns(supportedColumns)
	if err != nil {
		t.Errorf("expected no error for supported columns, got: %v", err)
	}
}

func TestValidateKeysetColumns_EAVAttributeUnsupported(t *testing.T) {
	unsupportedColumns := []model.KeysetColumn{
		{Attribute: "created_at", Direction: forma.SortOrderDesc},
		{Attribute: "user_email", Direction: forma.SortOrderAsc}, // EAV attribute
		{Attribute: "row_id", Direction: forma.SortOrderAsc},
	}

	err := validateKeysetColumns(unsupportedColumns)
	if err == nil {
		t.Fatal("expected error for EAV attribute, got nil")
	}

	expectedMsg := "keyset pagination on attribute \"user_email\" is not supported"
	if !strings.Contains(err.Error(), expectedMsg) {
		t.Errorf("expected error message to contain %q, got: %v", expectedMsg, err)
	}
}

func TestValidateKeysetColumns_EmptyColumnsValid(t *testing.T) {
	err := validateKeysetColumns(nil)
	if err != nil {
		t.Errorf("expected no error for nil columns, got: %v", err)
	}

	err = validateKeysetColumns([]model.KeysetColumn{})
	if err != nil {
		t.Errorf("expected no error for empty columns, got: %v", err)
	}
}

func TestValidateKeysetTiebreak_TrailingRowIDAccepted(t *testing.T) {
	cursor := &model.KeysetCursor{
		Columns: []model.KeysetColumn{
			{Attribute: "created_at", Direction: forma.SortOrderDesc},
			{Attribute: "row_id", Direction: forma.SortOrderAsc},
		},
	}
	if err := validateKeysetTiebreak(cursor); err != nil {
		t.Errorf("expected no error for cursor ending on row_id, got: %v", err)
	}
}

func TestValidateKeysetTiebreak_RowIDOnlyAccepted(t *testing.T) {
	cursor := &model.KeysetCursor{
		Columns: []model.KeysetColumn{{Attribute: "row_id", Direction: forma.SortOrderAsc}},
	}
	if err := validateKeysetTiebreak(cursor); err != nil {
		t.Errorf("expected no error for row_id-only cursor, got: %v", err)
	}
}

func TestValidateKeysetTiebreak_MissingRowIDRejected(t *testing.T) {
	cursor := &model.KeysetCursor{
		Columns: []model.KeysetColumn{{Attribute: "created_at", Direction: forma.SortOrderDesc}},
	}
	err := validateKeysetTiebreak(cursor)
	if err == nil {
		t.Fatal("expected error for cursor lacking trailing row_id, got nil")
	}
	// The message must name the offending column and the expected state.
	if !strings.Contains(err.Error(), "created_at") {
		t.Errorf("expected error to name the offending column %q, got: %v", "created_at", err)
	}
	if !strings.Contains(err.Error(), "row_id") {
		t.Errorf("expected error to name the expected \"row_id\" tiebreak, got: %v", err)
	}
}

func TestValidateKeysetTiebreak_NonTrailingRowIDRejected(t *testing.T) {
	// row_id present but not last: the trailing "count" is still non-unique.
	cursor := &model.KeysetCursor{
		Columns: []model.KeysetColumn{
			{Attribute: "row_id", Direction: forma.SortOrderAsc},
			{Attribute: "created_at", Direction: forma.SortOrderDesc},
		},
	}
	if err := validateKeysetTiebreak(cursor); err == nil {
		t.Fatal("expected error when row_id is not the final column, got nil")
	}
}

func TestValidateKeysetTiebreak_EmptyAndNilNoOp(t *testing.T) {
	if err := validateKeysetTiebreak(nil); err != nil {
		t.Errorf("expected no error for nil cursor, got: %v", err)
	}
	if err := validateKeysetTiebreak(&model.KeysetCursor{}); err != nil {
		t.Errorf("expected no error for empty cursor, got: %v", err)
	}
}

// TestPaginatedQueryTakesKeysetPathOnCursorAlone pins #381 item 3. The keyset
// branch used to require opts.KeysetEnabled as well as a cursor; with the flag
// unset, control fell into the in-memory merge, where RunOptimizedQuery
// applies no cursor while the DuckDB leg does (the renderer keys off
// q.KeysetCursor, never off the flag). The merged page was then
// hot-rows-unfiltered union cold-rows-filtered — the same silent-wrong-answer
// family as #354. An active cursor alone now selects the keyset path.
func TestPaginatedQueryTakesKeysetPathOnCursorAlone(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{}}
	engine := NewDBFederatedQueryEngine(pg, nil, nil, nil, forma.DuckDBConfig{Enabled: true}, nil, "")

	fq := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
		KeysetCursor: &model.KeysetCursor{
			Columns: []model.KeysetColumn{
				{Attribute: "created_at", Direction: forma.SortOrderDesc},
				// No trailing row_id: the keyset path validates and refuses,
				// while the in-memory merge path would have run happily. The
				// refusal is therefore proof of which branch was taken,
				// without needing a live DuckDB.
			},
			Values: []interface{}{int64(5)},
			Mode:   model.KeysetCursorModeAfter,
		},
	}

	_, _, err := engine.ExecuteFederatedPaginatedQuery(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav"},
		fq, 10, 0, nil, &model.FederatedQueryOptions{})

	require.Error(t, err, "an active cursor alone must select the keyset path")
	require.Contains(t, err.Error(), `expected "row_id"`)
	require.Zero(t, pg.runOptimizedCalls,
		"the keyset path runs, not the in-memory merge path which calls RunOptimizedQuery")
}
