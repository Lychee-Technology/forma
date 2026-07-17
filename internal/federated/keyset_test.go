package federated

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/lychee-technology/forma"
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
