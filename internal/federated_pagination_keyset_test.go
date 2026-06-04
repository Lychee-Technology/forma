package internal

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
)

func TestValidateKeysetColumns_SystemColumnsSupported(t *testing.T) {
	supportedColumns := []KeysetColumn{
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
	unsupportedColumns := []KeysetColumn{
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

	err = validateKeysetColumns([]KeysetColumn{})
	if err != nil {
		t.Errorf("expected no error for empty columns, got: %v", err)
	}
}
