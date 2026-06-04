package internal

import (
	"strings"
	"testing"

	"github.com/google/uuid"
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

func TestGenerateKeysetWhereClause_SingleColumnAsc(t *testing.T) {
	cursor := &KeysetCursor{
		Columns: []KeysetColumn{
			{Attribute: "created_at", Direction: forma.SortOrderAsc},
		},
		Values: []interface{}{int64(1000)},
		Mode:   KeysetCursorModeAfter,
	}
	clause, args := generateKeysetWhereClause(cursor, "", 0)
	expected := "(created_at > $1)"
	if clause != expected {
		t.Errorf("expected %q, got %q", expected, clause)
	}
	if len(args) != 1 || args[0] != int64(1000) {
		t.Errorf("expected args [1000], got %v", args)
	}
}

func TestGenerateKeysetWhereClause_SingleColumnDesc(t *testing.T) {
	cursor := &KeysetCursor{
		Columns: []KeysetColumn{
			{Attribute: "created_at", Direction: forma.SortOrderDesc},
		},
		Values: []interface{}{int64(1000)},
		Mode:   KeysetCursorModeAfter,
	}
	clause, args := generateKeysetWhereClause(cursor, "", 0)
	expected := "(created_at < $1)"
	if clause != expected {
		t.Errorf("expected %q, got %q", expected, clause)
	}
	if len(args) != 1 || args[0] != int64(1000) {
		t.Errorf("expected args [1000], got %v", args)
	}
}

func TestGenerateKeysetWhereClause_TwoColumnsMixedDirection(t *testing.T) {
	cursor := &KeysetCursor{
		Columns: []KeysetColumn{
			{Attribute: "created_at", Direction: forma.SortOrderDesc},
			{Attribute: "row_id", Direction: forma.SortOrderAsc},
		},
		Values: []interface{}{int64(1000), "abc-123"},
		Mode:   KeysetCursorModeAfter,
	}
	clause, args := generateKeysetWhereClause(cursor, "", 0)
	expected := "(created_at < $1) OR (created_at = $1 AND row_id > $2)"
	if clause != expected {
		t.Errorf("expected %q, got %q", expected, clause)
	}
	if len(args) != 2 || args[0] != int64(1000) || args[1] != "abc-123" {
		t.Errorf("expected args [1000, abc-123], got %v", args)
	}
}

func TestGenerateKeysetWhereClause_ParamOffset(t *testing.T) {
	cursor := &KeysetCursor{
		Columns: []KeysetColumn{
			{Attribute: "created_at", Direction: forma.SortOrderAsc},
		},
		Values: []interface{}{int64(1000)},
		Mode:   KeysetCursorModeAfter,
	}
	clause, args := generateKeysetWhereClause(cursor, "", 4)
	expected := "(created_at > $5)"
	if clause != expected {
		t.Errorf("expected %q, got %q", expected, clause)
	}
	if len(args) != 1 || args[0] != int64(1000) {
		t.Errorf("expected args [1000], got %v", args)
	}
}

func TestBuildKeysetOrderBy(t *testing.T) {
	t.Run("single_asc", func(t *testing.T) {
		cursor := &KeysetCursor{
			Columns: []KeysetColumn{
				{Attribute: "created_at", Direction: forma.SortOrderAsc},
			},
		}
		got := buildKeysetOrderBy(cursor)
		if got != "created_at ASC" {
			t.Errorf("expected 'created_at ASC', got %q", got)
		}
	})
	t.Run("mixed", func(t *testing.T) {
		cursor := &KeysetCursor{
			Columns: []KeysetColumn{
				{Attribute: "created_at", Direction: forma.SortOrderDesc},
				{Attribute: "row_id", Direction: forma.SortOrderAsc},
			},
		}
		got := buildKeysetOrderBy(cursor)
		if got != "created_at DESC, row_id ASC" {
			t.Errorf("expected 'created_at DESC, row_id ASC', got %q", got)
		}
	})
	t.Run("nil cursor defaults", func(t *testing.T) {
		got := buildKeysetOrderBy(nil)
		if got != "created_at DESC" {
			t.Errorf("expected 'created_at DESC', got %q", got)
		}
	})
}

func TestExtractCursorFromRecord(t *testing.T) {
	record := &PersistentRecord{
		RowID:      uuid.Must(uuid.Parse("12345678-1234-1234-1234-123456789012")),
		CreatedAt:  int64(1700000000),
		UpdatedAt:  int64(1700000100),
		SchemaID:   int16(42),
	}
	columns := []KeysetColumn{
		{Attribute: "created_at", Direction: forma.SortOrderDesc},
		{Attribute: "row_id", Direction: forma.SortOrderAsc},
	}
	cursor := extractCursorFromRecord(record, columns)
	if cursor == nil {
		t.Fatal("expected non-nil cursor")
	}
	if cursor.Mode != KeysetCursorModeAfter {
		t.Errorf("expected after mode, got %s", cursor.Mode)
	}
	if len(cursor.Values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(cursor.Values))
	}
	if cursor.Values[0] != int64(1700000000) {
		t.Errorf("expected created_at=1700000000, got %v", cursor.Values[0])
	}
	if cursor.Values[1] != "12345678-1234-1234-1234-123456789012" {
		t.Errorf("expected row_id, got %v", cursor.Values[1])
	}
}

func TestExtractCursorFromRecord_NilRecord(t *testing.T) {
	columns := []KeysetColumn{
		{Attribute: "created_at", Direction: forma.SortOrderDesc},
	}
	cursor := extractCursorFromRecord(nil, columns)
	if cursor != nil {
		t.Errorf("expected nil cursor for nil record")
	}
}

func TestExtractCursorFromRecord_EmptyColumns(t *testing.T) {
	record := &PersistentRecord{}
	cursor := extractCursorFromRecord(record, nil)
	if cursor != nil {
		t.Errorf("expected nil cursor for empty columns")
	}
}

func TestRecordColumnValue_SchemaID(t *testing.T) {
	record := &PersistentRecord{SchemaID: int16(103)}
	col := KeysetColumn{Attribute: "schema_id"}
	val := recordColumnValue(record, col)
	if val != int64(103) {
		t.Errorf("expected schema_id=103, got %v (%T)", val, val)
	}
}
