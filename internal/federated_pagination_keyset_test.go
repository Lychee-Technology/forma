package internal

import (
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

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

func TestGenerateKeysetWhereClause_ThreeColumns(t *testing.T) {
	cursor := &KeysetCursor{
		Columns: []KeysetColumn{
			{Attribute: "symbol", Direction: forma.SortOrderAsc},
			{Attribute: "created_at", Direction: forma.SortOrderDesc},
			{Attribute: "row_id", Direction: forma.SortOrderAsc},
		},
		Values: []interface{}{"SYM00001", int64(1000), "abc-123"},
		Mode:   KeysetCursorModeAfter,
	}
	clause, args := generateKeysetWhereClause(cursor, "", 0)
	expected := "(symbol > $1) OR (symbol = $1 AND created_at < $2) OR (symbol = $1 AND created_at = $2 AND row_id > $3)"
	if clause != expected {
		t.Errorf("expected %q, got %q", expected, clause)
	}
	if len(args) != 3 {
		t.Errorf("expected 3 args, got %d: %v", len(args), args)
	}
}

func TestGenerateKeysetWhereClause_BeforeMode(t *testing.T) {
	cursor := &KeysetCursor{
		Columns: []KeysetColumn{
			{Attribute: "created_at", Direction: forma.SortOrderAsc},
		},
		Values: []interface{}{int64(1000)},
		Mode:   KeysetCursorModeBefore,
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

func TestGenerateKeysetWhereClause_BeforeModeDesc(t *testing.T) {
	cursor := &KeysetCursor{
		Columns: []KeysetColumn{
			{Attribute: "created_at", Direction: forma.SortOrderDesc},
		},
		Values: []interface{}{int64(1000)},
		Mode:   KeysetCursorModeBefore,
	}
	clause, _ := generateKeysetWhereClause(cursor, "", 0)
	expected := "(created_at > $1)"
	if clause != expected {
		t.Errorf("expected %q, got %q", expected, clause)
	}
}

func TestGenerateKeysetWhereClause_WithTableAlias(t *testing.T) {
	cursor := &KeysetCursor{
		Columns: []KeysetColumn{
			{Attribute: "created_at", Direction: forma.SortOrderAsc},
			{Attribute: "row_id", Direction: forma.SortOrderAsc},
		},
		Values: []interface{}{int64(1000), "abc-123"},
		Mode:   KeysetCursorModeAfter,
	}
	clause, _ := generateKeysetWhereClause(cursor, "unified.", 0)
	expected := "(unified.created_at > $1) OR (unified.created_at = $1 AND unified.row_id > $2)"
	if clause != expected {
		t.Errorf("expected %q, got %q", expected, clause)
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
		RowID:     uuid.Must(uuid.Parse("12345678-1234-1234-1234-123456789012")),
		CreatedAt: int64(1700000000),
		UpdatedAt: int64(1700000100),
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
