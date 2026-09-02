package model

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
)

func TestKeysetCursorIsActive(t *testing.T) {
	cases := []struct {
		name   string
		cursor *KeysetCursor
		want   bool
	}{
		{"nil cursor is the open first page", nil, false},
		{"nil column slice is the open first page", &KeysetCursor{}, false},
		{"empty column slice is the open first page", &KeysetCursor{Columns: []KeysetColumn{}}, false},
		{"one column is active", &KeysetCursor{
			Columns: []KeysetColumn{{Attribute: "row_id", Direction: forma.SortOrderAsc}},
			Values:  []interface{}{"r1"},
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.cursor.IsActive(); got != tc.want {
				t.Errorf("IsActive() = %v, want %v", got, tc.want)
			}
		})
	}
}

type validateShapeCase struct {
	name    string
	cursor  *KeysetCursor
	wantErr string // "" means accepted
}

func runValidateShapeCases(t *testing.T, cases []validateShapeCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cursor.ValidateShape()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateShape() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidateShape() = nil, want error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("ValidateShape() = %q, want it to contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

// cursorOn builds an aligned, active, after-mode cursor over the named
// columns so a case exercises one rule rather than tripping another first.
func cursorOn(cols []KeysetColumn, values ...interface{}) *KeysetCursor {
	return &KeysetCursor{Columns: cols, Values: values, Mode: KeysetCursorModeAfter}
}

func createdAtThenRowID() []KeysetColumn {
	return []KeysetColumn{
		{Attribute: "created_at", Direction: forma.SortOrderDesc},
		{Attribute: "row_id", Direction: forma.SortOrderAsc},
	}
}

func TestKeysetCursorValidateShapeAlignment(t *testing.T) {
	runValidateShapeCases(t, []validateShapeCase{
		{
			name:   "nil cursor carries no obligation",
			cursor: nil,
		},
		{
			name:   "inactive cursor carries no obligation even with stray values",
			cursor: &KeysetCursor{Values: []interface{}{1, 2}},
		},
		{
			name:   "aligned cursor ending on row_id is valid",
			cursor: cursorOn(createdAtThenRowID(), int64(5), "r1"),
		},
		{
			name:    "short values are rejected",
			cursor:  cursorOn(createdAtThenRowID(), int64(5)),
			wantErr: "carries 2 column(s) but 1 value(s)",
		},
		{
			name: "nil values are rejected",
			cursor: &KeysetCursor{
				Columns: []KeysetColumn{{Attribute: "row_id", Direction: forma.SortOrderAsc}},
				Mode:    KeysetCursorModeAfter,
			},
			wantErr: "carries 1 column(s) but 0 value(s)",
		},
		{
			name: "surplus values are rejected",
			cursor: cursorOn(
				[]KeysetColumn{{Attribute: "row_id", Direction: forma.SortOrderAsc}},
				"r1", "r2"),
			wantErr: "carries 1 column(s) but 2 value(s)",
		},
	})
}

func TestKeysetCursorValidateShapeTiebreak(t *testing.T) {
	runValidateShapeCases(t, []validateShapeCase{
		{
			name: "upper-case ROW_ID is the tiebreak: DuckDB resolves it to row_id",
			cursor: cursorOn([]KeysetColumn{
				{Attribute: "created_at", Direction: forma.SortOrderDesc},
				{Attribute: "ROW_ID", Direction: forma.SortOrderAsc},
			}, int64(5), "r1"),
		},
		{
			name: "a name that merely FOLDS onto row_id is not the tiebreak",
			cursor: cursorOn([]KeysetColumn{
				{Attribute: "created_at", Direction: forma.SortOrderDesc},
				{Attribute: "row.id", Direction: forma.SortOrderAsc},
			}, int64(5), "r1"),
			wantErr: `final column is "row.id", expected "row_id"`,
		},
		{
			name: "cursor not ending on row_id is rejected",
			cursor: cursorOn([]KeysetColumn{
				{Attribute: "row_id", Direction: forma.SortOrderAsc},
				{Attribute: "created_at", Direction: forma.SortOrderDesc},
			}, "r1", int64(5)),
			wantErr: `final column is "created_at", expected "row_id"`,
		},
	})
}

func TestKeysetCursorValidateShapeBoundaryValues(t *testing.T) {
	var nilPtr *string
	runValidateShapeCases(t, []validateShapeCase{
		{
			name:    "a nil tiebreak boundary is rejected",
			cursor:  cursorOn(createdAtThenRowID(), int64(5), nil),
			wantErr: `keyset cursor value 2 (for column "row_id") is nil`,
		},
		{
			name:    "a nil prefix boundary is rejected",
			cursor:  cursorOn(createdAtThenRowID(), nil, "r1"),
			wantErr: `keyset cursor value 1 (for column "created_at") is nil`,
		},
		{
			name:    "a typed nil pointer binds NULL too, so it is rejected",
			cursor:  cursorOn(createdAtThenRowID(), nilPtr, "r1"),
			wantErr: `keyset cursor value 1 (for column "created_at") is nil`,
		},
		{
			name:   "zero-valued boundaries are legitimate and stay accepted",
			cursor: cursorOn(createdAtThenRowID(), int64(0), ""),
		},
		{
			name:   "a false boundary is legitimate and stays accepted",
			cursor: cursorOn(createdAtThenRowID(), false, "r1"),
		},
	})
}

func TestKeysetCursorValidateShapeMode(t *testing.T) {
	modeCursor := func(mode KeysetCursorMode) *KeysetCursor {
		return &KeysetCursor{
			Columns: createdAtThenRowID(),
			Values:  []interface{}{int64(5), "r1"},
			Mode:    mode,
		}
	}
	runValidateShapeCases(t, []validateShapeCase{
		{name: "after is a mode", cursor: modeCursor(KeysetCursorModeAfter)},
		{name: "before is a mode", cursor: modeCursor(KeysetCursorModeBefore)},
		{
			name:    "an unset mode is rejected rather than silently meaning before",
			cursor:  modeCursor(""),
			wantErr: `keyset cursor mode is "", expected "after" or "before"`,
		},
		{
			name:    "an unknown mode is rejected",
			cursor:  modeCursor("sideways"),
			wantErr: `keyset cursor mode is "sideways", expected "after" or "before"`,
		},
		{
			name:    "the mode enum is matched exactly: the renderer compares it byte-for-byte",
			cursor:  modeCursor("After"),
			wantErr: `keyset cursor mode is "After", expected "after" or "before"`,
		},
		{
			name:   "an inactive cursor carries no mode obligation",
			cursor: &KeysetCursor{Mode: "sideways"},
		},
	})
}

func TestKeysetCursorValidateShapeDirection(t *testing.T) {
	directionCursor := func(dir forma.SortOrder) *KeysetCursor {
		return cursorOn([]KeysetColumn{
			{Attribute: "created_at", Direction: dir},
			{Attribute: "row_id", Direction: forma.SortOrderAsc},
		}, int64(5), "r1")
	}
	runValidateShapeCases(t, []validateShapeCase{
		{name: "asc is a direction", cursor: directionCursor(forma.SortOrderAsc)},
		{name: "desc is a direction", cursor: directionCursor(forma.SortOrderDesc)},
		{name: "an empty direction is the documented asc default", cursor: directionCursor("")},
		{
			name:    "an unknown direction is rejected rather than silently meaning asc",
			cursor:  directionCursor("sideways"),
			wantErr: `keyset cursor column "created_at" has direction "sideways", expected "asc", "desc" or empty`,
		},
		{
			name:    "the direction enum is matched exactly: the renderer compares it byte-for-byte",
			cursor:  directionCursor("DESC"),
			wantErr: `keyset cursor column "created_at" has direction "DESC", expected "asc", "desc" or empty`,
		},
		{
			name: "the tiebreak column's direction is checked too",
			cursor: cursorOn([]KeysetColumn{
				{Attribute: "created_at", Direction: forma.SortOrderDesc},
				{Attribute: "row_id", Direction: "ascending"},
			}, int64(5), "r1"),
			wantErr: `keyset cursor column "row_id" has direction "ascending"`,
		},
	})
}
