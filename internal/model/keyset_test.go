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

func TestKeysetCursorValidateShape(t *testing.T) {
	cases := []struct {
		name    string
		cursor  *KeysetCursor
		wantErr string // "" means no error
	}{
		{
			name:   "nil cursor carries no obligation",
			cursor: nil,
		},
		{
			name:   "inactive cursor carries no obligation even with stray values",
			cursor: &KeysetCursor{Values: []interface{}{1, 2}},
		},
		{
			name: "aligned cursor ending on row_id is valid",
			cursor: &KeysetCursor{
				Columns: []KeysetColumn{
					{Attribute: "created_at", Direction: forma.SortOrderDesc},
					{Attribute: "row_id", Direction: forma.SortOrderAsc},
				},
				Values: []interface{}{int64(5), "r1"},
			},
		},
		{
			name: "short values are rejected",
			cursor: &KeysetCursor{
				Columns: []KeysetColumn{
					{Attribute: "created_at", Direction: forma.SortOrderDesc},
					{Attribute: "row_id", Direction: forma.SortOrderAsc},
				},
				Values: []interface{}{int64(5)},
			},
			wantErr: "carries 2 column(s) but 1 value(s)",
		},
		{
			name: "nil values are rejected",
			cursor: &KeysetCursor{
				Columns: []KeysetColumn{{Attribute: "row_id", Direction: forma.SortOrderAsc}},
			},
			wantErr: "carries 1 column(s) but 0 value(s)",
		},
		{
			name: "surplus values are rejected",
			cursor: &KeysetCursor{
				Columns: []KeysetColumn{{Attribute: "row_id", Direction: forma.SortOrderAsc}},
				Values:  []interface{}{"r1", "r2"},
			},
			wantErr: "carries 1 column(s) but 2 value(s)",
		},
		{
			name: "cursor not ending on row_id is rejected",
			cursor: &KeysetCursor{
				Columns: []KeysetColumn{
					{Attribute: "row_id", Direction: forma.SortOrderAsc},
					{Attribute: "created_at", Direction: forma.SortOrderDesc},
				},
				Values: []interface{}{"r1", int64(5)},
			},
			wantErr: `final column is "created_at", expected "row_id"`,
		},
	}
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
