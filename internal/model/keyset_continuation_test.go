package model

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
)

func sortOn(attr string, order forma.SortOrder) AttributeOrder {
	return AttributeOrder{AttrID: 1, AttrName: attr, SortOrder: order, ValueType: forma.ValueTypeNumeric}
}

func continuation(cols ...KeysetColumn) *KeysetCursor {
	vals := make([]interface{}, len(cols))
	for i := range vals {
		vals[i] = "v"
	}
	return &KeysetCursor{Columns: cols, Values: vals, Mode: KeysetCursorModeAfter}
}

// TestKeysetCursorValidateContinuation pins the continuation rule (#381
// review): with AttributeOrders present the cursor must be those orders plus
// an ascending row_id tiebreak; with none, the cursor is the order.
func TestKeysetCursorValidateContinuation(t *testing.T) {
	countAsc := []AttributeOrder{sortOn("count", forma.SortOrderAsc)}
	cases := []struct {
		name    string
		cursor  *KeysetCursor
		orders  []AttributeOrder
		wantErr string
	}{
		{
			name:   "an inactive cursor has nothing to continue",
			cursor: &KeysetCursor{},
			orders: countAsc,
		},
		{
			name:   "with no request order the cursor is the order",
			cursor: continuation(KeysetColumn{"created_at", forma.SortOrderAsc}, KeysetColumn{"row_id", forma.SortOrderDesc}),
		},
		{
			name:   "the sort attribute then row_id continues the order",
			cursor: continuation(KeysetColumn{"count", forma.SortOrderAsc}, KeysetColumn{"row_id", forma.SortOrderAsc}),
			orders: countAsc,
		},
		{
			name:   "an empty direction reads as ascending on both sides",
			cursor: continuation(KeysetColumn{"count", ""}, KeysetColumn{"row_id", ""}),
			orders: countAsc,
		},
		{
			name: "two sort keys continue in order",
			cursor: continuation(KeysetColumn{"region", forma.SortOrderDesc}, KeysetColumn{"count", forma.SortOrderAsc},
				KeysetColumn{"row_id", forma.SortOrderAsc}),
			orders: []AttributeOrder{sortOn("region", forma.SortOrderDesc), sortOn("count", forma.SortOrderAsc)},
		},
		{
			name:    "a cursor on another attribute replaces the request order and is refused",
			cursor:  continuation(KeysetColumn{"created_at", forma.SortOrderDesc}, KeysetColumn{"row_id", forma.SortOrderAsc}),
			orders:  countAsc,
			wantErr: `keyset cursor column 1 is "created_at" but the request sorts on "count"`,
		},
		{
			name:    "a flipped direction is refused",
			cursor:  continuation(KeysetColumn{"count", forma.SortOrderDesc}, KeysetColumn{"row_id", forma.SortOrderAsc}),
			orders:  countAsc,
			wantErr: `keyset cursor column 1 ("count") is descending but the request sorts "count" ascending`,
		},
		{
			name:    "a descending row_id tiebreak is refused: page one broke ties on row_id ASC",
			cursor:  continuation(KeysetColumn{"count", forma.SortOrderAsc}, KeysetColumn{"row_id", forma.SortOrderDesc}),
			orders:  countAsc,
			wantErr: `keyset cursor tiebreak "row_id" is descending but the sorted page it continues breaks ties on row_id ASC`,
		},
		{
			name:    "a cursor with an extra column is refused",
			cursor:  continuation(KeysetColumn{"count", forma.SortOrderAsc}, KeysetColumn{"created_at", forma.SortOrderDesc}, KeysetColumn{"row_id", forma.SortOrderAsc}),
			orders:  countAsc,
			wantErr: "keyset cursor carries 3 column(s) but the request sorts on 1 attribute(s)",
		},
		{
			name:    "a cursor on row_id alone does not continue a sorted request",
			cursor:  continuation(KeysetColumn{"row_id", forma.SortOrderAsc}),
			orders:  countAsc,
			wantErr: "keyset cursor carries 1 column(s) but the request sorts on 1 attribute(s)",
		},
		{
			name:    "the attribute name is matched byte-exactly: the sort key spelling is canonical",
			cursor:  continuation(KeysetColumn{"Count", forma.SortOrderAsc}, KeysetColumn{"row_id", forma.SortOrderAsc}),
			orders:  countAsc,
			wantErr: `keyset cursor column 1 is "Count" but the request sorts on "count"`,
		},
		{
			name:    "an order without an attribute name cannot be continued",
			cursor:  continuation(KeysetColumn{"integer_01", forma.SortOrderAsc}, KeysetColumn{"row_id", forma.SortOrderAsc}),
			orders:  []AttributeOrder{{AttrID: 1, ColumnName: "integer_01", SortOrder: forma.SortOrderAsc}},
			wantErr: "keyset cursor cannot continue sort key 1: the resolved order carries no attribute name",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cursor.ValidateContinuation(tc.orders)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateContinuation() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidateContinuation() = nil, want error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("ValidateContinuation() = %q, want it to contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}
