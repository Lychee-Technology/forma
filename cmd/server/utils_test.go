package main

import (
	"net/url"
	"reflect"
	"testing"

	"lychee.technology/ltbase/forma"
)

func TestParseSortParams(t *testing.T) {
	tests := []struct {
		name        string
		params      url.Values
		wantFields  []string
		wantOrder   forma.SortOrder
		expectError bool
	}{
		{
			name:       "no sort parameters",
			params:     url.Values{},
			wantFields: nil,
			wantOrder:  "",
		},
		{
			name:       "single sort defaults to asc",
			params:     url.Values{"sort_by": {"age"}},
			wantFields: []string{"age"},
			wantOrder:  forma.SortOrderAsc,
		},
		{
			name: "multi sort with csv and custom order",
			params: url.Values{
				"sort_by":    {"age,name ", " score"},
				"sort_order": {"DESC"},
			},
			wantFields: []string{"age", "name", "score"},
			wantOrder:  forma.SortOrderDesc,
		},
		{
			name: "invalid sort order",
			params: url.Values{
				"sort_by":    {"age"},
				"sort_order": {"up"},
			},
			expectError: true,
		},
		{
			name: "order without fields",
			params: url.Values{
				"sort_order": {"desc"},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields, order, err := parseSortParams(tt.params)

			if tt.expectError {
				if err == nil {
					t.Fatalf("expected error but got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !reflect.DeepEqual(tt.wantFields, fields) {
				t.Fatalf("expected fields %v, got %v", tt.wantFields, fields)
			}

			if tt.wantOrder != order {
				t.Fatalf("expected order %q, got %q", tt.wantOrder, order)
			}
		})
	}
}
