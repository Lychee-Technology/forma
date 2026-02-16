package main

import (
	"net/url"
	"reflect"
	"testing"

	"github.com/lychee-technology/forma"
)

func TestParseCreateObjects(t *testing.T) {
	tests := []struct {
		name       string
		body       any
		wantCount  int
		wantSingle bool
		wantErr    bool
	}{
		{
			name:       "single object",
			body:       map[string]any{"id": "a"},
			wantCount:  1,
			wantSingle: true,
		},
		{
			name:       "array of objects",
			body:       []any{map[string]any{"id": "a"}, map[string]any{"id": "b"}},
			wantCount:  2,
			wantSingle: false,
		},
		{
			name:    "array with non-object element",
			body:    []any{map[string]any{"id": "a"}, 42},
			wantErr: true,
		},
		{
			name:    "empty array",
			body:    []any{},
			wantErr: true,
		},
		{
			name:    "invalid scalar body",
			body:    "bad",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, single, err := parseCreateObjects(tt.body)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != tt.wantCount {
				t.Fatalf("expected %d objects, got %d", tt.wantCount, len(got))
			}
			if single != tt.wantSingle {
				t.Fatalf("expected single=%v, got %v", tt.wantSingle, single)
			}
		})
	}
}

func TestParseCSVParam(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{name: "empty", input: "", want: nil},
		{name: "spaces only", input: "  ", want: nil},
		{name: "single", input: "lead", want: []string{"lead"}},
		{name: "multiple trimmed", input: "lead, visit , user", want: []string{"lead", "visit", "user"}},
		{name: "deduplicated", input: "lead,visit,lead", want: []string{"lead", "visit"}},
		{name: "skip empty parts", input: "lead,, ,visit", want: []string{"lead", "visit"}},
		{name: "quoted comma value", input: `lead,"visit,user",lead`, want: []string{"lead", "visit,user"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseCSVParam(tt.input)
			if !reflect.DeepEqual(tt.want, got) {
				t.Fatalf("expected %v, got %v", tt.want, got)
			}
		})
	}
}

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
