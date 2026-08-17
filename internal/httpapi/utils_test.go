package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
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

// TestReadJSONBodyDecodesNumbersExactly guards #282: every HTTP body decode
// must deliver json.Number for numeric literals landing in any-typed sinks,
// so no payload rides float64 at the door.
func TestReadJSONBodyDecodesNumbersExactly(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"n": 9007199254740993}`))

	var decoded map[string]any
	if err := readJSONBody(req, &decoded); err != nil {
		t.Fatalf("readJSONBody: %v", err)
	}

	got := decoded["n"]
	if want := json.Number("9007199254740993"); got != want {
		t.Fatalf("decoded[n] = %#v (%T), want %#v", got, got, want)
	}
}

// TestParseHelpersPublishInvalidInput pins #360: every error a parse helper
// returns must carry forma.ErrInvalidInput and publish its whole message,
// because handlers route it through the disclosure gate — an unpublished
// error would keep its 400 but answer a redacted body.
func TestParseHelpersPublishInvalidInput(t *testing.T) {
	_, _, pathEmptyErr := parsePath("/api/v1/")
	_, _, pathFormatErr := parsePath("/api/v1/a/b/c")
	_, _, orderErr := parseSortParams(url.Values{"sort_by": {"age"}, "sort_order": {"up"}})
	_, _, danglingErr := parseSortParams(url.Values{"sort_order": {"desc"}})
	_, _, noFieldsErr := parseSortParams(url.Values{"sort_by": {" , "}})
	_, _, emptyArrErr := parseCreateObjects([]any{})
	_, _, nonObjErr := parseCreateObjects([]any{42})
	_, _, scalarErr := parseCreateObjects("bad")

	cases := map[string]struct {
		err  error
		want string
	}{
		"path empty schema":      {pathEmptyErr, "empty schema name"},
		"path format":            {pathFormatErr, "invalid path format"},
		"sort order":             {orderErr, "invalid sort_order: up"},
		"sort order without by":  {danglingErr, "sort_order requires sort_by to be specified"},
		"sort by without fields": {noFieldsErr, "sort_by provided but contained no valid fields"},
		"create empty array":     {emptyArrErr, "empty array not allowed"},
		"create non-object":      {nonObjErr, "body[0] must be an object"},
		"create scalar":          {scalarErr, "body must be an object or array"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if !errors.Is(tc.err, forma.ErrInvalidInput) {
				t.Fatalf("error does not carry ErrInvalidInput: %v", tc.err)
			}
			msg, ok := forma.ResolvePublicMessage(tc.err)
			if !ok {
				t.Fatalf("error publishes nothing: %v", tc.err)
			}
			if msg != tc.want {
				t.Fatalf("published %q, want %q", msg, tc.want)
			}
		})
	}
}
