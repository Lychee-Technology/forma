package main

import (
	"os"
	"strings"
	"testing"
)

// #732: the inference produces one type per family (numeric, text, date,
// bool), so regenerating over a ledger that holds a hand-set sibling must keep
// it. The case that motivated the fix: a bigint bound to double_01 came back as
// numeric (#710). Bindings, ids, items_type and required policies all survive.
func TestGenerateAttributesKeepsDeclaredTypeWithinFamily(t *testing.T) {
	dir := t.TempDir()
	schema := writeSchemaFixture(t, dir, "ledger.json", `{"type":"object","required":["qty"],"properties":{
	  "qty":     {"type":"integer"},
	  "count":   {"type":"number"},
	  "rank":    {"type":"integer"},
	  "ratio":   {"type":"number"},
	  "owner":   {"type":"string"},
	  "seenAt":  {"type":"string","format":"date-time"},
	  "sizes":   {"type":"array","items":{"type":"integer"}},
	  "added":   {"type":"boolean"}
	}}`)
	out := writeSchemaFixture(t, dir, "ledger_attributes.json", `{
	  "qty":    { "attributeID": 1, "valueType": "bigint", "column_binding": {"col_name":"double_01"} },
	  "count":  { "attributeID": 2, "valueType": "integer" },
	  "rank":   { "attributeID": 3, "valueType": "smallint", "column_binding": {"col_name":"smallint_01"} },
	  "ratio":  { "attributeID": 4, "valueType": "numeric" },
	  "owner":  { "attributeID": 5, "valueType": "uuid", "column_binding": {"col_name":"uuid_01"} },
	  "seenAt": { "attributeID": 6, "valueType": "datetime" },
	  "sizes":  { "attributeID": 7, "valueType": "list", "items_type": "bigint" }
	}`)

	if err := generateAttributesJSON(schema, out, false); err != nil {
		t.Fatalf("same-family regeneration must succeed: %v", err)
	}
	attrs := readGeneratedAttributes(t, out)
	for name, want := range map[string]struct {
		id        float64
		valueType string
		items     string
		binding   string
	}{
		"qty":    {1, "bigint", "", "double_01"},
		"count":  {2, "integer", "", ""},
		"rank":   {3, "smallint", "", "smallint_01"},
		"ratio":  {4, "numeric", "", ""},
		"owner":  {5, "uuid", "", "uuid_01"},
		"seenAt": {6, "datetime", "", ""},
		"sizes":  {7, "list", "bigint", ""},
		"added":  {8, "bool", "", ""}, // a new attribute still gets the inferred type
	} {
		got := attrs[name]
		if got["attributeID"] != want.id || got["valueType"] != want.valueType {
			t.Errorf("%s = id %v valueType %v, want id %v valueType %s", name, got["attributeID"], got["valueType"], want.id, want.valueType)
		}
		if items, _ := got["items_type"].(string); items != want.items {
			t.Errorf("%s items_type = %q, want %q", name, items, want.items)
		}
		binding, _ := got["column_binding"].(map[string]any)
		if col, _ := binding["col_name"].(string); col != want.binding {
			t.Errorf("%s column_binding = %v, want %q", name, got["column_binding"], want.binding)
		}
	}
	if attrs["qty"]["required_policy"] != requiredPolicyAlways {
		t.Errorf("qty required_policy = %v, want refreshed from the schema", attrs["qty"]["required_policy"])
	}
}

// #732: a change of family is a type change the tool cannot prove safe, so it
// refuses and writes nothing. Each case names the attribute and both types.
func TestGenerateAttributesRefusesFamilyChange(t *testing.T) {
	for name, tc := range map[string]struct {
		node, entry string
		mentions    []string
	}{
		"number to string":  {`{"type":"string"}`, `{"attributeID":1,"valueType":"bigint"}`, []string{"valueType bigint (numeric family)", "text (text family)"}},
		"boolean to number": {`{"type":"number"}`, `{"attributeID":1,"valueType":"bool"}`, []string{"valueType bool", "numeric (numeric family)"}},
		"text to date":      {`{"type":"string","format":"date"}`, `{"attributeID":1,"valueType":"text"}`, []string{"valueType text", "date (date family)"}},
		"uuid to date-time": {`{"type":"string","format":"date-time"}`, `{"attributeID":1,"valueType":"uuid"}`, []string{"valueType uuid", "date (date family)"}},
		"list to scalar":    {`{"type":"string"}`, `{"attributeID":1,"valueType":"list","items_type":"text"}`, []string{"valueType list", "text (text family)"}},
		"scalar to list":    {`{"type":"array","items":{"type":"string"}}`, `{"attributeID":1,"valueType":"bigint"}`, []string{"valueType bigint", "list (list family)"}},
		// The same family as the element: the ledger cannot tell a live
		// scalar from a pre-#204 flattened array, so it is refused too, with
		// the by-hand remedy for the legacy case.
		"text to array of strings":    {`{"type":"array","items":{"type":"string"}}`, `{"attributeID":1,"valueType":"text"}`, []string{"valueType text", "list (list family)", "#204", "items_type text"}},
		"bigint to array of integers": {`{"type":"array","items":{"type":"integer"}}`, `{"attributeID":1,"valueType":"bigint"}`, []string{"valueType bigint", "list (list family)", "#204", "items_type bigint"}},
		"list element":                {`{"type":"array","items":{"type":"string"}}`, `{"attributeID":1,"valueType":"list","items_type":"integer"}`, []string{"items_type integer (numeric family)", "text (text family)"}},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			schema := writeSchemaFixture(t, dir, "s.json", `{"type":"object","properties":{"attr":`+tc.node+`}}`)
			out := writeSchemaFixture(t, dir, "s_attributes.json", `{"attr":`+tc.entry+`}`)
			before, _ := os.ReadFile(out)

			err := generateAttributesJSON(schema, out, false)
			if err == nil {
				t.Fatal("expected the family change to be refused")
			}
			for _, want := range append([]string{"attribute attr", "s.json", "#710", "new attribute name"}, tc.mentions...) {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not name %q", err.Error(), want)
				}
			}
			if after, _ := os.ReadFile(out); string(after) != string(before) {
				t.Errorf("refused run rewrote the attributes file:\n%s", after)
			}
		})
	}
}

// Every refused attribute is reported in one run, in name order, so an
// operator fixes them together rather than one regeneration at a time.
func TestGenerateAttributesReportsEveryRefusal(t *testing.T) {
	dir := t.TempDir()
	schema := writeSchemaFixture(t, dir, "s.json",
		`{"type":"object","properties":{"b":{"type":"string"},"a":{"type":"boolean"},"c":{"type":"integer"}}}`)
	out := writeSchemaFixture(t, dir, "s_attributes.json", `{
	  "a": {"attributeID":1,"valueType":"numeric"},
	  "b": {"attributeID":2,"valueType":"integer"},
	  "c": {"attributeID":3,"valueType":"bigint"}
	}`)

	err := generateAttributesJSON(schema, out, false)
	if err == nil {
		t.Fatal("expected refusals")
	}
	msg := err.Error()
	if !strings.Contains(msg, "refuse 2 implicit type change(s)") {
		t.Errorf("error %q does not count both refusals", msg)
	}
	if a, b := strings.Index(msg, "attribute a:"), strings.Index(msg, "attribute b:"); a < 0 || b < 0 || a > b {
		t.Errorf("error %q must name a before b", msg)
	}
	if strings.Contains(msg, "attribute c:") {
		t.Errorf("c keeps its family and must not be refused: %q", msg)
	}
}

// The #189 evolutions that change no existing type keep working on a ledger
// that holds hand-set types: adding, removing (retiring) and renaming (remove
// plus add), and an EAV-only attribute promoted to a bound column by hand.
func TestGenerateAttributesKeepsNoTypeChangeEvolutions(t *testing.T) {
	dir := t.TempDir()
	schema := writeSchemaFixture(t, dir, "s.json",
		`{"type":"object","properties":{"qty":{"type":"integer"},"renamed":{"type":"string"},"fresh":{"type":"number"}}}`)
	out := writeSchemaFixture(t, dir, "s_attributes.json", `{
	  "qty":      {"attributeID":1,"valueType":"bigint","column_binding":{"col_name":"bigint_01"}},
	  "old_name": {"attributeID":2,"valueType":"text"}
	}`)

	if err := generateAttributesJSON(schema, out, false); err != nil {
		t.Fatalf("no-type-change evolution must succeed: %v", err)
	}
	attrs := readGeneratedAttributes(t, out)
	if attrs["qty"]["valueType"] != "bigint" || attrs["qty"]["column_binding"] == nil {
		t.Errorf("promoted qty must keep bigint and its binding: %v", attrs["qty"])
	}
	if attrs["old_name"]["retired"] != true || attrs["old_name"]["attributeID"] != float64(2) {
		t.Errorf("dropped old_name must be retired under id 2: %v", attrs["old_name"])
	}
	// New ids follow the existing maximum in name order.
	if attrs["fresh"]["attributeID"] != float64(3) || attrs["fresh"]["valueType"] != "numeric" {
		t.Errorf("fresh must be a new numeric attribute with id 3: %v", attrs["fresh"])
	}
	if attrs["renamed"]["attributeID"] != float64(4) || attrs["renamed"]["valueType"] != "text" {
		t.Errorf("renamed must be a new text attribute with id 4: %v", attrs["renamed"])
	}
}

// A retired bigint the inference can only express as numeric is restored as
// bigint: the re-add keeps the family, so the preserved rows keep their type.
func TestGenerateAttributesReAddRetiredKeepsDeclaredWidth(t *testing.T) {
	dir := t.TempDir()
	schema := writeSchemaFixture(t, dir, "s.json", `{"type":"object","properties":{"qty":{"type":"integer"}}}`)
	out := writeSchemaFixture(t, dir, "s_attributes.json",
		`{"qty":{"attributeID":4,"valueType":"bigint","retired":true}}`)

	if err := generateAttributesJSON(schema, out, false); err != nil {
		t.Fatalf("same-family re-add must succeed: %v", err)
	}
	qty := readGeneratedAttributes(t, out)["qty"]
	if _, retired := qty["retired"]; retired || qty["valueType"] != "bigint" || qty["attributeID"] != float64(4) {
		t.Errorf("re-added qty must be live bigint id 4: %v", qty)
	}
}
