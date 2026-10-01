package internal

import (
	"reflect"
	"testing"

	"github.com/lychee-technology/forma"
)

func TestMergeMapsDeepMerge(t *testing.T) {
	existing := map[string]any{
		"status": "open",
		"contact": map[string]any{
			"name":  "Alice",
			"phone": "123",
			"address": map[string]any{
				"city": "SF",
				"zip":  "94107",
			},
		},
		"tags": []any{"a", "b"},
	}
	updates := map[string]any{
		"status": "closed",
		"contact": map[string]any{
			"phone": "456",
			"address": map[string]any{
				"zip": "94109",
			},
		},
	}

	result := mergeMaps(existing, updates)

	expected := map[string]any{
		"status": "closed",
		"contact": map[string]any{
			"name":  "Alice",
			"phone": "456",
			"address": map[string]any{
				"city": "SF",
				"zip":  "94109",
			},
		},
		"tags": []any{"a", "b"},
	}

	if !reflect.DeepEqual(result, expected) {
		t.Fatalf("unexpected merge result: %#v", result)
	}

	contact := existing["contact"].(map[string]any)
	if contact["phone"] != "123" {
		t.Fatalf("expected existing map to remain unchanged")
	}
}

func TestGetValueAtPathAndReadStringAtPath(t *testing.T) {
	input := map[string]any{
		"a": map[string]any{
			"b": 123,
		},
	}

	if val := getValueAtPath(input, "a.b"); val != 123 {
		t.Fatalf("expected 123, got %v", val)
	}

	if val := getValueAtPath(input, "a.c"); val != nil {
		t.Fatalf("expected nil for missing path, got %v", val)
	}

	str, ok := readStringAtPath(input, "a.b")
	if !ok || str != "123" {
		t.Fatalf("expected stringified value '123', got %q", str)
	}
}

func TestSetNestedValue(t *testing.T) {
	input := map[string]any{}
	setNestedValue(input, "contact.name", "Jane")

	contact, ok := input["contact"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested map to be created")
	}
	if contact["name"] != "Jane" {
		t.Fatalf("expected nested value to be set, got %v", contact["name"])
	}
}

func TestApplyProjection(t *testing.T) {
	record := &forma.DataRecord{
		SchemaName: "visit",
		Attributes: map[string]any{
			"id": "id-1",
			"contact": map[string]any{
				"name":  "Alice",
				"phone": "123",
			},
		},
	}

	applyProjection([]*forma.DataRecord{record}, []string{"contact.name"})

	if _, exists := record.Attributes["id"]; exists {
		t.Fatalf("expected id to be filtered out")
	}

	contact, ok := record.Attributes["contact"].(map[string]any)
	if !ok {
		t.Fatalf("expected contact map to exist")
	}
	if _, exists := contact["phone"]; exists {
		t.Fatalf("expected contact.phone to be filtered out")
	}
	if contact["name"] != "Alice" {
		t.Fatalf("expected contact.name to remain, got %v", contact["name"])
	}
}

// replacedByUpdate must report exactly the attributes whose stored value
// cannot reach the write, so MergeBase may skip converting them (#590
// review): every attribute it reports is one mergeMaps or the literal-key
// dedupe (#312) takes from the update, and every attribute it keeps is one a
// partial update preserves.
func TestReplacedByUpdate(t *testing.T) {
	cases := []struct {
		name    string
		attr    string
		updates any
		want    bool
		literal bool // a dotted key, which the dedupe resolves, not mergeMaps
	}{
		{"scalar names the attribute", "total", map[string]any{"total": 5}, true, false},
		{"unrelated key keeps it", "total", map[string]any{"title": "x"}, false, false},
		{"nested leaf", "contact.email", map[string]any{"contact": map[string]any{"email": "e"}}, true, false},
		{"nested sibling keeps it", "contact.email", map[string]any{"contact": map[string]any{"phone": "p"}}, false, false},
		{"empty object merges nothing", "contact.email", map[string]any{"contact": map[string]any{}}, false, false},
		{"scalar replaces the subtree", "contact.email", map[string]any{"contact": "flat"}, true, false},
		{"null replaces the subtree", "contact.email", map[string]any{"contact": nil}, true, false},
		{"array replaces a list", "totals", map[string]any{"totals": []any{1, 2}}, true, false},
		{"empty array clears a list", "totals", map[string]any{"totals": []any{}}, true, false},
		{"array replaces a list of objects", "items.name", map[string]any{"items": []any{map[string]any{"name": "n"}}}, true, false},
		{"object at the path end", "total", map[string]any{"total": map[string]any{"x": 1}}, true, false},
		{"literal dotted key", "contact.email", map[string]any{"contact.email": "e"}, true, true},
		{"literal key below a nested one", "a.b.c", map[string]any{"a": map[string]any{"b.c": 1}}, true, true},
		{"nested key below a literal one", "a.b.c", map[string]any{"a.b": map[string]any{"c": 1}}, true, true},
		{"literal prefix without the leaf", "a.b.c", map[string]any{"a.b": map[string]any{"d": 1}}, false, true},
		{"name prefix is not a segment", "contact.email", map[string]any{"contact.e": "x"}, false, false},
		{"nil updates", "total", nil, false, false},
		{"non-object updates", "total", []any{map[string]any{"total": 5}}, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := replacedByUpdate(tc.updates)(tc.attr); got != tc.want {
				t.Fatalf("replacedByUpdate(%#v)(%q) = %v, want %v", tc.updates, tc.attr, got, tc.want)
			}
			updates, isObject := tc.updates.(map[string]any)
			if !isObject || tc.literal {
				return
			}
			// For nested spellings, mergeMaps is the oracle: the stored value
			// survives the merge exactly when the attribute is kept.
			existing := map[string]any{}
			setNestedValue(existing, tc.attr, "stored")
			survives := getValueAtPath(mergeMaps(existing, updates), tc.attr) == "stored"
			if survives == tc.want {
				t.Fatalf("mergeMaps keeps the stored %q: %v, but replacedByUpdate reports %v", tc.attr, survives, tc.want)
			}
		})
	}
}
