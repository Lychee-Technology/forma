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
