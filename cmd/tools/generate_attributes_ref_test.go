package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func writeSchemaFixture(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write schema fixture %s: %v", name, err)
	}
	return path
}

func readGeneratedAttributes(t *testing.T, path string) map[string]map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read generated attributes %s: %v", path, err)
	}
	var attrs map[string]map[string]any
	if err := json.Unmarshal(data, &attrs); err != nil {
		t.Fatalf("parse generated attributes %s: %v", path, err)
	}
	return attrs
}

func sortedAttrKeys(attrs map[string]map[string]any) []string {
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// #315: an object-valued $ref in properties must expand into dotted
// attributes exactly as the equivalent inline object would, instead of
// collapsing to a single scalar and shifting every later attributeID.
func TestGenerateAttributesExpandsObjectRef(t *testing.T) {
	dir := t.TempDir()
	writeSchemaFixture(t, dir, "common.json", `{
  "$defs": {
    "address": {
      "type": "object",
      "required": ["street"],
      "properties": {
        "street": {"type": "string"},
        "zip": {"type": "integer"}
      }
    }
  }
}`)
	schemaPath := writeSchemaFixture(t, dir, "main.json", `{
  "type": "object",
  "required": ["address"],
  "properties": {
    "address": {"$ref": "common.json#/$defs/address"},
    "name": {"type": "string"}
  }
}`)
	outputPath := filepath.Join(dir, "main_attributes.json")

	if err := generateAttributesJSON(schemaPath, outputPath); err != nil {
		t.Fatalf("generateAttributesJSON: %v", err)
	}
	attrs := readGeneratedAttributes(t, outputPath)

	if _, exists := attrs["address"]; exists {
		t.Errorf("object $ref collapsed to scalar attribute \"address\"; keys: %v", sortedAttrKeys(attrs))
	}
	street, ok := attrs["address.street"]
	if !ok {
		t.Fatalf("expected expanded attribute \"address.street\"; keys: %v", sortedAttrKeys(attrs))
	}
	if got := street["valueType"]; got != "text" {
		t.Errorf("address.street valueType = %v, want text", got)
	}
	if got := street["required_policy"]; got != "required_always" {
		t.Errorf("address.street required_policy = %v, want required_always", got)
	}
	zip, ok := attrs["address.zip"]
	if !ok {
		t.Fatalf("expected expanded attribute \"address.zip\"; keys: %v", sortedAttrKeys(attrs))
	}
	if got := zip["valueType"]; got != "numeric" {
		t.Errorf("address.zip valueType = %v, want numeric", got)
	}
	if _, hasPolicy := zip["required_policy"]; hasPolicy {
		t.Errorf("address.zip should be optional (no required_policy), got %v", zip["required_policy"])
	}
	if name, ok := attrs["name"]; !ok || name["valueType"] != "text" {
		t.Errorf("inline scalar \"name\" should survive unchanged as text, got %v", name)
	}
}

// Spec §"Generator fix": scalar $ref nodes now see the target's real
// keywords. A $ref to a date-time string must emit valueType "date"
// (previously the accidental fallthrough emitted "text").
func TestGenerateAttributesScalarRefUsesTargetKeywords(t *testing.T) {
	dir := t.TempDir()
	schemaPath := writeSchemaFixture(t, dir, "main.json", `{
  "type": "object",
  "$defs": {
    "ts": {"type": "string", "format": "date-time"}
  },
  "properties": {
    "when": {"$ref": "#/$defs/ts"}
  }
}`)
	outputPath := filepath.Join(dir, "main_attributes.json")

	if err := generateAttributesJSON(schemaPath, outputPath); err != nil {
		t.Fatalf("generateAttributesJSON: %v", err)
	}
	attrs := readGeneratedAttributes(t, outputPath)
	when, ok := attrs["when"]
	if !ok {
		t.Fatalf("expected attribute \"when\"; keys: %v", sortedAttrKeys(attrs))
	}
	if got := when["valueType"]; got != "date" {
		t.Errorf("when valueType = %v, want date (inlined format date-time)", got)
	}
}
