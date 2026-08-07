package main

import (
	"strings"
	"testing"
)

// #342: an attribute dropped from the schema keeps its ledger entry, but the
// generator must brand it retired so the freed attributeID stays bound to the
// EAV rows it already owns instead of being handed to a new attribute.
func TestGenerateAttributes_RemovalMarksRetired(t *testing.T) {
	dir := t.TempDir()
	schema := writeSchemaFixture(t, dir, "user.json",
		`{"type":"object","properties":{"name":{"type":"string"}}}`)
	out := writeSchemaFixture(t, dir, "user_attributes.json", `{
	  "name":    { "attributeID": 1, "valueType": "text" },
	  "old_col": { "attributeID": 3, "valueType": "text", "required_policy": "required_always" }
	}`)

	if err := generateAttributesJSON(schema, out, false); err != nil {
		t.Fatalf("generate: %v", err)
	}
	attrs := readGeneratedAttributes(t, out)
	oldCol, exists := attrs["old_col"]
	if !exists {
		t.Fatalf("removed attribute must stay in the ledger")
	}
	if retired, _ := oldCol["retired"].(bool); !retired {
		t.Fatalf("removed attribute must be marked retired, got %v", oldCol)
	}
	if id, _ := oldCol["attributeID"].(float64); id != 3 {
		t.Fatalf("retired attributeID changed: %v", oldCol["attributeID"])
	}
	if _, ok := oldCol["required_policy"]; ok {
		t.Fatalf("retired attribute must be forced optional, got %v", oldCol["required_policy"])
	}
}

// #342: re-adding a retired name with the identical physical type restores the
// preserved rows, so the retired marker must be cleared and the original
// attributeID kept.
func TestGenerateAttributes_ReAddSameTypeClearsRetired(t *testing.T) {
	dir := t.TempDir()
	schema := writeSchemaFixture(t, dir, "user.json",
		`{"type":"object","properties":{"name":{"type":"string"},"old_col":{"type":"string"}}}`)
	out := writeSchemaFixture(t, dir, "user_attributes.json", `{
	  "name":    { "attributeID": 1, "valueType": "text" },
	  "old_col": { "attributeID": 3, "valueType": "text", "retired": true }
	}`)

	if err := generateAttributesJSON(schema, out, false); err != nil {
		t.Fatalf("generate: %v", err)
	}
	attrs := readGeneratedAttributes(t, out)
	oldCol := attrs["old_col"]
	if _, exists := oldCol["retired"]; exists {
		t.Fatalf("re-added attribute must lose the retired marker: %v", oldCol)
	}
	if id, _ := oldCol["attributeID"].(float64); id != 3 {
		t.Fatalf("re-add must keep attributeID 3, got %v", oldCol["attributeID"])
	}
}

// #342: re-adding a retired name under a different physical type would leave
// the preserved rows unreadable, so the generator must refuse and say so.
func TestGenerateAttributes_ReAddDifferentTypeRejected(t *testing.T) {
	dir := t.TempDir()
	schema := writeSchemaFixture(t, dir, "user.json",
		`{"type":"object","properties":{"name":{"type":"string"},"old_col":{"type":"number"}}}`)
	out := writeSchemaFixture(t, dir, "user_attributes.json", `{
	  "name":    { "attributeID": 1, "valueType": "text" },
	  "old_col": { "attributeID": 3, "valueType": "text", "retired": true }
	}`)

	err := generateAttributesJSON(schema, out, false)
	if err == nil {
		t.Fatalf("expected type-mismatch error on re-add")
	}
	for _, want := range []string{"old_col", "text", "numeric"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not name %q", err.Error(), want)
		}
	}
}

// #342: a retired entry still occupies its attributeID, so a genuinely new
// attribute must be numbered above it rather than reusing the freed id.
func TestGenerateAttributes_NewAttributeDoesNotReuseRetiredID(t *testing.T) {
	dir := t.TempDir()
	schema := writeSchemaFixture(t, dir, "user.json",
		`{"type":"object","properties":{"name":{"type":"string"},"nickname":{"type":"string"}}}`)
	out := writeSchemaFixture(t, dir, "user_attributes.json", `{
	  "name":    { "attributeID": 1, "valueType": "text" },
	  "old_col": { "attributeID": 3, "valueType": "text", "retired": true }
	}`)

	if err := generateAttributesJSON(schema, out, false); err != nil {
		t.Fatalf("generate: %v", err)
	}
	attrs := readGeneratedAttributes(t, out)
	if id, _ := attrs["nickname"]["attributeID"].(float64); id != 4 {
		t.Fatalf("new attribute must get maxID+1=4 (retired id 3 reserved), got %v", id)
	}
}
