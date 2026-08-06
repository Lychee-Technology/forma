package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLoadExistingAttributesFileNotExists tests loading non-existent file
func TestLoadExistingAttributesFileNotExists(t *testing.T) {
	tempDir := t.TempDir()
	nonExistentPath := filepath.Join(tempDir, "nonexistent.json")

	result, err := loadExistingAttributes(nonExistentPath)
	if err != nil {
		t.Fatalf("expected no error for non-existent file, got %v", err)
	}

	if len(result) != 0 {
		t.Fatalf("expected empty map for non-existent file, got %d attributes", len(result))
	}
}

// TestLoadExistingAttributesValidFile tests loading valid attributes file
func TestLoadExistingAttributesValidFile(t *testing.T) {
	tempDir := t.TempDir()
	attrPath := filepath.Join(tempDir, "attributes.json")

	attrs := map[string]map[string]any{
		"name": {
			"attributeID": float64(1),
			"valueType":   "text",
		},
		"age": {
			"attributeID": float64(2),
			"valueType":   "numeric",
		},
	}

	data, _ := json.Marshal(attrs)
	if err := os.WriteFile(attrPath, data, 0o644); err != nil {
		t.Fatalf("failed to write attributes file: %v", err)
	}

	result, err := loadExistingAttributes(attrPath)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(result) != 2 {
		t.Fatalf("expected 2 attributes, got %d", len(result))
	}

	if id, ok := result["name"]["attributeID"].(float64); !ok || id != 1 {
		t.Errorf("unexpected attributeID for name")
	}
}

// TestLoadExistingAttributesInvalidJSON tests loading invalid JSON file
func TestLoadExistingAttributesInvalidJSON(t *testing.T) {
	tempDir := t.TempDir()
	attrPath := filepath.Join(tempDir, "invalid.json")

	if err := os.WriteFile(attrPath, []byte("{invalid json"), 0o644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	_, err := loadExistingAttributes(attrPath)
	if err == nil || !strings.Contains(err.Error(), "parse existing attributes JSON") {
		t.Fatalf("expected parse error, got %v", err)
	}
}

// TestWriteAttributesMapBasic tests writing attributes to file
func TestWriteAttributesMapBasic(t *testing.T) {
	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "output.json")

	attrs := map[string]map[string]any{
		"name": {
			"attributeID": 1,
			"valueType":   "text",
		},
		"age": {
			"attributeID": 2,
			"valueType":   "numeric",
		},
	}

	err := writeAttributesMap(outputPath, attrs)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verify file exists and content is valid JSON
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}

	var result map[string]map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}

	if len(result) != 2 {
		t.Fatalf("expected 2 attributes in output, got %d", len(result))
	}
}

// TestWriteAttributesMapKeySorting tests that keys are sorted in output
func TestWriteAttributesMapKeySorting(t *testing.T) {
	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "output.json")

	attrs := map[string]map[string]any{
		"zebra": {
			"attributeID": 1,
			"valueType":   "text",
		},
		"apple": {
			"attributeID": 2,
			"valueType":   "text",
		},
		"middle": {
			"attributeID": 3,
			"valueType":   "text",
		},
	}

	err := writeAttributesMap(outputPath, attrs)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	data, _ := os.ReadFile(outputPath)
	content := string(data)

	// Check that keys appear in alphabetical order in the JSON
	applePos := strings.Index(content, `"apple"`)
	middlePos := strings.Index(content, `"middle"`)
	zebraPos := strings.Index(content, `"zebra"`)

	if applePos == -1 || middlePos == -1 || zebraPos == -1 {
		t.Fatalf("not all keys found in output")
	}

	if !(applePos < middlePos && middlePos < zebraPos) {
		t.Errorf("keys are not in sorted order in output")
	}
}

// TestWriteAttributesMapCreatesDirectory tests directory creation
func TestWriteAttributesMapCreatesDirectory(t *testing.T) {
	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "subdir", "nested", "output.json")

	attrs := map[string]map[string]any{
		"test": {
			"attributeID": 1,
			"valueType":   "text",
		},
	}

	err := writeAttributesMap(outputPath, attrs)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if _, err := os.Stat(outputPath); err != nil {
		t.Fatalf("file not created in nested directory: %v", err)
	}
}

// TestGenerateAttributesJSONNewFile tests generation for new schema
func TestGenerateAttributesJSONNewFile(t *testing.T) {
	tempDir := t.TempDir()
	schemaPath := filepath.Join(tempDir, "schema.json")
	outputPath := filepath.Join(tempDir, "attributes.json")

	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"name": map[string]any{"type": "string"},
			"age":  map[string]any{"type": "integer"},
		},
	}
	schemaData, _ := json.Marshal(schema)
	if err := os.WriteFile(schemaPath, schemaData, 0o644); err != nil {
		t.Fatalf("failed to write schema file: %v", err)
	}

	err := generateAttributesJSON(schemaPath, outputPath, true)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Load and verify output
	data, _ := os.ReadFile(outputPath)
	var result map[string]map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}

	if len(result) != 2 {
		t.Fatalf("expected 2 attributes, got %d", len(result))
	}

	// Check IDs are assigned sequentially
	nameID := int(result["name"]["attributeID"].(float64))
	ageID := int(result["age"]["attributeID"].(float64))

	if nameID <= 0 || ageID <= 0 {
		t.Errorf("expected positive IDs, got name=%d, age=%d", nameID, ageID)
	}

	if nameID == ageID {
		t.Errorf("expected different IDs, got %d for both", nameID)
	}
}

// TestGenerateAttributesJSONPreserveExistingIDs tests that existing attribute IDs are preserved
func TestGenerateAttributesJSONPreserveExistingIDs(t *testing.T) {
	tempDir := t.TempDir()
	schemaPath := filepath.Join(tempDir, "schema.json")
	outputPath := filepath.Join(tempDir, "attributes.json")

	// Create initial schema and attributes
	schema1 := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"name": map[string]any{"type": "string"},
			"age":  map[string]any{"type": "integer"},
		},
	}
	schemaData, _ := json.Marshal(schema1)
	if err := os.WriteFile(schemaPath, schemaData, 0o644); err != nil {
		t.Fatalf("failed to write schema file: %v", err)
	}

	_ = generateAttributesJSON(schemaPath, outputPath, true)

	// Read first generation result
	data, _ := os.ReadFile(outputPath)
	var firstResult map[string]map[string]any
	if err := json.Unmarshal(data, &firstResult); err != nil {
		t.Fatalf("failed to unmarshal first result: %v", err)
	}

	originalNameID := int(firstResult["name"]["attributeID"].(float64))
	originalAgeID := int(firstResult["age"]["attributeID"].(float64))

	// Now add a new attribute to the schema
	schema2 := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"name":  map[string]any{"type": "string"},
			"age":   map[string]any{"type": "integer"},
			"email": map[string]any{"type": "string"},
		},
	}
	schemaData, _ = json.Marshal(schema2)
	if err := os.WriteFile(schemaPath, schemaData, 0o644); err != nil {
		t.Fatalf("failed to write schema file: %v", err)
	}

	_ = generateAttributesJSON(schemaPath, outputPath, true)

	// Read second generation result
	data, _ = os.ReadFile(outputPath)
	var secondResult map[string]map[string]any
	if err := json.Unmarshal(data, &secondResult); err != nil {
		t.Fatalf("failed to unmarshal second result: %v", err)
	}

	// Check that original IDs are preserved
	if newNameID := int(secondResult["name"]["attributeID"].(float64)); newNameID != originalNameID {
		t.Errorf("name ID changed: was %d, now %d", originalNameID, newNameID)
	}

	if newAgeID := int(secondResult["age"]["attributeID"].(float64)); newAgeID != originalAgeID {
		t.Errorf("age ID changed: was %d, now %d", originalAgeID, newAgeID)
	}

	// Check that new attribute gets new ID
	emailID := int(secondResult["email"]["attributeID"].(float64))
	if emailID <= originalNameID || emailID <= originalAgeID {
		t.Errorf("email ID should be larger than existing IDs, got %d", emailID)
	}
}

// TestGenerateAttributesJSONRemoveAttributeFromSchema tests that removed attributes are still preserved
func TestGenerateAttributesJSONRemoveAttributeFromSchema(t *testing.T) {
	tempDir := t.TempDir()
	schemaPath := filepath.Join(tempDir, "schema.json")
	outputPath := filepath.Join(tempDir, "attributes.json")

	// Create initial schema with 3 attributes
	schema1 := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"name":  map[string]any{"type": "string"},
			"age":   map[string]any{"type": "integer"},
			"email": map[string]any{"type": "string"},
		},
	}
	schemaData, _ := json.Marshal(schema1)
	_ = os.WriteFile(schemaPath, schemaData, 0o644)

	_ = generateAttributesJSON(schemaPath, outputPath, true)

	// Now remove one attribute from schema
	schema2 := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"name": map[string]any{"type": "string"},
			"age":  map[string]any{"type": "integer"},
		},
	}
	schemaData, _ = json.Marshal(schema2)
	_ = os.WriteFile(schemaPath, schemaData, 0o644)

	_ = generateAttributesJSON(schemaPath, outputPath, true)

	// Verify that email is still in the attributes file
	data, _ := os.ReadFile(outputPath)
	var result map[string]map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}

	if _, ok := result["email"]; !ok {
		t.Errorf("removed attribute 'email' should still exist in attributes file")
	}

	if len(result) != 3 {
		t.Fatalf("expected 3 attributes (including removed), got %d", len(result))
	}
}

func TestGenerateAttributesJSONRemovedAttributeClearsRequiredFlag(t *testing.T) {
	tempDir := t.TempDir()
	schemaPath := filepath.Join(tempDir, "schema.json")
	outputPath := filepath.Join(tempDir, "attributes.json")

	initialSchema := map[string]any{
		"type": "object",
		"required": []any{
			"mustKeep",
			"toBeRemoved",
		},
		"properties": map[string]any{
			"mustKeep":    map[string]any{"type": "string"},
			"toBeRemoved": map[string]any{"type": "string"},
		},
	}
	schemaData, _ := json.Marshal(initialSchema)
	_ = os.WriteFile(schemaPath, schemaData, 0o644)

	if err := generateAttributesJSON(schemaPath, outputPath, true); err != nil {
		t.Fatalf("expected no error on initial generation, got %v", err)
	}

	updatedSchema := map[string]any{
		"type": "object",
		"required": []any{
			"mustKeep",
		},
		"properties": map[string]any{
			"mustKeep": map[string]any{"type": "string"},
		},
	}
	schemaData, _ = json.Marshal(updatedSchema)
	_ = os.WriteFile(schemaPath, schemaData, 0o644)

	if err := generateAttributesJSON(schemaPath, outputPath, true); err != nil {
		t.Fatalf("expected no error on regeneration, got %v", err)
	}

	data, _ := os.ReadFile(outputPath)
	var result map[string]map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}

	if _, exists := result["toBeRemoved"]; !exists {
		t.Fatalf("expected removed attribute to still be preserved")
	}

	if _, ok := result["toBeRemoved"]["required"]; ok {
		t.Errorf("expected removed attribute to clear legacy required flag")
	}
	if _, ok := result["toBeRemoved"]["required_policy"]; ok {
		t.Errorf("expected removed attribute to clear required_policy")
	}
}

// TestGenerateAttributesJSONUpdateValueType tests that value types are updated when schema changes
