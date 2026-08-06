package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerateAttributesJSONUpdateValueType(t *testing.T) {
	tempDir := t.TempDir()
	schemaPath := filepath.Join(tempDir, "schema.json")
	outputPath := filepath.Join(tempDir, "attributes.json")

	// Create initial schema
	schema1 := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"createdAt": map[string]any{
				"type": "string",
			},
		},
	}
	schemaData, _ := json.Marshal(schema1)
	_ = os.WriteFile(schemaPath, schemaData, 0o644)

	_ = generateAttributesJSON(schemaPath, outputPath, true)

	// Update schema to add date format
	schema2 := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"createdAt": map[string]any{
				"type":   "string",
				"format": "date",
			},
		},
	}
	schemaData, _ = json.Marshal(schema2)
	_ = os.WriteFile(schemaPath, schemaData, 0o644)

	_ = generateAttributesJSON(schemaPath, outputPath, true)

	// Verify value type was updated
	data, _ := os.ReadFile(outputPath)
	var result map[string]map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}

	if valueType, ok := result["createdAt"]["valueType"].(string); !ok || valueType != "date" {
		t.Errorf("expected valueType to be updated to 'date', got %v", result["createdAt"]["valueType"])
	}
}

func TestGenerateAttributesJSONMarksRequiredAttributes(t *testing.T) {
	tempDir := t.TempDir()
	schemaPath := filepath.Join(tempDir, "schema.json")
	outputPath := filepath.Join(tempDir, "attributes.json")

	schema := map[string]any{
		"type": "object",
		"required": []any{
			"name",
		},
		"properties": map[string]any{
			"name": map[string]any{"type": "string"},
			"age":  map[string]any{"type": "integer"},
		},
	}
	schemaData, _ := json.Marshal(schema)
	_ = os.WriteFile(schemaPath, schemaData, 0o644)

	if err := generateAttributesJSON(schemaPath, outputPath, true); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	data, _ := os.ReadFile(outputPath)
	var result map[string]map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}

	if policy, ok := result["name"]["required_policy"].(string); !ok || policy != requiredPolicyAlways {
		t.Errorf("expected required_policy %q for name attribute, got %v", requiredPolicyAlways, result["name"]["required_policy"])
	}

	if _, ok := result["age"]["required"]; ok {
		t.Errorf("did not expect legacy required flag for age attribute")
	}
	if _, ok := result["age"]["required_policy"]; ok {
		t.Errorf("did not expect required_policy for age attribute")
	}
}

func TestGenerateAttributesJSONUpdatesRequiredFlag(t *testing.T) {
	tempDir := t.TempDir()
	schemaPath := filepath.Join(tempDir, "schema.json")
	outputPath := filepath.Join(tempDir, "attributes.json")

	initialSchema := map[string]any{
		"type": "object",
		"required": []any{
			"name",
		},
		"properties": map[string]any{
			"name": map[string]any{"type": "string"},
			"age":  map[string]any{"type": "integer"},
		},
	}
	schemaData, _ := json.Marshal(initialSchema)
	_ = os.WriteFile(schemaPath, schemaData, 0o644)

	if err := generateAttributesJSON(schemaPath, outputPath, true); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	data, _ := os.ReadFile(outputPath)
	var firstResult map[string]map[string]any
	_ = json.Unmarshal(data, &firstResult)

	if policy, ok := firstResult["name"]["required_policy"].(string); !ok || policy != requiredPolicyAlways {
		t.Fatalf("expected name required_policy to be %q after first generation", requiredPolicyAlways)
	}

	nameID := int(firstResult["name"]["attributeID"].(float64))
	ageID := int(firstResult["age"]["attributeID"].(float64))

	updatedSchema := map[string]any{
		"type": "object",
		"required": []any{
			"age",
		},
		"properties": map[string]any{
			"name": map[string]any{"type": "string"},
			"age":  map[string]any{"type": "integer"},
		},
	}
	schemaData, _ = json.Marshal(updatedSchema)
	_ = os.WriteFile(schemaPath, schemaData, 0o644)

	if err := generateAttributesJSON(schemaPath, outputPath, true); err != nil {
		t.Fatalf("expected no error on regeneration, got %v", err)
	}

	data, _ = os.ReadFile(outputPath)
	var secondResult map[string]map[string]any
	_ = json.Unmarshal(data, &secondResult)

	if _, ok := secondResult["name"]["required"]; ok {
		t.Errorf("expected name to no longer include legacy required")
	}
	if _, ok := secondResult["name"]["required_policy"]; ok {
		t.Errorf("expected name to no longer include required_policy")
	}

	if policy, ok := secondResult["age"]["required_policy"].(string); !ok || policy != requiredPolicyAlways {
		t.Errorf("expected age required_policy to become %q", requiredPolicyAlways)
	}

	if newNameID := int(secondResult["name"]["attributeID"].(float64)); newNameID != nameID {
		t.Errorf("expected name attribute ID to remain the same, got %d (was %d)", newNameID, nameID)
	}

	if newAgeID := int(secondResult["age"]["attributeID"].(float64)); newAgeID != ageID {
		t.Errorf("expected age attribute ID to remain the same, got %d (was %d)", newAgeID, ageID)
	}
}

// TestGenerateAttributesJSONMaxIDCalculation tests correct maxID calculation
func TestGenerateAttributesJSONMaxIDCalculation(t *testing.T) {
	tempDir := t.TempDir()
	schemaPath := filepath.Join(tempDir, "schema.json")
	outputPath := filepath.Join(tempDir, "attributes.json")

	// Create initial attributes with specific IDs
	existingAttrs := map[string]map[string]any{
		"attr1": {"attributeID": 5, "valueType": "text"},
		"attr2": {"attributeID": 10, "valueType": "text"},
		"attr3": {"attributeID": 7, "valueType": "text"},
	}
	attrData, _ := json.Marshal(existingAttrs)
	_ = os.WriteFile(outputPath, attrData, 0o644)

	// Create schema with new attributes
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"newAttr1": map[string]any{"type": "string"},
			"newAttr2": map[string]any{"type": "string"},
		},
	}
	schemaData, _ := json.Marshal(schema)
	_ = os.WriteFile(schemaPath, schemaData, 0o644)

	_ = generateAttributesJSON(schemaPath, outputPath, true)

	// Verify new IDs start after maxID (10)
	data, _ := os.ReadFile(outputPath)
	var result map[string]map[string]any
	_ = json.Unmarshal(data, &result)

	newID1 := int(result["newAttr1"]["attributeID"].(float64))
	newID2 := int(result["newAttr2"]["attributeID"].(float64))

	if newID1 <= 10 || newID2 <= 10 {
		t.Errorf("new IDs should be > 10 (maxID), got %d and %d", newID1, newID2)
	}

	if newID1 == newID2 {
		t.Errorf("new IDs should be unique, got %d for both", newID1)
	}
}

// TestGenerateAttributesJSONInvalidSchemaFile tests error handling for invalid schema files
func TestGenerateAttributesJSONInvalidSchemaFile(t *testing.T) {
	tempDir := t.TempDir()
	schemaPath := filepath.Join(tempDir, "nonexistent.json")
	outputPath := filepath.Join(tempDir, "attributes.json")

	err := generateAttributesJSON(schemaPath, outputPath, true)
	if err == nil || !strings.Contains(err.Error(), "inline schema") {
		t.Fatalf("expected read error, got %v", err)
	}
}

// TestGenerateAttributesJSONInvalidJSON tests error handling for invalid JSON in schema
func TestGenerateAttributesJSONInvalidJSON(t *testing.T) {
	tempDir := t.TempDir()
	schemaPath := filepath.Join(tempDir, "schema.json")
	outputPath := filepath.Join(tempDir, "attributes.json")

	_ = os.WriteFile(schemaPath, []byte("{invalid json"), 0o644)

	err := generateAttributesJSON(schemaPath, outputPath, true)
	if err == nil || !strings.Contains(err.Error(), "inline schema") {
		t.Fatalf("expected parse error, got %v", err)
	}
}

// TestFormatJSONValue tests JSON value formatting
func TestFormatJSONValue(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected string
	}{
		{
			name:     "integer value",
			value:    42,
			expected: "42",
		},
		{
			name:     "float64 whole number",
			value:    float64(42),
			expected: "42",
		},
		{
			name:     "float64 decimal",
			value:    3.14,
			expected: "3.14",
		},
		{
			name:     "string value",
			value:    "test",
			expected: `"test"`,
		},
		{
			name:     "boolean true",
			value:    true,
			expected: "true",
		},
		{
			name:     "boolean false",
			value:    false,
			expected: "false",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatJSONValue(tt.value)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

// TestIntegrationWorkflow tests complete workflow: generate, modify schema, regenerate
func TestIntegrationWorkflow(t *testing.T) {
	tempDir := t.TempDir()
	schemaPath := filepath.Join(tempDir, "schema.json")
	outputPath := filepath.Join(tempDir, "schema_attributes.json")

	// Step 1: Initial generation
	schema1 := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"id":   map[string]any{"type": "string"},
			"name": map[string]any{"type": "string"},
		},
	}
	schemaData, _ := json.Marshal(schema1)
	_ = os.WriteFile(schemaPath, schemaData, 0o644)

	err := generateAttributesJSON(schemaPath, outputPath, true)
	if err != nil {
		t.Fatalf("first generation failed: %v", err)
	}

	data, _ := os.ReadFile(outputPath)
	var result1 map[string]map[string]any
	_ = json.Unmarshal(data, &result1)

	idFromFirstGen := int(result1["id"]["attributeID"].(float64))
	nameFromFirstGen := int(result1["name"]["attributeID"].(float64))

	// Step 2: Modify schema - add new attribute, remove one
	schema2 := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"id":    map[string]any{"type": "string"},
			"email": map[string]any{"type": "string"},
		},
	}
	schemaData, _ = json.Marshal(schema2)
	_ = os.WriteFile(schemaPath, schemaData, 0o644)

	err = generateAttributesJSON(schemaPath, outputPath, true)
	if err != nil {
		t.Fatalf("second generation failed: %v", err)
	}

	data, _ = os.ReadFile(outputPath)
	var result2 map[string]map[string]any
	_ = json.Unmarshal(data, &result2)

	// Step 3: Verify
	if len(result2) != 3 {
		t.Fatalf("expected 3 attributes (id, name, email), got %d", len(result2))
	}

	// Verify IDs are preserved for existing attributes
	if idFromSecondGen := int(result2["id"]["attributeID"].(float64)); idFromSecondGen != idFromFirstGen {
		t.Errorf("id attribute ID changed from %d to %d", idFromFirstGen, idFromSecondGen)
	}

	if nameFromSecondGen := int(result2["name"]["attributeID"].(float64)); nameFromSecondGen != nameFromFirstGen {
		t.Errorf("name attribute ID changed from %d to %d", nameFromFirstGen, nameFromSecondGen)
	}

	// Verify new attribute exists
	if _, ok := result2["email"]; !ok {
		t.Errorf("new attribute 'email' not found")
	}
}

// TestWriteAttributesMapPropertyOrder tests that attributeID and valueType are written first
func TestWriteAttributesMapPropertyOrder(t *testing.T) {
	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "output.json")

	attrs := map[string]map[string]any{
		"test": {
			"attributeID": 1,
			"valueType":   "text",
			"extra":       "data",
		},
	}

	err := writeAttributesMap(outputPath, attrs)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	data, _ := os.ReadFile(outputPath)
	content := string(data)

	// Check order: attributeID should appear before valueType, and valueType before extra
	idPos := strings.Index(content, `"attributeID"`)
	typePos := strings.Index(content, `"valueType"`)
	extraPos := strings.Index(content, `"extra"`)

	if !(idPos < typePos && typePos < extraPos) {
		t.Errorf("property order is incorrect. attributeID at %d, valueType at %d, extra at %d", idPos, typePos, extraPos)
	}
}

func TestGenerateAttributesJSONNewAttributesAreSorted(t *testing.T) {
	tempDir := t.TempDir()
	schemaPath := filepath.Join(tempDir, "schema.json")
	outputPath := filepath.Join(tempDir, "attributes.json")

	// Create schema with new attributes
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"zebra":  map[string]any{"type": "string"},
			"apple":  map[string]any{"type": "string"},
			"middle": map[string]any{"type": "string"},
		},
	}
	schemaData, _ := json.Marshal(schema)
	_ = os.WriteFile(schemaPath, schemaData, 0o644)

	_ = generateAttributesJSON(schemaPath, outputPath, true)

	data, _ := os.ReadFile(outputPath)
	var result map[string]map[string]any
	_ = json.Unmarshal(data, &result)

	// Get the IDs for each attribute
	appleID := int(result["apple"]["attributeID"].(float64))
	middleID := int(result["middle"]["attributeID"].(float64))
	zebraID := int(result["zebra"]["attributeID"].(float64))

	// IDs should be assigned in sorted order of attribute names
	if !(appleID < middleID && middleID < zebraID) {
		t.Errorf("IDs not assigned in sorted order: apple=%d, middle=%d, zebra=%d", appleID, middleID, zebraID)
	}
}
