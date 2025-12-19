package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRunGenerateAttributesHelpFlag tests the help flag
func TestRunGenerateAttributesHelpFlag(t *testing.T) {
	args := []string{"-h"}
	err := runGenerateAttributes(args)
	if err != nil {
		t.Fatalf("expected no error with -h flag, got %v", err)
	}
}

// TestRunGenerateAttributesMissingSchemaArgument tests when neither -schema nor -schema-file is provided
func TestRunGenerateAttributesMissingSchemaArgument(t *testing.T) {
	tempDir := t.TempDir()
	args := []string{"-out", filepath.Join(tempDir, "output.json")}
	err := runGenerateAttributes(args)
	if err == nil || !strings.Contains(err.Error(), "either -schema or -schema-file must be provided") {
		t.Fatalf("expected error about missing schema argument, got %v", err)
	}
}

// TestRunGenerateAttributesWithSchemaName tests using -schema parameter
func TestRunGenerateAttributesWithSchemaName(t *testing.T) {
	tempDir := t.TempDir()
	schemaPath := filepath.Join(tempDir, "test.json")
	outputPath := filepath.Join(tempDir, "test_attributes.json")

	// Create test schema file
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"name": map[string]any{
				"type": "string",
			},
			"age": map[string]any{
				"type": "integer",
			},
		},
	}
	schemaData, _ := json.Marshal(schema)
	os.WriteFile(schemaPath, schemaData, 0o644)

	args := []string{
		"-schema-dir", tempDir,
		"-schema", "test",
		"-out", outputPath,
	}
	err := runGenerateAttributes(args)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verify output file exists
	if _, err := os.Stat(outputPath); err != nil {
		t.Fatalf("output file not created: %v", err)
	}
}

// TestRunGenerateAttributesWithSchemaFile tests using -schema-file parameter
func TestRunGenerateAttributesWithSchemaFile(t *testing.T) {
	tempDir := t.TempDir()
	schemaPath := filepath.Join(tempDir, "custom_schema.json")
	outputPath := filepath.Join(tempDir, "output.json")

	// Create test schema file
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"email": map[string]any{
				"type": "string",
			},
		},
	}
	schemaData, _ := json.Marshal(schema)
	os.WriteFile(schemaPath, schemaData, 0o644)

	args := []string{
		"-schema-file", schemaPath,
		"-out", outputPath,
	}
	err := runGenerateAttributes(args)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verify output file exists
	if _, err := os.Stat(outputPath); err != nil {
		t.Fatalf("output file not created: %v", err)
	}
}

// TestRunGenerateAttributesDefaultOutputPath tests default output path generation
func TestRunGenerateAttributesDefaultOutputPath(t *testing.T) {
	tempDir := t.TempDir()
	schemaPath := filepath.Join(tempDir, "schema.json")

	// Create test schema file
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"id": map[string]any{
				"type": "string",
			},
		},
	}
	schemaData, _ := json.Marshal(schema)
	os.WriteFile(schemaPath, schemaData, 0o644)

	args := []string{
		"-schema-file", schemaPath,
	}
	err := runGenerateAttributes(args)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verify default output file is created next to schema
	expectedOutputPath := filepath.Join(tempDir, "schema_attributes.json")
	if _, err := os.Stat(expectedOutputPath); err != nil {
		t.Fatalf("default output file not created at expected location: %v", err)
	}
}

// TestGetSchemaTypeString tests type extraction from schema
func TestGetSchemaTypeString(t *testing.T) {
	tests := []struct {
		name     string
		schema   map[string]any
		expected string
	}{
		{
			name:     "string type",
			schema:   map[string]any{"type": "string"},
			expected: "string",
		},
		{
			name:     "integer type",
			schema:   map[string]any{"type": "integer"},
			expected: "integer",
		},
		{
			name:     "array type",
			schema:   map[string]any{"type": "array"},
			expected: "array",
		},
		{
			name:     "object type",
			schema:   map[string]any{"type": "object"},
			expected: "object",
		},
		{
			name:     "type as array returns first",
			schema:   map[string]any{"type": []any{"string", "null"}},
			expected: "string",
		},
		{
			name:     "missing type",
			schema:   map[string]any{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getSchemaType(tt.schema)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

// TestGetValueType tests value type mapping
func TestGetValueType(t *testing.T) {
	tests := []struct {
		name     string
		schema   map[string]any
		expected string
	}{
		{
			name:     "string type",
			schema:   map[string]any{"type": "string"},
			expected: "text",
		},
		{
			name:     "string with date format",
			schema:   map[string]any{"type": "string", "format": "date"},
			expected: "date",
		},
		{
			name:     "string with date-time format",
			schema:   map[string]any{"type": "string", "format": "date-time"},
			expected: "date",
		},
		{
			name:     "integer type",
			schema:   map[string]any{"type": "integer"},
			expected: "numeric",
		},
		{
			name:     "number type",
			schema:   map[string]any{"type": "number"},
			expected: "numeric",
		},
		{
			name:     "boolean type",
			schema:   map[string]any{"type": "boolean"},
			expected: "bool",
		},
		{
			name:     "unknown type defaults to text",
			schema:   map[string]any{"type": "unknown"},
			expected: "text",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getValueType(tt.schema)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

// TestTraverseSchemaSimpleProperties tests simple property extraction
func TestTraverseSchemaSimpleProperties(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"name": map[string]any{
				"type": "string",
			},
			"age": map[string]any{
				"type": "integer",
			},
			"active": map[string]any{
				"type": "boolean",
			},
		},
	}

	result := traverseSchema(schema, "", false, make(map[string]attributeSpec))

	if len(result) != 3 {
		t.Fatalf("expected 3 attributes, got %d", len(result))
	}

	expectedAttrs := map[string]string{
		"name":   "text",
		"age":    "numeric",
		"active": "bool",
	}

	for attrName, expectedType := range expectedAttrs {
		if attr, ok := result[attrName]; !ok {
			t.Errorf("attribute %q not found", attrName)
		} else if attr.ValueType != expectedType {
			t.Errorf("attribute %q: expected type %q, got %q", attrName, expectedType, attr.ValueType)
		}
	}
}

// TestTraverseSchemaNestedObjects tests nested object traversal
func TestTraverseSchemaNestedObjects(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"user": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"name": map[string]any{
						"type": "string",
					},
					"email": map[string]any{
						"type": "string",
					},
				},
			},
		},
	}

	result := traverseSchema(schema, "", false, make(map[string]attributeSpec))

	if len(result) != 2 {
		t.Fatalf("expected 2 attributes, got %d", len(result))
	}

	expectedPaths := []string{"user.name", "user.email"}
	for _, expectedPath := range expectedPaths {
		if _, ok := result[expectedPath]; !ok {
			t.Errorf("expected path %q not found", expectedPath)
		}
	}
}

// TestTraverseSchemaDeepNesting tests deeply nested objects
func TestTraverseSchemaDeepNesting(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"level1": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"level2": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"value": map[string]any{
								"type": "string",
							},
						},
					},
				},
			},
		},
	}

	result := traverseSchema(schema, "", false, make(map[string]attributeSpec))

	if _, ok := result["level1.level2.value"]; !ok {
		t.Errorf("deeply nested path not found")
	}
}

// TestTraverseSchemaArrayOfStrings tests array of primitive types
func TestTraverseSchemaArrayOfStrings(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"tags": map[string]any{
				"type": "array",
				"items": map[string]any{
					"type": "string",
				},
			},
		},
	}

	result := traverseSchema(schema, "", false, make(map[string]attributeSpec))

	if attr, ok := result["tags"]; !ok {
		t.Errorf("tags attribute not found")
	} else if attr.ValueType != "text" {
		t.Errorf("expected tags to have type text, got %q", attr.ValueType)
	}
}

// TestTraverseSchemaArrayOfObjects tests array of objects
func TestTraverseSchemaArrayOfObjects(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"contacts": map[string]any{
				"type": "array",
				"items": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"email": map[string]any{
							"type": "string",
						},
						"phone": map[string]any{
							"type": "string",
						},
					},
				},
			},
		},
	}

	result := traverseSchema(schema, "", false, make(map[string]attributeSpec))

	expectedPaths := []string{"contacts.email", "contacts.phone"}
	for _, path := range expectedPaths {
		if _, ok := result[path]; !ok {
			t.Errorf("expected path %q not found", path)
		}
	}
}

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
	os.WriteFile(attrPath, data, 0o644)

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

	os.WriteFile(attrPath, []byte("{invalid json"), 0o644)

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
	os.WriteFile(schemaPath, schemaData, 0o644)

	err := generateAttributesJSON(schemaPath, outputPath)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Load and verify output
	data, _ := os.ReadFile(outputPath)
	var result map[string]map[string]any
	json.Unmarshal(data, &result)

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
	os.WriteFile(schemaPath, schemaData, 0o644)

	generateAttributesJSON(schemaPath, outputPath)

	// Read first generation result
	data, _ := os.ReadFile(outputPath)
	var firstResult map[string]map[string]any
	json.Unmarshal(data, &firstResult)

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
	os.WriteFile(schemaPath, schemaData, 0o644)

	generateAttributesJSON(schemaPath, outputPath)

	// Read second generation result
	data, _ = os.ReadFile(outputPath)
	var secondResult map[string]map[string]any
	json.Unmarshal(data, &secondResult)

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
	os.WriteFile(schemaPath, schemaData, 0o644)

	generateAttributesJSON(schemaPath, outputPath)

	// Now remove one attribute from schema
	schema2 := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"name": map[string]any{"type": "string"},
			"age":  map[string]any{"type": "integer"},
		},
	}
	schemaData, _ = json.Marshal(schema2)
	os.WriteFile(schemaPath, schemaData, 0o644)

	generateAttributesJSON(schemaPath, outputPath)

	// Verify that email is still in the attributes file
	data, _ := os.ReadFile(outputPath)
	var result map[string]map[string]any
	json.Unmarshal(data, &result)

	if _, ok := result["email"]; !ok {
		t.Errorf("removed attribute 'email' should still exist in attributes file")
	}

	if len(result) != 3 {
		t.Fatalf("expected 3 attributes (including removed), got %d", len(result))
	}
}

// TestGenerateAttributesJSONUpdateValueType tests that value types are updated when schema changes
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
	os.WriteFile(schemaPath, schemaData, 0o644)

	generateAttributesJSON(schemaPath, outputPath)

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
	os.WriteFile(schemaPath, schemaData, 0o644)

	generateAttributesJSON(schemaPath, outputPath)

	// Verify value type was updated
	data, _ := os.ReadFile(outputPath)
	var result map[string]map[string]any
	json.Unmarshal(data, &result)

	if valueType, ok := result["createdAt"]["valueType"].(string); !ok || valueType != "date" {
		t.Errorf("expected valueType to be updated to 'date', got %v", result["createdAt"]["valueType"])
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
	os.WriteFile(outputPath, attrData, 0o644)

	// Create schema with new attributes
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"newAttr1": map[string]any{"type": "string"},
			"newAttr2": map[string]any{"type": "string"},
		},
	}
	schemaData, _ := json.Marshal(schema)
	os.WriteFile(schemaPath, schemaData, 0o644)

	generateAttributesJSON(schemaPath, outputPath)

	// Verify new IDs start after maxID (10)
	data, _ := os.ReadFile(outputPath)
	var result map[string]map[string]any
	json.Unmarshal(data, &result)

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

	err := generateAttributesJSON(schemaPath, outputPath)
	if err == nil || !strings.Contains(err.Error(), "read schema file") {
		t.Fatalf("expected read error, got %v", err)
	}
}

// TestGenerateAttributesJSONInvalidJSON tests error handling for invalid JSON in schema
func TestGenerateAttributesJSONInvalidJSON(t *testing.T) {
	tempDir := t.TempDir()
	schemaPath := filepath.Join(tempDir, "schema.json")
	outputPath := filepath.Join(tempDir, "attributes.json")

	os.WriteFile(schemaPath, []byte("{invalid json"), 0o644)

	err := generateAttributesJSON(schemaPath, outputPath)
	if err == nil || !strings.Contains(err.Error(), "parse schema JSON") {
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
	os.WriteFile(schemaPath, schemaData, 0o644)

	err := generateAttributesJSON(schemaPath, outputPath)
	if err != nil {
		t.Fatalf("first generation failed: %v", err)
	}

	data, _ := os.ReadFile(outputPath)
	var result1 map[string]map[string]any
	json.Unmarshal(data, &result1)

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
	os.WriteFile(schemaPath, schemaData, 0o644)

	err = generateAttributesJSON(schemaPath, outputPath)
	if err != nil {
		t.Fatalf("second generation failed: %v", err)
	}

	data, _ = os.ReadFile(outputPath)
	var result2 map[string]map[string]any
	json.Unmarshal(data, &result2)

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

// TestTraverseSchemaEmptySchema tests handling of empty schema with object type
func TestTraverseSchemaEmptySchema(t *testing.T) {
	schema := map[string]any{
		"type": "object",
	}

	result := traverseSchema(schema, "", false, make(map[string]attributeSpec))

	// Schema with object type but no properties should still add the root path as an attribute
	// since it falls through to the default case
	if len(result) == 0 {
		t.Errorf("expected attribute for root path, got %d attributes", len(result))
	}
}

// TestGenerateAttributesJSONNewAttributesAreSorted tests that new attributes are processed in sorted order
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
	os.WriteFile(schemaPath, schemaData, 0o644)

	generateAttributesJSON(schemaPath, outputPath)

	data, _ := os.ReadFile(outputPath)
	var result map[string]map[string]any
	json.Unmarshal(data, &result)

	// Get the IDs for each attribute
	appleID := int(result["apple"]["attributeID"].(float64))
	middleID := int(result["middle"]["attributeID"].(float64))
	zebraID := int(result["zebra"]["attributeID"].(float64))

	// IDs should be assigned in sorted order of attribute names
	if !(appleID < middleID && middleID < zebraID) {
		t.Errorf("IDs not assigned in sorted order: apple=%d, middle=%d, zebra=%d", appleID, middleID, zebraID)
	}
}
