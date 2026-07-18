package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

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

	result := traverseSchema(schema, "", false, make(map[string]attributeSpec), true)

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

	result := traverseSchema(schema, "", false, make(map[string]attributeSpec), true)

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

	result := traverseSchema(schema, "", false, make(map[string]attributeSpec), true)

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

	result := traverseSchema(schema, "", false, make(map[string]attributeSpec), true)

	// #204: primitive arrays are list containers with the element type carried
	// in items_type, so multiplicity survives the CDC export.
	if attr, ok := result["tags"]; !ok {
		t.Errorf("tags attribute not found")
	} else {
		if attr.ValueType != "list" {
			t.Errorf("expected tags to have type list, got %q", attr.ValueType)
		}
		if attr.ItemsType != "text" {
			t.Errorf("expected tags items_type text, got %q", attr.ItemsType)
		}
	}
}

// TestRunGenerateAttributesListRegeneration: regenerating over an existing
// attributes file must flip a legacy scalar-typed array to list+items_type
// while preserving its attributeID; scalar attrs never gain items_type.
func TestRunGenerateAttributesListRegeneration(t *testing.T) {
	tempDir := t.TempDir()
	schemaPath := filepath.Join(tempDir, "test.json")
	outputPath := filepath.Join(tempDir, "test_attributes.json")

	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"name": map[string]any{"type": "string"},
			"tags": map[string]any{
				"type":  "array",
				"items": map[string]any{"type": "integer"},
			},
		},
	}
	schemaData, _ := json.Marshal(schema)
	if err := os.WriteFile(schemaPath, schemaData, 0o644); err != nil {
		t.Fatalf("failed to write schema file: %v", err)
	}

	// Legacy attributes file: tags flattened to its element type (pre-#204).
	legacy := `{
  "name": { "attributeID": 1, "valueType": "text" },
  "tags": { "attributeID": 2, "valueType": "numeric" }
}`
	if err := os.WriteFile(outputPath, []byte(legacy), 0o644); err != nil {
		t.Fatalf("failed to write legacy attributes file: %v", err)
	}

	args := []string{"-schema-dir", tempDir, "-schema", "test", "-out", outputPath}
	if err := runGenerateAttributes(context.Background(), args); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	var out map[string]map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("parse output: %v", err)
	}

	tags := out["tags"]
	if tags["attributeID"] != float64(2) {
		t.Errorf("tags attributeID = %v, want 2 (preserved)", tags["attributeID"])
	}
	if tags["valueType"] != "list" {
		t.Errorf("tags valueType = %v, want list", tags["valueType"])
	}
	if tags["items_type"] != "numeric" {
		t.Errorf("tags items_type = %v, want numeric", tags["items_type"])
	}
	name := out["name"]
	if name["valueType"] != "text" {
		t.Errorf("name valueType = %v, want text", name["valueType"])
	}
	if _, ok := name["items_type"]; ok {
		t.Errorf("name must not gain items_type, got %v", name["items_type"])
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

	result := traverseSchema(schema, "", false, make(map[string]attributeSpec), true)

	expectedPaths := []string{"contacts.email", "contacts.phone"}
	for _, path := range expectedPaths {
		if _, ok := result[path]; !ok {
			t.Errorf("expected path %q not found", path)
		}
	}
}

func TestTraverseSchemaMarksRequiredProperties(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"required": []any{
			"name",
			"contact",
		},
		"properties": map[string]any{
			"name": map[string]any{
				"type": "string",
			},
			"nickname": map[string]any{
				"type": "string",
			},
			"contact": map[string]any{
				"type": "object",
				"required": []any{
					"email",
				},
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
	}

	result := traverseSchema(schema, "", false, make(map[string]attributeSpec), true)

	if attr := result["name"]; attr.RequiredPolicy != requiredPolicyAlways {
		t.Errorf("expected name policy to be %q, got %q", requiredPolicyAlways, attr.RequiredPolicy)
	}

	if attr := result["nickname"]; attr.RequiredPolicy != requiredPolicyOptional {
		t.Errorf("expected nickname policy to be %q, got %q", requiredPolicyOptional, attr.RequiredPolicy)
	}

	if attr := result["contact.email"]; attr.RequiredPolicy != requiredPolicyAlways {
		t.Errorf("expected contact.email policy to be %q, got %q", requiredPolicyAlways, attr.RequiredPolicy)
	}

	if attr := result["contact.phone"]; attr.RequiredPolicy != requiredPolicyOptional {
		t.Errorf("expected contact.phone policy to be %q, got %q", requiredPolicyOptional, attr.RequiredPolicy)
	}
}

func TestTraverseSchemaSkipsRequiredWhenParentOptional(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"required": []any{
			"name",
		},
		"properties": map[string]any{
			"name": map[string]any{
				"type": "string",
			},
			"contact": map[string]any{
				"type": "object",
				"required": []any{
					"email",
				},
				"properties": map[string]any{
					"email": map[string]any{
						"type": "string",
					},
				},
			},
		},
	}

	result := traverseSchema(schema, "", false, make(map[string]attributeSpec), true)

	if attr := result["contact.email"]; attr.RequiredPolicy != requiredPolicyIfParentPresent {
		t.Errorf("expected contact.email policy to be %q when contact is optional, got %q", requiredPolicyIfParentPresent, attr.RequiredPolicy)
	}
}

// TestTraverseSchemaEmptySchema tests handling of empty schema with object type
func TestTraverseSchemaEmptySchema(t *testing.T) {
	schema := map[string]any{
		"type": "object",
	}

	result := traverseSchema(schema, "", false, make(map[string]attributeSpec), true)

	// Schema with object type but no properties should still add the root path as an attribute
	// since it falls through to the default case
	if len(result) == 0 {
		t.Errorf("expected attribute for root path, got %d attributes", len(result))
	}
}

// TestGenerateAttributesJSONNewAttributesAreSorted tests that new attributes are processed in sorted order
