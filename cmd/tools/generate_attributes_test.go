package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRunGenerateAttributesHelpFlag tests the help flag
func TestRunGenerateAttributesHelpFlag(t *testing.T) {
	args := []string{"-h"}
	err := runGenerateAttributes(context.Background(), args)
	if err != nil {
		t.Fatalf("expected no error with -h flag, got %v", err)
	}
}

// TestRunGenerateAttributesMissingSchemaArgument tests when neither -schema nor -schema-file is provided
func TestRunGenerateAttributesMissingSchemaArgument(t *testing.T) {
	tempDir := t.TempDir()
	args := []string{"-out", filepath.Join(tempDir, "output.json")}
	err := runGenerateAttributes(context.Background(), args)
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
	if err := os.WriteFile(schemaPath, schemaData, 0o644); err != nil {
		t.Fatalf("failed to write schema file: %v", err)
	}

	args := []string{
		"-schema-dir", tempDir,
		"-schema", "test",
		"-out", outputPath,
	}
	err := runGenerateAttributes(context.Background(), args)
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
	if err := os.WriteFile(schemaPath, schemaData, 0o644); err != nil {
		t.Fatalf("failed to write schema file: %v", err)
	}

	args := []string{
		"-schema-file", schemaPath,
		"-out", outputPath,
	}
	err := runGenerateAttributes(context.Background(), args)
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
	if err := os.WriteFile(schemaPath, schemaData, 0o644); err != nil {
		t.Fatalf("failed to write schema file: %v", err)
	}

	args := []string{
		"-schema-file", schemaPath,
	}
	err := runGenerateAttributes(context.Background(), args)
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
