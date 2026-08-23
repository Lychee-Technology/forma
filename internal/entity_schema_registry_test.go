package internal

import (
	"os"
	"path/filepath"
	"testing"
)

// TestSchemaRegistry_LoadSchemas tests schema loading
func TestSchemaRegistry_LoadSchemas(t *testing.T) {
	schemaDir := "../cmd/server/schemas"
	registry, err := newFileSchemaRegistryFromDir(schemaDir)
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}

	// Test schema retrieval by name
	schemaID, cache, err := registry.GetSchemaAttributeCacheByName("visit")
	if err != nil {
		t.Errorf("GetSchemaByName failed: %v", err)
	}

	if schemaID == 0 {
		t.Error("Expected non-zero schema ID")
	}

	if cache == nil {
		t.Error("Expected schema attribute cache, got nil")
	}

	// Check that we have some attributes in the cache
	if len(cache) == 0 {
		t.Error("Expected non-empty attribute cache")
	}

	// Test that we can access an attribute
	if _, ok := cache["id"]; !ok {
		t.Error("Expected 'id' attribute in cache")
	}

	// Test listing schemas
	schemas := registry.ListSchemas()
	if len(schemas) == 0 {
		t.Error("Expected at least one schema")
	}

	if len(schemas) < 2 {
		t.Logf("Warning: Expected at least 2 schemas, got %d", len(schemas))
	}
}

// TestSchemaRegistry_GetSchemaByID tests retrieval by ID
func TestSchemaRegistry_GetSchemaByID(t *testing.T) {
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}

	// First get a schema by name to obtain its ID
	schemaID, _, err := registry.GetSchemaAttributeCacheByName("visit")
	if err != nil {
		t.Fatalf("failed to get schema by name: %v", err)
	}

	// Now retrieve by ID
	name, schema, err := registry.GetSchemaAttributeCacheByID(schemaID)
	if err != nil {
		t.Errorf("GetSchemaByID failed: %v", err)
	}

	if name != "visit" {
		t.Errorf("Expected name 'visit', got '%s'", name)
	}

	if schema == nil {
		t.Error("Expected schema data, got nil")
	}
}

// TestFileSchemaRegistry_InvalidDirectory tests error handling for invalid directory
func TestFileSchemaRegistry_InvalidDirectory(t *testing.T) {
	_, err := newFileSchemaRegistryFromDir("/nonexistent/directory")
	if err == nil {
		t.Error("Expected error for invalid directory, got nil")
	}
}

// TestFileSchemaRegistry_NoSchemaFiles tests error handling when no schema files exist
func TestFileSchemaRegistry_NoSchemaFiles(t *testing.T) {
	// Create a temporary empty directory
	tmpDir, err := os.MkdirTemp("", "test-schemas-")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	_, err = newFileSchemaRegistryFromDir(tmpDir)
	if err == nil {
		t.Error("Expected error when no schema files found, got nil")
	}
}

// TestFileSchemaRegistry_InvalidJSON tests error handling for invalid JSON files
func TestFileSchemaRegistry_InvalidJSON(t *testing.T) {
	// Create a temporary directory with an invalid JSON file
	tmpDir, err := os.MkdirTemp("", "test-schemas-")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Write invalid JSON
	invalidFile := filepath.Join(tmpDir, "invalid.json")
	err = os.WriteFile(invalidFile, []byte("{invalid json"), 0644)
	if err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err = newFileSchemaRegistryFromDir(tmpDir)
	if err == nil {
		t.Error("Expected error for invalid JSON, got nil")
	}
}
