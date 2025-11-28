package main

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/lychee-technology/forma"
)

// fileSchemaRegistry is a file-based implementation of SchemaRegistry
type fileSchemaRegistry struct {
	mu           sync.RWMutex
	schemaDir    string
	nameToID     map[string]int16
	idToName     map[int16]string
	schemas      map[string]forma.SchemaAttributeCache
	nextSchemaID int16
}

// NewFileSchemaRegistry creates a new file-based schema registry.
// It loads JSON schema files from the specified directory.
func NewFileSchemaRegistry(schemaDir string) (forma.SchemaRegistry, error) {
	registry := &fileSchemaRegistry{
		schemaDir:    schemaDir,
		nameToID:     make(map[string]int16),
		idToName:     make(map[int16]string),
		schemas:      make(map[string]forma.SchemaAttributeCache),
		nextSchemaID: 100,
	}

	if err := registry.loadSchemas(); err != nil {
		return nil, err
	}

	return registry, nil
}

// loadSchemas loads all JSON schema files from the schema directory
func (r *fileSchemaRegistry) loadSchemas() error {
	entries, err := os.ReadDir(r.schemaDir)
	if err != nil {
		return fmt.Errorf("failed to read schema directory: %w", err)
	}

	// Collect schema files (excluding *_attributes.json files)
	var schemaFiles []fs.DirEntry
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") && !strings.HasSuffix(entry.Name(), "_attributes.json") {
			schemaFiles = append(schemaFiles, entry)
		}
	}

	// Sort by filename for deterministic schema ID assignment
	sort.Slice(schemaFiles, func(i, j int) bool {
		return schemaFiles[i].Name() < schemaFiles[j].Name()
	})

	// Load and register each schema
	for _, entry := range schemaFiles {
		schemaName := strings.TrimSuffix(entry.Name(), ".json")

		// Load attribute metadata from corresponding _attributes.json file
		attributesFile := filepath.Join(r.schemaDir, schemaName+"_attributes.json")
		attributeData, err := os.ReadFile(attributesFile)
		if err != nil {
			return fmt.Errorf("failed to read attributes file %s: %w", attributesFile, err)
		}

		// Parse attribute metadata JSON into a temporary structure
		var rawAttributes map[string]map[string]any
		if err := json.Unmarshal(attributeData, &rawAttributes); err != nil {
			return fmt.Errorf("failed to parse attributes file %s: %w", attributesFile, err)
		}

		// Convert to SchemaAttributeCache
		cache := make(forma.SchemaAttributeCache)
		for attrName, attrData := range rawAttributes {
			meta, err := parseAttributeMetadata(attrName, attrData, attributesFile)
			if err != nil {
				return err
			}
			cache[attrName] = meta
		}

		schemaID := r.nextSchemaID
		r.nameToID[schemaName] = schemaID
		r.idToName[schemaID] = schemaName
		r.schemas[schemaName] = cache
		r.nextSchemaID++
	}

	if len(r.nameToID) == 0 {
		return fmt.Errorf("no schema files found in directory: %s", r.schemaDir)
	}

	return nil
}

// GetSchemaByName retrieves schema ID and schema definition by schema name
func (r *fileSchemaRegistry) GetSchemaByName(name string) (int16, forma.SchemaAttributeCache, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	schemaID, exists := r.nameToID[name]
	if !exists {
		return 0, nil, fmt.Errorf("schema not found: %s", name)
	}

	schema, exists := r.schemas[name]
	if !exists {
		return 0, nil, fmt.Errorf("schema data not found: %s", name)
	}

	// Return a copy to prevent external mutations
	schemaCopy := copySchemaAttributeCache(schema)
	return schemaID, schemaCopy, nil
}

// GetSchemaByID retrieves schema name and definition by schema ID
func (r *fileSchemaRegistry) GetSchemaByID(id int16) (string, forma.SchemaAttributeCache, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	name, exists := r.idToName[id]
	if !exists {
		return "", nil, fmt.Errorf("schema not found for ID: %d", id)
	}

	schema, exists := r.schemas[name]
	if !exists {
		return "", nil, fmt.Errorf("schema data not found for ID: %d", id)
	}

	// Return a copy to prevent external mutations
	schemaCopy := copySchemaAttributeCache(schema)
	return name, schemaCopy, nil
}

// ListSchemas returns a list of all registered schema names
func (r *fileSchemaRegistry) ListSchemas() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	schemas := make([]string, 0, len(r.nameToID))
	for name := range r.nameToID {
		schemas = append(schemas, name)
	}

	sort.Strings(schemas)
	return schemas
}

// copySchemaAttributeCache creates a deep copy of a SchemaAttributeCache
func copySchemaAttributeCache(cache forma.SchemaAttributeCache) forma.SchemaAttributeCache {
	result := make(forma.SchemaAttributeCache, len(cache))
	for key, value := range cache {
		result[key] = value
	}
	return result
}

// parseAttributeMetadata converts raw JSON metadata into AttributeMetadata structs
func parseAttributeMetadata(attrName string, attrData map[string]any, source string) (forma.AttributeMetadata, error) {
	meta := forma.AttributeMetadata{AttributeName: attrName}

	// Parse attributeID
	attrIDRaw, ok := attrData["attributeID"].(float64)
	if !ok {
		return forma.AttributeMetadata{}, fmt.Errorf("invalid or missing attributeID for attribute %s in %s", attrName, source)
	}
	meta.AttributeID = int16(attrIDRaw)

	// Parse valueType
	valueTypeStr, ok := attrData["valueType"].(string)
	if !ok {
		return forma.AttributeMetadata{}, fmt.Errorf("invalid or missing valueType for attribute %s in %s", attrName, source)
	}
	meta.ValueType = forma.ValueType(valueTypeStr)

	// Parse optional column_binding
	if bindingRaw, exists := attrData["column_binding"]; exists {
		binding, err := parseColumnBinding(bindingRaw, attrName, source)
		if err != nil {
			return forma.AttributeMetadata{}, err
		}
		meta.ColumnBinding = binding
	}

	return meta, nil
}

// parseColumnBinding parses column binding configuration
func parseColumnBinding(raw any, attrName, source string) (*forma.MainColumnBinding, error) {
	bindingMap, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid column_binding format for attribute %s in %s", attrName, source)
	}

	colNameStr, ok := bindingMap["col_name"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid or missing col_name in column_binding for attribute %s in %s", attrName, source)
	}

	binding := &forma.MainColumnBinding{
		ColumnName: forma.MainColumn(colNameStr),
	}

	if encodingStr, ok := bindingMap["encoding"].(string); ok {
		binding.Encoding = forma.MainColumnEncoding(encodingStr)
	}

	return binding, nil
}
