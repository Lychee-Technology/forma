package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
)

// fileSchemaRegistry is a SchemaRegistry implementation that loads schema
// attribute definitions from JSON files on disk. It supports two modes:
// 1. Database-backed: reads schema name to ID mappings from a PostgreSQL table
// 2. Directory-based: scans directory for schema files and auto-assigns IDs
type fileSchemaRegistry struct {
	mu                    sync.RWMutex
	pool                  *pgxpool.Pool // nil when using directory-based mode
	schemaTable           string        // empty when using directory-based mode
	schemaDir             string
	nameToID              map[string]int16
	idToName              map[int16]string
	schemaAttributeCaches map[int16]forma.SchemaAttributeCache
	schemas               map[int16]forma.JSONSchema
}

// NewFileSchemaRegistry creates a new schema registry that reads schema mappings
// from a PostgreSQL table and loads attribute definitions from JSON files.
//
// Parameters:
//   - pool: PostgreSQL connection pool
//   - schemaTable: Name of the schema_registry table (e.g., "schema_registry_1234567890")
//   - schemaDir: Directory containing the *_attributes.json files
func NewFileSchemaRegistry(pool *pgxpool.Pool, schemaTable string, schemaDir string) (forma.SchemaRegistry, error) {
	registry := &fileSchemaRegistry{
		pool:                  pool,
		schemaTable:           schemaTable,
		schemaDir:             schemaDir,
		nameToID:              make(map[string]int16),
		idToName:              make(map[int16]string),
		schemaAttributeCaches: make(map[int16]forma.SchemaAttributeCache),
		schemas:               make(map[int16]forma.JSONSchema),
	}

	if err := registry.loadSchemasFromDB(); err != nil {
		return nil, err
	}

	return registry, nil
}

// loadSchemasFromDB reads schema mappings from the database and loads attribute
// definitions from JSON files on disk.
func (r *fileSchemaRegistry) loadSchemasFromDB() error {
	ctx := context.Background()

	// Step 1: Read schema_name -> schema_id mappings from database
	query := fmt.Sprintf("SELECT schema_name, schema_id FROM %s ORDER BY schema_name", sanitizeIdentifier(r.schemaTable))
	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query schema registry table: %w", err)
	}
	defer rows.Close()

	// Collect all schema mappings
	type schemaMapping struct {
		name string
		id   int16
	}
	var mappings []schemaMapping

	for rows.Next() {
		var name string
		var id int16
		if err := rows.Scan(&name, &id); err != nil {
			return fmt.Errorf("failed to scan schema row: %w", err)
		}
		mappings = append(mappings, schemaMapping{name: name, id: id})
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating schema rows: %w", err)
	}

	if len(mappings) == 0 {
		return fmt.Errorf("no schemas found in table: %s", r.schemaTable)
	}

	// Step 2: For each schema, load attribute metadata from the corresponding _attributes.json file
	for _, mapping := range mappings {
		schemaName := mapping.name
		schemaID := mapping.id

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
			meta, err := parseFileAttributeMetadata(attrName, attrData, attributesFile)
			if err != nil {
				return err
			}
			cache[attrName] = meta
		}

		// Load main schema JSON file (e.g., lead.json)
		schemaFile := filepath.Join(r.schemaDir, schemaName+".json")
		schemaData, err := os.ReadFile(schemaFile)
		if err != nil {
			// Schema file is optional, skip if not found
			if !os.IsNotExist(err) {
				return fmt.Errorf("failed to read schema file %s: %w", schemaFile, err)
			}
		} else {
			jsonSchema, err := parseJSONSchemaFile(schemaData, schemaID, schemaName)
			if err != nil {
				return fmt.Errorf("failed to parse schema file %s: %w", schemaFile, err)
			}
			r.schemas[schemaID] = jsonSchema
		}

		r.nameToID[schemaName] = schemaID
		r.idToName[schemaID] = schemaName
		r.schemaAttributeCaches[schemaID] = cache
	}

	return nil
}

// parseJSONSchemaFile parses a JSON Schema file and converts it to forma.JSONSchema structure
func parseJSONSchemaFile(data []byte, schemaID int16, schemaName string) (forma.JSONSchema, error) {
	var rawSchema map[string]any
	if err := json.Unmarshal(data, &rawSchema); err != nil {
		return forma.JSONSchema{}, fmt.Errorf("failed to unmarshal JSON schema: %w", err)
	}

	jsonSchema := forma.JSONSchema{
		ID:         schemaID,
		Name:       schemaName,
		Schema:     string(data),
		Properties: make(map[string]*forma.PropertySchema),
	}

	// Parse required fields
	if required, ok := rawSchema["required"].([]any); ok {
		for _, r := range required {
			if s, ok := r.(string); ok {
				jsonSchema.Required = append(jsonSchema.Required, s)
			}
		}
	}

	// Parse properties
	if properties, ok := rawSchema["properties"].(map[string]any); ok {
		defs := make(map[string]any)
		if d, ok := rawSchema["$defs"].(map[string]any); ok {
			defs = d
		}
		for propName, propValue := range properties {
			if propMap, ok := propValue.(map[string]any); ok {
				propSchema := parsePropertySchema(propName, propMap, defs, jsonSchema.Required)
				jsonSchema.Properties[propName] = propSchema
			}
		}
	}

	return jsonSchema, nil
}

// parsePropertySchema parses a single property from JSON Schema
func parsePropertySchema(name string, prop map[string]any, defs map[string]any, requiredFields []string) *forma.PropertySchema {
	// Handle $ref
	if ref, ok := prop["$ref"].(string); ok {
		// Resolve $ref (e.g., "#/$defs/id")
		resolved := resolveRef(ref, defs)
		if resolved != nil {
			prop = resolved
		}
	}

	schema := &forma.PropertySchema{
		Name: name,
	}

	// Check if this field is required
	for _, r := range requiredFields {
		if r == name {
			schema.Required = true
			break
		}
	}

	// Parse type
	if t, ok := prop["type"].(string); ok {
		schema.Type = t
	}

	// Parse format
	if f, ok := prop["format"].(string); ok {
		schema.Format = f
	}

	// Parse pattern
	if p, ok := prop["pattern"].(string); ok {
		schema.Pattern = p
	}

	// Parse enum
	if e, ok := prop["enum"].([]any); ok {
		schema.Enum = e
	}

	// Parse default
	if d, exists := prop["default"]; exists {
		schema.Default = d
	}

	// Parse minimum/maximum
	if min, ok := prop["minimum"].(float64); ok {
		schema.Minimum = &min
	}
	if max, ok := prop["maximum"].(float64); ok {
		schema.Maximum = &max
	}

	// Parse minLength/maxLength
	if minLen, ok := prop["minLength"].(float64); ok {
		v := int(minLen)
		schema.MinLength = &v
	}
	if maxLen, ok := prop["maxLength"].(float64); ok {
		v := int(maxLen)
		schema.MaxLength = &v
	}

	// Parse x-relation
	if relation, ok := prop["x-relation"].(map[string]any); ok {
		schema.Relation = &forma.RelationSchema{}
		if target, ok := relation["target"].(string); ok {
			schema.Relation.Target = target
		}
		if relType, ok := relation["type"].(string); ok {
			schema.Relation.Type = relType
		}
	}

	// Parse x-storage
	if storage, ok := prop["x-storage"].(string); ok {
		schema.Storage = storage
	}

	// Parse items (for arrays)
	if items, ok := prop["items"].(map[string]any); ok {
		schema.Items = parsePropertySchema("", items, defs, nil)
	}

	// Parse nested properties (for objects)
	if nestedProps, ok := prop["properties"].(map[string]any); ok {
		schema.Properties = make(map[string]*forma.PropertySchema)
		nestedRequired := []string{}
		if nr, ok := prop["required"].([]any); ok {
			for _, r := range nr {
				if s, ok := r.(string); ok {
					nestedRequired = append(nestedRequired, s)
				}
			}
		}
		for nestedName, nestedValue := range nestedProps {
			if nestedMap, ok := nestedValue.(map[string]any); ok {
				schema.Properties[nestedName] = parsePropertySchema(nestedName, nestedMap, defs, nestedRequired)
			}
		}
	}

	return schema
}

// resolveRef resolves a JSON Schema $ref reference
func resolveRef(ref string, defs map[string]any) map[string]any {
	// Handle "#/$defs/xxx" format
	if len(ref) > 8 && ref[:8] == "#/$defs/" {
		defName := ref[8:]
		if def, ok := defs[defName].(map[string]any); ok {
			return def
		}
	}
	return nil
}

// GetSchemaAttributeCacheByName retrieves schema ID and schema definition by schema name
func (r *fileSchemaRegistry) GetSchemaAttributeCacheByName(name string) (int16, forma.SchemaAttributeCache, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	schemaID, exists := r.nameToID[name]
	if !exists {
		return 0, nil, fmt.Errorf("schema not found: %s", name)
	}

	schema, exists := r.schemaAttributeCaches[schemaID]
	if !exists {
		return 0, nil, fmt.Errorf("schema data not found: %s", name)
	}

	// Return a copy to prevent external mutations
	schemaCopy := copyFileSchemaAttributeCache(schema)
	return schemaID, schemaCopy, nil
}

// GetSchemaAttributeCacheByID retrieves schema name and definition by schema ID
func (r *fileSchemaRegistry) GetSchemaAttributeCacheByID(id int16) (string, forma.SchemaAttributeCache, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	name, exists := r.idToName[id]
	if !exists {
		return "", nil, fmt.Errorf("schema not found for ID: %d", id)
	}

	schema, exists := r.schemaAttributeCaches[id]
	if !exists {
		return "", nil, fmt.Errorf("schema data not found for ID: %d", id)
	}

	// Return a copy to prevent external mutations
	schemaCopy := copyFileSchemaAttributeCache(schema)
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

// GetSchemaByName retrieves schema ID and JSONSchema by schema name
func (r *fileSchemaRegistry) GetSchemaByName(name string) (int16, forma.JSONSchema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	schemaID, exists := r.nameToID[name]
	if !exists {
		return 0, forma.JSONSchema{}, fmt.Errorf("schema not found: %s", name)
	}

	schema, exists := r.schemas[schemaID]
	if !exists {
		return 0, forma.JSONSchema{}, fmt.Errorf("schema data not found: %s", name)
	}

	return schemaID, schema, nil
}

// GetSchemaByID retrieves schema name and JSONSchema by schema ID
func (r *fileSchemaRegistry) GetSchemaByID(id int16) (string, forma.JSONSchema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	name, exists := r.idToName[id]
	if !exists {
		return "", forma.JSONSchema{}, fmt.Errorf("schema not found for ID: %d", id)
	}

	schema, exists := r.schemas[id]
	if !exists {
		return "", forma.JSONSchema{}, fmt.Errorf("schema data not found for ID: %d", id)
	}

	return name, schema, nil
}

// isAttributesFile checks if a filename is an attributes file (ends with _attributes.json)
func isAttributesFile(name string) bool {
	return len(name) > 16 && name[len(name)-16:] == "_attributes.json"
}

// copyFileSchemaAttributeCache creates a deep copy of a SchemaAttributeCache
func copyFileSchemaAttributeCache(cache forma.SchemaAttributeCache) forma.SchemaAttributeCache {
	result := make(forma.SchemaAttributeCache, len(cache))
	for key, value := range cache {
		result[key] = value
	}
	return result
}

// parseFileAttributeMetadata converts raw JSON metadata into AttributeMetadata structs
func parseFileAttributeMetadata(attrName string, attrData map[string]any, source string) (forma.AttributeMetadata, error) {
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
		binding, err := parseFileColumnBinding(bindingRaw, attrName, source)
		if err != nil {
			return forma.AttributeMetadata{}, err
		}
		meta.ColumnBinding = binding
	}

	return meta, nil
}

// NewFileSchemaRegistryFromDirectory creates a schema registry that scans
// a directory for schema files and auto-assigns IDs (starting from 100).
// This mode does not require a database connection.
//
// Parameters:
//   - schemaDir: Directory containing the schema files (*.json and *_attributes.json)
func NewFileSchemaRegistryFromDirectory(schemaDir string) (forma.SchemaRegistry, error) {
	registry := &fileSchemaRegistry{
		pool:                  nil, // no database connection
		schemaTable:           "",  // empty when using directory-based mode
		schemaDir:             schemaDir,
		nameToID:              make(map[string]int16),
		idToName:              make(map[int16]string),
		schemaAttributeCaches: make(map[int16]forma.SchemaAttributeCache),
		schemas:               make(map[int16]forma.JSONSchema),
	}

	if err := registry.loadSchemasFromDirectory(); err != nil {
		return nil, err
	}

	return registry, nil
}

// loadSchemasFromDirectory scans the schema directory for schema files and
// auto-assigns IDs starting from 100.
func (r *fileSchemaRegistry) loadSchemasFromDirectory() error {
	entries, err := os.ReadDir(r.schemaDir)
	if err != nil {
		return fmt.Errorf("failed to read schema directory: %w", err)
	}

	// Collect schema files (excluding *_attributes.json files)
	var schemaNames []string
	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() && len(name) > 5 && name[len(name)-5:] == ".json" && !isAttributesFile(name) {
			schemaName := name[:len(name)-5] // remove .json extension
			schemaNames = append(schemaNames, schemaName)
		}
	}

	// Sort by name for deterministic schema ID assignment
	sort.Strings(schemaNames)

	// Assign IDs starting from 100
	nextSchemaID := int16(100)

	// Load and register each schema
	for _, schemaName := range schemaNames {
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
			meta, err := parseFileAttributeMetadata(attrName, attrData, attributesFile)
			if err != nil {
				return err
			}
			cache[attrName] = meta
		}

		schemaID := nextSchemaID

		// Load main schema JSON file (e.g., lead.json)
		schemaFile := filepath.Join(r.schemaDir, schemaName+".json")
		schemaData, err := os.ReadFile(schemaFile)
		if err != nil {
			// Schema file is optional, skip if not found
			if !os.IsNotExist(err) {
				return fmt.Errorf("failed to read schema file %s: %w", schemaFile, err)
			}
		} else {
			jsonSchema, err := parseJSONSchemaFile(schemaData, schemaID, schemaName)
			if err != nil {
				return fmt.Errorf("failed to parse schema file %s: %w", schemaFile, err)
			}
			r.schemas[schemaID] = jsonSchema
		}

		r.nameToID[schemaName] = schemaID
		r.idToName[schemaID] = schemaName
		r.schemaAttributeCaches[schemaID] = cache
		nextSchemaID++
	}

	if len(r.nameToID) == 0 {
		return fmt.Errorf("no schema files found in directory: %s", r.schemaDir)
	}

	return nil
}

// parseFileColumnBinding parses column binding configuration
func parseFileColumnBinding(raw any, attrName, source string) (*forma.MainColumnBinding, error) {
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
