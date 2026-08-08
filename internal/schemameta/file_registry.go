package schemameta

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlutil"
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
	attrIDToName          map[int16]map[int16]string
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
	return NewFileSchemaRegistryContext(context.Background(), pool, schemaTable, schemaDir)
}

func NewFileSchemaRegistryContext(ctx context.Context, pool *pgxpool.Pool, schemaTable string, schemaDir string) (forma.SchemaRegistry, error) {
	registry := &fileSchemaRegistry{
		pool:                  pool,
		schemaTable:           schemaTable,
		schemaDir:             schemaDir,
		nameToID:              make(map[string]int16),
		idToName:              make(map[int16]string),
		schemaAttributeCaches: make(map[int16]forma.SchemaAttributeCache),
		attrIDToName:          make(map[int16]map[int16]string),
		schemas:               make(map[int16]forma.JSONSchema),
	}

	if err := registry.loadSchemasFromDB(ctx); err != nil {
		return nil, err
	}

	return registry, nil
}

func (r *fileSchemaRegistry) loadSchemaArtifacts(schemaName string, schemaID int16) (forma.SchemaAttributeCache, *forma.JSONSchema, error) {
	attributesFile := filepath.Join(r.schemaDir, schemaName+"_attributes.json")
	attributeData, err := os.ReadFile(attributesFile)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read attributes file %s: %w", attributesFile, err)
	}

	var rawAttributes map[string]map[string]any
	if err := json.Unmarshal(attributeData, &rawAttributes); err != nil {
		return nil, nil, fmt.Errorf("failed to parse attributes file %s: %w", attributesFile, err)
	}

	cache := make(forma.SchemaAttributeCache)
	for attrName, attrData := range rawAttributes {
		meta, err := parseAttributeMetadata(attrName, attrData, attributesFile)
		if err != nil {
			return nil, nil, err
		}
		cache[attrName] = meta
	}

	schemaFile := filepath.Join(r.schemaDir, schemaName+".json")
	schemaData, err := os.ReadFile(schemaFile)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, nil, fmt.Errorf("failed to read schema file %s: %w", schemaFile, err)
		}
		return cache, nil, nil
	}

	jsonSchema, err := parseJSONSchemaFile(schemaData, schemaID, schemaName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse schema file %s: %w", schemaFile, err)
	}
	return cache, &jsonSchema, nil
}

// registerSchema indexes one schema. Its only error comes from
// buildAttrIDToName, which names the schema and both colliding attributes but
// not the schema id — so both load paths wrap it with that id: it is the
// physical key the attributes are indexed under and, in directory mode, is
// auto-assigned and otherwise invisible to the operator.
func (r *fileSchemaRegistry) registerSchema(schemaName string, schemaID int16, cache forma.SchemaAttributeCache, schema *forma.JSONSchema) error {
	attrIDToName, err := buildAttrIDToName(schemaName, cache)
	if err != nil {
		return err
	}

	r.nameToID[schemaName] = schemaID
	r.idToName[schemaID] = schemaName
	r.schemaAttributeCaches[schemaID] = cache
	r.attrIDToName[schemaID] = attrIDToName
	if schema != nil {
		r.schemas[schemaID] = *schema
	}
	return nil
}

func buildAttrIDToName(schemaName string, cache forma.SchemaAttributeCache) (map[int16]string, error) {
	attrIDToName := make(map[int16]string, len(cache))
	for attrName, meta := range cache {
		if existingAttr, exists := attrIDToName[meta.AttributeID]; exists {
			return nil, fmt.Errorf("schema %s has duplicate attribute id %d for %s and %s", schemaName, meta.AttributeID, existingAttr, attrName)
		}
		attrIDToName[meta.AttributeID] = attrName
	}
	return attrIDToName, nil
}

// loadSchemasFromDB reads schema mappings from the database and loads attribute
// definitions from JSON files on disk.
func (r *fileSchemaRegistry) loadSchemasFromDB(ctx context.Context) error {
	// Step 1: Read schema_name -> schema_id mappings from database
	query := fmt.Sprintf("SELECT schema_name, schema_id FROM %s ORDER BY schema_name", sqlutil.SanitizeIdentifier(r.schemaTable))
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
		if existingName, exists := r.idToName[schemaID]; exists {
			return fmt.Errorf("duplicate schema id %d for %s and %s", schemaID, existingName, schemaName)
		}
		if existingID, exists := r.nameToID[schemaName]; exists {
			return fmt.Errorf("duplicate schema name %s for ids %d and %d", schemaName, existingID, schemaID)
		}

		cache, schema, err := r.loadSchemaArtifacts(schemaName, schemaID)
		if err != nil {
			return fmt.Errorf("failed to load schema %s listed in registry table %s: %w", schemaName, r.schemaTable, err)
		}
		// Validate the FULL cache before stripping — see activeAttributeCache (#342).
		if err := validateSchemaAttributeCache(schemaName, cache); err != nil {
			return fmt.Errorf("attribute metadata in %s: %w", r.schemaDir, err)
		}
		if err := r.registerSchema(schemaName, schemaID, activeAttributeCache(cache), schema); err != nil {
			return fmt.Errorf("register schema id %d: %w", schemaID, err)
		}
	}

	return nil
}

// schemaMetadataForID returns registry-owned metadata maps for internal read-only use.
// Callers must not mutate the returned maps. Returning internal references is
// safe only because the registry is immutable after construction; if schema
// hot-reload is ever introduced, this aliasing contract must be revisited
// (copy-on-read or generational maps).
func (r *fileSchemaRegistry) schemaMetadataForID(id int16) (forma.SchemaAttributeCache, map[int16]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if _, exists := r.idToName[id]; !exists {
		return nil, nil, forma.WithOperatorDetail(forma.NotFoundf("schema not found"), fmt.Errorf("schema id %d", id))
	}

	schema, exists := r.schemaAttributeCaches[id]
	if !exists {
		return nil, nil, forma.WithOperatorDetail(forma.NotFoundf("schema data not found"), fmt.Errorf("schema id %d", id))
	}

	idToName, exists := r.attrIDToName[id]
	if !exists {
		return nil, nil, forma.WithOperatorDetail(forma.NotFoundf("attribute id index not found"), fmt.Errorf("schema id %d", id))
	}

	return schema, idToName, nil
}

// GetSchemaAttributeCacheByName retrieves schema ID and schema definition by schema name
func (r *fileSchemaRegistry) GetSchemaAttributeCacheByName(name string) (int16, forma.SchemaAttributeCache, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	schemaID, exists := r.nameToID[name]
	if !exists {
		return 0, nil, forma.NotFoundf("schema not found: %s", name)
	}

	schema, exists := r.schemaAttributeCaches[schemaID]
	if !exists {
		return 0, nil, forma.NotFoundf("schema data not found: %s", name)
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
		return "", nil, forma.WithOperatorDetail(forma.NotFoundf("schema not found"), fmt.Errorf("schema id %d", id))
	}

	schema, exists := r.schemaAttributeCaches[id]
	if !exists {
		return "", nil, forma.WithOperatorDetail(forma.NotFoundf("schema data not found"), fmt.Errorf("schema id %d", id))
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
		return 0, forma.JSONSchema{}, forma.NotFoundf("schema not found: %s", name)
	}

	schema, exists := r.schemas[schemaID]
	if !exists {
		return 0, forma.JSONSchema{}, forma.NotFoundf("schema data not found: %s", name)
	}

	return schemaID, schema, nil
}

// GetSchemaByID retrieves schema name and JSONSchema by schema ID
func (r *fileSchemaRegistry) GetSchemaByID(id int16) (string, forma.JSONSchema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	name, exists := r.idToName[id]
	if !exists {
		return "", forma.JSONSchema{}, forma.WithOperatorDetail(forma.NotFoundf("schema not found"), fmt.Errorf("schema id %d", id))
	}

	schema, exists := r.schemas[id]
	if !exists {
		return "", forma.JSONSchema{}, forma.WithOperatorDetail(forma.NotFoundf("schema data not found"), fmt.Errorf("schema id %d", id))
	}

	return name, schema, nil
}

func hasSuffix(name, suffix string) bool {
	suffixLen := len(suffix)
	return len(name) > suffixLen && name[len(name)-suffixLen:] == suffix
}

// copyFileSchemaAttributeCache creates a deep copy of a SchemaAttributeCache
func copyFileSchemaAttributeCache(cache forma.SchemaAttributeCache) forma.SchemaAttributeCache {
	result := make(forma.SchemaAttributeCache, len(cache))
	maps.Copy(result, cache)
	return result
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
		attrIDToName:          make(map[int16]map[int16]string),
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
		if !entry.IsDir() && hasSuffix(name, ".json") && !hasSuffix(name, "_full.json") && !hasSuffix(name, "_attributes.json") {
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
		schemaID := nextSchemaID

		cache, schema, err := r.loadSchemaArtifacts(schemaName, schemaID)
		if err != nil {
			return fmt.Errorf("failed to load schema %s discovered in schema directory: %w", schemaName, err)
		}
		// Validate the FULL cache before stripping — see activeAttributeCache (#342).
		if err := validateSchemaAttributeCache(schemaName, cache); err != nil {
			return fmt.Errorf("attribute metadata in %s: %w", r.schemaDir, err)
		}
		if err := r.registerSchema(schemaName, schemaID, activeAttributeCache(cache), schema); err != nil {
			return fmt.Errorf("register schema id %d: %w", schemaID, err)
		}
		nextSchemaID++
	}

	if len(r.nameToID) == 0 {
		return fmt.Errorf("no schema files found in directory: %s", r.schemaDir)
	}

	return nil
}
