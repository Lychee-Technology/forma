package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

// MetadataCache holds all metadata mappings for fast lookups
type MetadataCache struct {
	mu sync.RWMutex

	// Schema mappings
	schemaNameToID map[string]int16
	schemaIDToName map[int16]string

	// Attribute mappings: (schema_name, attr_name) -> AttributeMeta
	attributeMetadata map[string]map[string]forma.AttributeMetadata

	// Schema caches for transformer
	schemaCaches map[string]forma.SchemaAttributeCache
}

// NewMetadataCache creates a new metadata cache
func NewMetadataCache() *MetadataCache {
	return &MetadataCache{
		schemaNameToID:    make(map[string]int16),
		schemaIDToName:    make(map[int16]string),
		attributeMetadata: make(map[string]map[string]forma.AttributeMetadata),
		schemaCaches:      make(map[string]forma.SchemaAttributeCache),
	}
}

// GetSchemaID retrieves schema ID by name (thread-safe)
func (mc *MetadataCache) GetSchemaID(schemaName string) (int16, bool) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	id, ok := mc.schemaNameToID[schemaName]
	if !ok {
		zap.S().Warnw("schema name not found in cache", "schema_name", schemaName, "cache_size", len(mc.schemaNameToID))
	}
	return id, ok
}

// GetSchemaName retrieves schema name by ID (thread-safe)
func (mc *MetadataCache) GetSchemaName(schemaID int16) (string, bool) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	name, ok := mc.schemaIDToName[schemaID]
	if !ok {
		zap.S().Warnw("schema ID not found in cache", "schema_id", schemaID, "cache_size", len(mc.schemaIDToName))
	}
	return name, ok
}

// GetSchemaCache retrieves the schema attribute cache for a schema (thread-safe)
func (mc *MetadataCache) GetSchemaCache(schemaName string) (forma.SchemaAttributeCache, bool) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	cache, ok := mc.schemaCaches[schemaName]
	if !ok {
		zap.S().Warnw("schema not found in cache", "schema", schemaName, "cache_size", len(mc.schemaCaches))
	}
	return cache, ok
}

func (mc *MetadataCache) GetSchemaCacheByID(schemaID int16) (forma.SchemaAttributeCache, bool) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	schemaName, ok := mc.GetSchemaName(schemaID)
	if !ok {
		return nil, false
	}
	return mc.GetSchemaCache(schemaName) 
}

// ListSchemas returns all schema names (thread-safe)
func (mc *MetadataCache) ListSchemas() []string {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	schemas := make([]string, 0, len(mc.schemaNameToID))
	for name := range mc.schemaNameToID {
		schemas = append(schemas, name)
	}
	return schemas
}

// MetadataLoader loads schema and attribute metadata from database and JSON files
type MetadataLoader struct {
	pool            *pgxpool.Pool
	schemaTableName string
	schemaDirectory string
}

// NewMetadataLoader creates a new metadata loader
func NewMetadataLoader(pool *pgxpool.Pool, schemaTableName, schemaDirectory string) *MetadataLoader {
	return &MetadataLoader{
		pool:            pool,
		schemaTableName: schemaTableName,
		schemaDirectory: schemaDirectory,
	}
}

// LoadMetadata loads all metadata and returns a cache
func (ml *MetadataLoader) LoadMetadata(ctx context.Context) (*MetadataCache, error) {
	cache := NewMetadataCache()

	// Step 1: Load schema mappings from database
	if err := ml.loadSchemaRegistry(ctx, cache); err != nil {
		return nil, fmt.Errorf("failed to load schema registry: %w", err)
	}

	// Step 2: Load attribute metadata from JSON files
	if err := ml.loadAttributeMetadataFromFiles(cache); err != nil {
		return nil, fmt.Errorf("failed to load attribute metadata: %w", err)
	}

	return cache, nil
}

// loadSchemaRegistry loads schema name to ID mappings from database
func (ml *MetadataLoader) loadSchemaRegistry(ctx context.Context, cache *MetadataCache) error {
	query := fmt.Sprintf("SELECT schema_name, schema_id FROM %s", ml.schemaTableName)

	rows, err := ml.pool.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query schema registry: %w", err)
	}
	defer rows.Close()

	cache.mu.Lock()
	defer cache.mu.Unlock()

	for rows.Next() {
		var schemaName string
		var schemaID int16

		if err := rows.Scan(&schemaName, &schemaID); err != nil {
			return fmt.Errorf("failed to scan schema row: %w", err)
		}

		cache.schemaNameToID[schemaName] = schemaID
		cache.schemaIDToName[schemaID] = schemaName
		zap.S().Infow("Cached schema", "schema_id", schemaID, "schema_name", schemaName)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating schema rows: %w", err)
	}

	if len(cache.schemaNameToID) == 0 {
		return fmt.Errorf("no schemas found in registry table")
	}

	zap.S().Infow("Loaded schemas from database", "count", len(cache.schemaNameToID))
	return nil
}

// loadAttributeMetadataFromFiles loads attribute metadata from JSON files
func (ml *MetadataLoader) loadAttributeMetadataFromFiles(cache *MetadataCache) error {
	entries, err := os.ReadDir(ml.schemaDirectory)
	if err != nil {
		return fmt.Errorf("failed to read schema directory: %w", err)
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	// Load attribute metadata for each schema that exists in the registry
	for schemaName := range cache.schemaNameToID {
		attributesFile := filepath.Join(ml.schemaDirectory, schemaName+"_attributes.json")

		// Check if file exists
		if _, err := os.Stat(attributesFile); os.IsNotExist(err) {
			zap.S().Warnw("attribute file not found; skipping schema", "schema", schemaName)
			continue
		}

		// Read and parse attribute metadata
		data, err := os.ReadFile(attributesFile)
		if err != nil {
			return fmt.Errorf("failed to read attributes file %s: %w", attributesFile, err)
		}

		var rawAttributes map[string]map[string]interface{}
		if err := json.Unmarshal(data, &rawAttributes); err != nil {
			return fmt.Errorf("failed to parse attributes file %s: %w", attributesFile, err)
		}

		// Convert to AttributeMeta map
		attrMap := make(map[string]forma.AttributeMetadata)
		schemaCache := make(forma.SchemaAttributeCache)

		for attrName, attrData := range rawAttributes {
			meta, err := parseAttributeMetadata(attrName, attrData, attributesFile)
			if err != nil {
				return err
			}
			attrMap[attrName] = meta
			schemaCache[attrName] = meta
		}

		cache.attributeMetadata[schemaName] = attrMap
		cache.schemaCaches[schemaName] = schemaCache

		zap.S().Infow("Loaded attributes for schema", "count", len(attrMap), "schema", schemaName)
	}

	// Also check for any schema files without database entries
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if len(name) > len("_attributes.json") && name[len(name)-len("_attributes.json"):] == "_attributes.json" {
			schemaName := name[:len(name)-len("_attributes.json")]
			if _, exists := cache.schemaNameToID[schemaName]; !exists {
				zap.S().Warnw("attribute file present without registry entry", "schema", schemaName)
			}
		}
	}

	return nil
}
