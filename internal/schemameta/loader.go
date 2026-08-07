package schemameta

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlutil"
	"go.uber.org/zap"
)

// MetadataCache holds all metadata mappings for fast lookups
type MetadataCache struct {
	mu sync.RWMutex

	// Schema mappings
	schemaNameToID map[string]int16
	schemaIDToName map[int16]string

	// Attribute mappings: (schema_id, attr_name) -> AttributeMeta
	attributeMetadata map[int16]map[string]forma.AttributeMetadata

	// Schema caches for transformer
	schemaCaches map[int16]forma.SchemaAttributeCache

	// fingerprints holds a deterministic content hash per schema, computed at
	// registration; plan caches key on it so metadata content changes orphan
	// stale entries (#142).
	fingerprints map[int16]string
}

// NewMetadataCache creates a new metadata cache
func NewMetadataCache() *MetadataCache {
	return &MetadataCache{
		schemaNameToID:    make(map[string]int16),
		schemaIDToName:    make(map[int16]string),
		attributeMetadata: make(map[int16]map[string]forma.AttributeMetadata),
		schemaCaches:      make(map[int16]forma.SchemaAttributeCache),
		fingerprints:      make(map[int16]string),
	}
}

func (mc *MetadataCache) RegisterSchema(schemaName string, schemaID int16, cache forma.SchemaAttributeCache) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if existingName, exists := mc.schemaIDToName[schemaID]; exists && existingName != schemaName {
		return fmt.Errorf("duplicate schema id %d for %s and %s", schemaID, existingName, schemaName)
	}
	if existingID, exists := mc.schemaNameToID[schemaName]; exists && existingID != schemaID {
		return fmt.Errorf("duplicate schema name %s for ids %d and %d", schemaName, existingID, schemaID)
	}
	// Store a private copy: the caller keeps ownership of its map, so later
	// caller-side mutations cannot silently invalidate cached plans (#142).
	// Retired entries are a validation-only ledger and never reach consumers (#342).
	snapshot := activeAttributeCache(copySchemaAttributeCache(cache))
	mc.schemaNameToID[schemaName] = schemaID
	mc.schemaIDToName[schemaID] = schemaName
	mc.schemaCaches[schemaID] = snapshot
	mc.attributeMetadata[schemaID] = map[string]forma.AttributeMetadata(snapshot)
	mc.fingerprints[schemaID] = fingerprintSchema(schemaName, snapshot)
	return nil
}

func copySchemaAttributeCache(cache forma.SchemaAttributeCache) forma.SchemaAttributeCache {
	snapshot := make(forma.SchemaAttributeCache, len(cache))
	for name, meta := range cache {
		if meta.ColumnBinding != nil {
			binding := *meta.ColumnBinding
			meta.ColumnBinding = &binding
		}
		snapshot[name] = meta
	}
	return snapshot
}

// fingerprintSchema hashes the registered metadata content deterministically
// (sorted by attribute name) so equal content yields equal fingerprints.
func fingerprintSchema(schemaName string, cache forma.SchemaAttributeCache) string {
	names := make([]string, 0, len(cache))
	for name := range cache {
		names = append(names, name)
	}
	sort.Strings(names)

	h := sha256.New()
	write := func(parts ...string) {
		for _, p := range parts {
			_, _ = h.Write([]byte(p))
			_, _ = h.Write([]byte{0})
		}
	}
	write("schema-fp-v1", schemaName)
	for _, name := range names {
		meta := cache[name]
		write(name, strconv.Itoa(int(meta.AttributeID)), string(meta.ValueType), strconv.FormatBool(meta.Required))
		if meta.ColumnBinding != nil {
			write(string(meta.ColumnBinding.ColumnName), string(meta.ColumnBinding.Encoding))
		} else {
			write("eav")
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

// SchemaFingerprint returns the content fingerprint recorded at registration;
// plan-cache keys include it as the invalidation lever.
func (mc *MetadataCache) SchemaFingerprint(schemaID int16) (string, bool) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	fp, ok := mc.fingerprints[schemaID]
	return fp, ok
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

// GetSchemaCache retrieves the schema attribute cache for a schema
// (thread-safe). The returned map is the registry-owned snapshot — callers
// must treat it as read-only.
func (mc *MetadataCache) GetSchemaCache(schemaName string) (forma.SchemaAttributeCache, bool) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	schemaId, ok := mc.schemaNameToID[schemaName]
	if !ok {
		zap.S().Warnw("schema name not found in cache", "schema_name", schemaName, "cache_size", len(mc.schemaNameToID))
		return nil, false
	}
	cache, ok := mc.schemaCaches[schemaId]
	if !ok {
		zap.S().Warnw("schema not found in cache", "schema", schemaName, "cache_size", len(mc.schemaCaches))
	}
	return cache, ok
}

// GetSchemaCacheByID is the schema-ID variant of GetSchemaCache; the same
// read-only contract applies.
func (mc *MetadataCache) GetSchemaCacheByID(schemaID int16) (forma.SchemaAttributeCache, bool) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	cache, ok := mc.schemaCaches[schemaID]
	return cache, ok
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

// DBPool is a minimal interface for the methods MetadataLoader needs.
type DBPool interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// MetadataLoader loads schema and attribute metadata from database and JSON files
type MetadataLoader struct {
	pool            DBPool
	schemaTableName string
	schemaDirectory string
}

// NewMetadataLoader creates a new metadata loader
func NewMetadataLoader(pool DBPool, schemaTableName, schemaDirectory string) *MetadataLoader {
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
	query := fmt.Sprintf("SELECT schema_name, schema_id FROM %s", sqlutil.SanitizeIdentifier(ml.schemaTableName))

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

		if existingName, exists := cache.schemaIDToName[schemaID]; exists {
			return fmt.Errorf("duplicate schema id %d for %s and %s", schemaID, existingName, schemaName)
		}
		if existingID, exists := cache.schemaNameToID[schemaName]; exists {
			return fmt.Errorf("duplicate schema name %s for ids %d and %d", schemaName, existingID, schemaID)
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
	for schemaName, schemaID := range cache.schemaNameToID {
		zap.S().Infow("Loading attributes for schema", "schema_name", schemaName, "schema_id", schemaID)
		if cache.attributeMetadata[schemaID] != nil && cache.schemaCaches[schemaID] != nil {
			continue // already loaded
		}
		attributesFile := filepath.Join(ml.schemaDirectory, schemaName+"_attributes.json")

		// Registry entries without attribute files are a bootstrap error. If we
		// continue, requests will fail later with missing cache lookups.
		if _, err := os.Stat(attributesFile); os.IsNotExist(err) {
			return fmt.Errorf("attributes file not found for schema %s: %s", schemaName, attributesFile)
		}

		// Read and parse attribute metadata
		data, err := os.ReadFile(attributesFile)
		if err != nil {
			return fmt.Errorf("failed to read attributes file %s: %w", attributesFile, err)
		}

		var rawAttributes map[string]map[string]any
		if err := json.Unmarshal(data, &rawAttributes); err != nil {
			return fmt.Errorf("failed to parse attributes file %s: %w", attributesFile, err)
		}

		// Convert to AttributeMeta map
		schemaCache := make(forma.SchemaAttributeCache)

		for attrName, attrData := range rawAttributes {
			meta, err := parseAttributeMetadata(attrName, attrData, attributesFile)
			if err != nil {
				return err
			}
			schemaCache[attrName] = meta
		}
		// Validate on the FULL cache (retired entries included) and only then
		// strip them: the retired ledger guards id/binding reuse (#342).
		if err := validateSchemaAttributeCache(schemaName, schemaCache); err != nil {
			return err
		}
		active := activeAttributeCache(schemaCache)

		cache.attributeMetadata[schemaID] = map[string]forma.AttributeMetadata(active)
		cache.schemaCaches[schemaID] = active

		zap.S().Infow("Loaded attributes for schema", "count", len(active), "schema", schemaName)
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
