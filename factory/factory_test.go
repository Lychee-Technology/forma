package factory

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Mock SchemaRegistry for testing
// ---------------------------------------------------------------------------

type mockSchemaRegistry struct {
	nameToID map[string]int16
	idToName map[int16]string
	schemas  map[string]forma.SchemaAttributeCache
}

func newMockSchemaRegistry() *mockSchemaRegistry {
	return &mockSchemaRegistry{
		nameToID: map[string]int16{"test": 1},
		idToName: map[int16]string{1: "test"},
		schemas: map[string]forma.SchemaAttributeCache{
			"test": {
				"id": forma.AttributeMetadata{
					AttributeName: "id",
					AttributeID:   1,
					ValueType:     forma.ValueTypeText,
				},
			},
		},
	}
}

func (m *mockSchemaRegistry) GetSchemaAttributeCacheByName(name string) (int16, forma.SchemaAttributeCache, error) {
	id, ok := m.nameToID[name]
	if !ok {
		return 0, nil, fmt.Errorf("schema not found: %s", name)
	}
	cache := m.schemas[name]
	return id, cache, nil
}

func (m *mockSchemaRegistry) GetSchemaAttributeCacheByID(id int16) (string, forma.SchemaAttributeCache, error) {
	name, ok := m.idToName[id]
	if !ok {
		return "", nil, fmt.Errorf("schema not found for ID: %d", id)
	}
	cache := m.schemas[name]
	return name, cache, nil
}

func (m *mockSchemaRegistry) GetSchemaByName(name string) (int16, forma.JSONSchema, error) {
	id, ok := m.nameToID[name]
	if !ok {
		return 0, forma.JSONSchema{}, fmt.Errorf("schema not found: %s", name)
	}
	return id, forma.JSONSchema{ID: id, Name: name}, nil
}

func (m *mockSchemaRegistry) GetSchemaByID(id int16) (string, forma.JSONSchema, error) {
	name, ok := m.idToName[id]
	if !ok {
		return "", forma.JSONSchema{}, fmt.Errorf("schema not found for ID: %d", id)
	}
	return name, forma.JSONSchema{ID: id, Name: name}, nil
}

func (m *mockSchemaRegistry) ListSchemas() []string {
	schemas := make([]string, 0, len(m.nameToID))
	for name := range m.nameToID {
		schemas = append(schemas, name)
	}
	return schemas
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

func writeAttributesFile(t *testing.T, dir, schemaName string, attrs map[string]any) {
	t.Helper()
	data, err := json.Marshal(attrs)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(dir, schemaName+"_attributes.json"), data, 0644)
	require.NoError(t, err)
}

func writeSchemaFile(t *testing.T, dir, schemaName string, schema map[string]any) {
	t.Helper()
	data, err := json.Marshal(schema)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(dir, schemaName+".json"), data, 0644)
	require.NoError(t, err)
}

// connectTestPostgres establishes a connection to the test PostgreSQL database.
// Skips the test if DATABASE_URL is not set.
func connectTestPostgres(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL not set; skipping integration test")
	}

	pool, err := pgxpool.New(ctx, dbURL)
	require.NoError(t, err)

	t.Cleanup(func() {
		pool.Close()
	})

	return pool
}

// createTempTables creates temporary tables for testing and returns their names.
func createTempTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (schemaRegistryTable, entityMainTable, eavDataTable string) {
	t.Helper()

	suffix := time.Now().UnixNano()
	schemaRegistryTable = fmt.Sprintf("schema_registry_test_%d", suffix)
	entityMainTable = fmt.Sprintf("entity_main_test_%d", suffix)
	eavDataTable = fmt.Sprintf("eav_data_test_%d", suffix)

	// Create schema registry table
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			schema_name TEXT PRIMARY KEY,
			schema_id SMALLINT NOT NULL
		)
	`, schemaRegistryTable))
	require.NoError(t, err)

	// Create entity main table (simplified version)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			ltbase_schema_id SMALLINT NOT NULL,
			ltbase_row_id UUID NOT NULL,
			ltbase_created_at BIGINT,
			ltbase_updated_at BIGINT,
			ltbase_deleted_at BIGINT,
			text_01 TEXT,
			text_02 TEXT,
			text_03 TEXT,
			text_04 TEXT,
			text_05 TEXT,
			text_06 TEXT,
			text_07 TEXT,
			text_08 TEXT,
			text_09 TEXT,
			text_10 TEXT,
			smallint_01 SMALLINT,
			smallint_02 SMALLINT,
			smallint_03 SMALLINT,
			integer_01 INTEGER,
			integer_02 INTEGER,
			integer_03 INTEGER,
			bigint_01 BIGINT,
			bigint_02 BIGINT,
			bigint_03 BIGINT,
			double_01 DOUBLE PRECISION,
			double_02 DOUBLE PRECISION,
			double_03 DOUBLE PRECISION,
			uuid_01 UUID,
			uuid_02 UUID,
			PRIMARY KEY (ltbase_schema_id, ltbase_row_id)
		)
	`, entityMainTable))
	require.NoError(t, err)

	// Create EAV data table
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			schema_id SMALLINT NOT NULL,
			row_id UUID NOT NULL,
			attr_id SMALLINT NOT NULL,
			array_indices TEXT,
			value_text TEXT,
			value_numeric DOUBLE PRECISION
		)
	`, eavDataTable))
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		pool.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", schemaRegistryTable))
		pool.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", entityMainTable))
		pool.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", eavDataTable))
	})

	return schemaRegistryTable, entityMainTable, eavDataTable
}

// ---------------------------------------------------------------------------
// Unit tests for collectTablesFromPool (uses pgxmock)
// ---------------------------------------------------------------------------

func TestCollectTablesFromPool_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT table_name FROM information_schema.tables`).WillReturnError(assert.AnError)

	_, err = collectTablesFromPool(mock)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to verify database connection")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCollectTablesFromPool_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows([]string{"table_name"}).
		AddRow("schema_registry").
		AddRow("eav_data")
	mock.ExpectQuery(`SELECT table_name FROM information_schema.tables`).WillReturnRows(rows)

	tables, err := collectTablesFromPool(mock)
	require.NoError(t, err)
	assert.Contains(t, tables, "schema_registry")
	assert.Contains(t, tables, "eav_data")
	require.NoError(t, mock.ExpectationsWereMet())
}


// ---------------------------------------------------------------------------
// Unit tests for NewEntityManagerWithConfig (uses test hooks + mock loader)
// ---------------------------------------------------------------------------

type mockMetadataLoader struct {
	cache *internal.MetadataCache
	err   error
}

func (m *mockMetadataLoader) LoadMetadata(ctx context.Context) (*internal.MetadataCache, error) {
	return m.cache, m.err
}

func withTableCollector(t *testing.T, collector func(queryPool) ([]string, error)) {
	t.Helper()
	original := tableCollector
	tableCollector = collector
	t.Cleanup(func() {
		tableCollector = original
	})
}

func withMetadataLoaderFactory(t *testing.T, factory func(pool *pgxpool.Pool, schemaTable, schemaDir string) metadataLoader) {
	t.Helper()
	original := defaultMetadataLoaderFactory
	defaultMetadataLoaderFactory = factory
	t.Cleanup(func() {
		defaultMetadataLoaderFactory = original
	})
}

func TestNewEntityManagerWithConfig_Unit_TableCollectorError(t *testing.T) {
	withTableCollector(t, func(pool queryPool) ([]string, error) {
		return nil, assert.AnError
	})

	config := forma.DefaultConfig(newMockSchemaRegistry())

	em, err := NewEntityManagerWithConfig(config, nil)

	assert.Nil(t, em)
	assert.Error(t, err)
}

func TestNewEntityManagerWithConfig_Unit_MissingRequiredTables(t *testing.T) {
	withTableCollector(t, func(pool queryPool) ([]string, error) {
		return []string{"schema_registry"}, nil
	})

	config := forma.DefaultConfig(newMockSchemaRegistry())
	config.Database.TableNames = forma.TableNames{
		SchemaRegistry: "schema_registry",
		EAVData:        "eav_data",
	}

	em, err := NewEntityManagerWithConfig(config, nil)

	assert.Nil(t, em)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "required tables are missing")
}

func TestNewEntityManagerWithConfig_Unit_MetadataLoaderError(t *testing.T) {
	cache := internal.NewMetadataCache()
	withTableCollector(t, func(pool queryPool) ([]string, error) {
		return []string{"schema_registry", "eav_data"}, nil
	})
	withMetadataLoaderFactory(t, func(pool *pgxpool.Pool, schemaTable, schemaDir string) metadataLoader {
		return &mockMetadataLoader{cache: cache, err: fmt.Errorf("simulated loader error")}
	})

	config := forma.DefaultConfig(newMockSchemaRegistry())
	config.Database.TableNames = forma.TableNames{
		SchemaRegistry: "schema_registry",
		EAVData:        "eav_data",
	}
	config.Entity.SchemaDirectory = t.TempDir()

	em, err := NewEntityManagerWithConfig(config, nil)

	assert.Nil(t, em)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load metadata")
}

func TestNewEntityManagerWithConfig_Unit_NilSchemaRegistry(t *testing.T) {
	cache := internal.NewMetadataCache()
	withTableCollector(t, func(pool queryPool) ([]string, error) {
		return []string{"schema_registry", "eav_data"}, nil
	})
	withMetadataLoaderFactory(t, func(pool *pgxpool.Pool, schemaTable, schemaDir string) metadataLoader {
		return &mockMetadataLoader{cache: cache, err: nil}
	})

	config := forma.DefaultConfig(nil)
	config.Database.TableNames = forma.TableNames{
		SchemaRegistry: "schema_registry",
		EAVData:        "eav_data",
	}
	config.Entity.SchemaDirectory = t.TempDir()

	em, err := NewEntityManagerWithConfig(config, nil)

	assert.Nil(t, em)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "config.SchemaRegistry is required")
}

func TestNewEntityManagerWithConfig_Unit_Success(t *testing.T) {
	cache := internal.NewMetadataCache()
	withTableCollector(t, func(pool queryPool) ([]string, error) {
		return []string{"schema_registry", "eav_data"}, nil
	})
	withMetadataLoaderFactory(t, func(pool *pgxpool.Pool, schemaTable, schemaDir string) metadataLoader {
		return &mockMetadataLoader{cache: cache, err: nil}
	})

	config := forma.DefaultConfig(newMockSchemaRegistry())
	config.Database.TableNames = forma.TableNames{
		SchemaRegistry: "schema_registry",
		EAVData:        "eav_data",
	}
	config.Entity.SchemaDirectory = t.TempDir()

	em, err := NewEntityManagerWithConfig(config, nil)

	assert.NotNil(t, em)
	assert.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Integration Tests for NewEntityManagerWithConfig
// ---------------------------------------------------------------------------

func TestNewEntityManagerWithConfig_Integration_Success(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := connectTestPostgres(t, ctx)
	schemaRegistryTable, _, eavDataTable := createTempTables(t, ctx, pool)

	// Create temp schema directory
	dir := t.TempDir()

	// Insert schema into registry
	_, err := pool.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s (schema_name, schema_id) VALUES ($1, $2)",
		schemaRegistryTable,
	), "test", int16(1))
	require.NoError(t, err)

	// Write attributes file
	writeAttributesFile(t, dir, "test", map[string]any{
		"id": map[string]any{
			"attributeID": float64(1),
			"valueType":   "text",
		},
	})

	// Write schema file
	writeSchemaFile(t, dir, "test", map[string]any{
		"type": "object",
		"properties": map[string]any{
			"id": map[string]any{"type": "string"},
		},
	})

	config := forma.DefaultConfig(newMockSchemaRegistry())
	config.Database.TableNames = forma.TableNames{
		SchemaRegistry: schemaRegistryTable,
		EAVData:        eavDataTable,
	}
	config.Entity.SchemaDirectory = dir

	em, err := NewEntityManagerWithConfig(config, pool)

	assert.NoError(t, err)
	assert.NotNil(t, em)
}

func TestNewEntityManagerWithConfig_Integration_MissingTables(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := connectTestPostgres(t, ctx)

	// Use non-existent table names
	config := forma.DefaultConfig(newMockSchemaRegistry())
	config.Database.TableNames = forma.TableNames{
		SchemaRegistry: "nonexistent_schema_registry",
		EAVData:        "nonexistent_eav_data",
	}
	config.Entity.SchemaDirectory = t.TempDir()

	em, err := NewEntityManagerWithConfig(config, pool)

	assert.Nil(t, em)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "required tables are missing")
}

func TestNewEntityManagerWithConfig_Integration_NilSchemaRegistry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := connectTestPostgres(t, ctx)
	schemaRegistryTable, _, eavDataTable := createTempTables(t, ctx, pool)

	// Create temp schema directory
	dir := t.TempDir()

	// Insert schema into registry
	_, err := pool.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s (schema_name, schema_id) VALUES ($1, $2)",
		schemaRegistryTable,
	), "test", int16(1))
	require.NoError(t, err)

	// Write attributes file
	writeAttributesFile(t, dir, "test", map[string]any{
		"id": map[string]any{
			"attributeID": float64(1),
			"valueType":   "text",
		},
	})

	// Create config with nil SchemaRegistry
	config := forma.DefaultConfig(nil)
	config.Database.TableNames = forma.TableNames{
		SchemaRegistry: schemaRegistryTable,
		EAVData:        eavDataTable,
	}
	config.Entity.SchemaDirectory = dir
	config.SchemaRegistry = nil

	em, err := NewEntityManagerWithConfig(config, pool)

	assert.Nil(t, em)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "config.SchemaRegistry is required")
}

func TestNewEntityManagerWithConfig_Integration_MetadataLoaderError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := connectTestPostgres(t, ctx)
	schemaRegistryTable, _, eavDataTable := createTempTables(t, ctx, pool)

	// Don't insert any schemas - this will cause metadata loader to fail

	config := forma.DefaultConfig(newMockSchemaRegistry())
	config.Database.TableNames = forma.TableNames{
		SchemaRegistry: schemaRegistryTable,
		EAVData:        eavDataTable,
	}
	config.Entity.SchemaDirectory = t.TempDir()

	em, err := NewEntityManagerWithConfig(config, pool)

	assert.Nil(t, em)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load metadata")
}

func TestNewEntityManagerWithConfig_NilPool(t *testing.T) {
	config := forma.DefaultConfig(newMockSchemaRegistry())

	// This will panic with nil pool - we verify the function doesn't handle nil gracefully
	assert.Panics(t, func() {
		NewEntityManagerWithConfig(config, nil)
	})
}

// ---------------------------------------------------------------------------
// Integration Tests for NewFileSchemaRegistry
// ---------------------------------------------------------------------------

func TestNewFileSchemaRegistry_Integration_Success(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := connectTestPostgres(t, ctx)

	// Create schema registry table
	suffix := time.Now().UnixNano()
	tableName := fmt.Sprintf("schema_registry_fsr_%d", suffix)

	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			schema_name TEXT PRIMARY KEY,
			schema_id SMALLINT NOT NULL
		)
	`, tableName))
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		pool.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	})

	// Insert schemas
	_, err = pool.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s (schema_name, schema_id) VALUES ($1, $2)",
		tableName,
	), "test", int16(1))
	require.NoError(t, err)

	// Create temp schema directory with required files
	dir := t.TempDir()
	writeAttributesFile(t, dir, "test", map[string]any{
		"id": map[string]any{
			"attributeID": float64(1),
			"valueType":   "text",
		},
	})
	writeSchemaFile(t, dir, "test", map[string]any{
		"type": "object",
		"properties": map[string]any{
			"id": map[string]any{"type": "string"},
		},
	})

	registry, err := NewFileSchemaRegistry(pool, tableName, dir)

	assert.NoError(t, err)
	assert.NotNil(t, registry)

	// Verify registry works
	schemas := registry.ListSchemas()
	assert.Contains(t, schemas, "test")

	schemaID, cache, err := registry.GetSchemaAttributeCacheByName("test")
	assert.NoError(t, err)
	assert.Equal(t, int16(1), schemaID)
	assert.Contains(t, cache, "id")
}

func TestNewFileSchemaRegistry_Integration_EmptyRegistry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := connectTestPostgres(t, ctx)

	// Create empty schema registry table
	suffix := time.Now().UnixNano()
	tableName := fmt.Sprintf("schema_registry_empty_%d", suffix)

	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			schema_name TEXT PRIMARY KEY,
			schema_id SMALLINT NOT NULL
		)
	`, tableName))
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		pool.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	})

	// Don't insert any schemas

	registry, err := NewFileSchemaRegistry(pool, tableName, t.TempDir())

	assert.Nil(t, registry)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no schemas found")
}

func TestNewFileSchemaRegistry_Integration_InvalidDirectory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := connectTestPostgres(t, ctx)

	// Create schema registry table with a schema entry
	suffix := time.Now().UnixNano()
	tableName := fmt.Sprintf("schema_registry_invdir_%d", suffix)

	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			schema_name TEXT PRIMARY KEY,
			schema_id SMALLINT NOT NULL
		)
	`, tableName))
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		pool.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	})

	// Insert a schema
	_, err = pool.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s (schema_name, schema_id) VALUES ($1, $2)",
		tableName,
	), "test", int16(1))
	require.NoError(t, err)

	// Use non-existent directory
	registry, err := NewFileSchemaRegistry(pool, tableName, "/nonexistent/directory")

	assert.Nil(t, registry)
	assert.Error(t, err)
}

func TestNewFileSchemaRegistry_Integration_MissingAttributesFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := connectTestPostgres(t, ctx)

	// Create schema registry table
	suffix := time.Now().UnixNano()
	tableName := fmt.Sprintf("schema_registry_noattr_%d", suffix)

	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			schema_name TEXT PRIMARY KEY,
			schema_id SMALLINT NOT NULL
		)
	`, tableName))
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		pool.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	})

	// Insert a schema
	_, err = pool.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s (schema_name, schema_id) VALUES ($1, $2)",
		tableName,
	), "test", int16(1))
	require.NoError(t, err)

	// Create empty directory (no attributes file)
	dir := t.TempDir()

	registry, err := NewFileSchemaRegistry(pool, tableName, dir)

	assert.Nil(t, registry)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read attributes file")
}

func TestNewFileSchemaRegistry_NilPool(t *testing.T) {
	// This will panic with nil pool
	assert.Panics(t, func() {
		NewFileSchemaRegistry(nil, "test_table", t.TempDir())
	})
}
