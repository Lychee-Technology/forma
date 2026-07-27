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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
			ltbase_created_by TEXT,
			ltbase_updated_by TEXT,
			ltbase_deleted_by TEXT,
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
		_, _ = pool.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", schemaRegistryTable))
		_, _ = pool.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", entityMainTable))
		_, _ = pool.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", eavDataTable))
	})

	return schemaRegistryTable, entityMainTable, eavDataTable
}

// ---------------------------------------------------------------------------
// Integration Tests for NewEntityManagerWithConfig
// ---------------------------------------------------------------------------

func TestNewEntityManagerWithConfig_Integration_Success(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := connectTestPostgres(t, ctx)
	schemaRegistryTable, entityMainTable, eavDataTable := createTempTables(t, ctx, pool)

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
		EntityMain:     entityMainTable,
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
	schemaRegistryTable, entityMainTable, eavDataTable := createTempTables(t, ctx, pool)

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
		EntityMain:     entityMainTable,
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
	schemaRegistryTable, entityMainTable, eavDataTable := createTempTables(t, ctx, pool)

	// Don't insert any schemas - this will cause metadata loader to fail

	config := forma.DefaultConfig(newMockSchemaRegistry())
	config.Database.TableNames = forma.TableNames{
		SchemaRegistry: schemaRegistryTable,
		EAVData:        eavDataTable,
		EntityMain:     entityMainTable,
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
		_, _ = NewEntityManagerWithConfig(config, nil)
	})
}
