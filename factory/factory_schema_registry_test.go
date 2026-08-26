package factory

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Integration Tests for NewFileSchemaRegistry
// ---------------------------------------------------------------------------

func TestNewFileSchemaRegistry_Integration_Success(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testdb.Connect(t, ctx)

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
		_, _ = pool.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
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

	pool := testdb.Connect(t, ctx)

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
		_, _ = pool.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
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

	pool := testdb.Connect(t, ctx)

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
		_, _ = pool.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
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

	pool := testdb.Connect(t, ctx)

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
		_, _ = pool.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
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

func TestNewFileSchemaRegistry_Integration_DuplicateSchemaIDFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testdb.Connect(t, ctx)

	suffix := time.Now().UnixNano()
	tableName := fmt.Sprintf("schema_registry_dup_id_%d", suffix)

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
		_, _ = pool.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	})

	_, err = pool.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s (schema_name, schema_id) VALUES ($1, $2), ($3, $4)",
		tableName,
	), "alpha", int16(1), "beta", int16(1))
	require.NoError(t, err)

	dir := t.TempDir()
	writeAttributesFile(t, dir, "alpha", map[string]any{
		"id": map[string]any{"attributeID": float64(1), "valueType": "text"},
	})
	writeAttributesFile(t, dir, "beta", map[string]any{
		"id": map[string]any{"attributeID": float64(2), "valueType": "text"},
	})

	registry, err := NewFileSchemaRegistry(pool, tableName, dir)
	assert.Nil(t, registry)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate schema id")
}

func TestNewFileSchemaRegistry_NilPool(t *testing.T) {
	// This will panic with nil pool
	assert.Panics(t, func() {
		_, _ = NewFileSchemaRegistry(nil, "test_table", t.TempDir())
	})
}
