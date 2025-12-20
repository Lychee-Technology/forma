package internal

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// MetadataCache unit tests
// ---------------------------------------------------------------------------

func TestMetadataCache_GetSchemaID(t *testing.T) {
	mc := NewMetadataCache()
	mc.schemaNameToID["foo"] = 42

	id, ok := mc.GetSchemaID("foo")
	assert.True(t, ok)
	assert.Equal(t, int16(42), id)

	_, ok = mc.GetSchemaID("missing")
	assert.False(t, ok)
}

func TestMetadataCache_GetSchemaName(t *testing.T) {
	mc := NewMetadataCache()
	mc.schemaIDToName[7] = "bar"

	name, ok := mc.GetSchemaName(7)
	assert.True(t, ok)
	assert.Equal(t, "bar", name)

	_, ok = mc.GetSchemaName(999)
	assert.False(t, ok)
}

func TestMetadataCache_GetSchemaCache(t *testing.T) {
	mc := NewMetadataCache()
	mc.schemaNameToID["baz"] = 3
	mc.schemaCaches[3] = forma.SchemaAttributeCache{"attr": forma.AttributeMetadata{AttributeName: "attr"}}

	cache, ok := mc.GetSchemaCache("baz")
	require.True(t, ok)
	assert.Contains(t, cache, "attr")

	_, ok = mc.GetSchemaCache("missing")
	assert.False(t, ok)
}

func TestMetadataCache_GetSchemaCacheByID(t *testing.T) {
	mc := NewMetadataCache()
	mc.schemaIDToName[5] = "qux"
	mc.schemaNameToID["qux"] = 5
	mc.schemaCaches[5] = forma.SchemaAttributeCache{"a": forma.AttributeMetadata{}}

	cache, ok := mc.GetSchemaCacheByID(5)
	require.True(t, ok)
	assert.NotNil(t, cache)

	_, ok = mc.GetSchemaCacheByID(999)
	assert.False(t, ok)
}

func TestMetadataCache_ListSchemas(t *testing.T) {
	mc := NewMetadataCache()
	mc.schemaNameToID["x"] = 1
	mc.schemaNameToID["y"] = 2

	schemas := mc.ListSchemas()
	assert.ElementsMatch(t, []string{"x", "y"}, schemas)
}

// ---------------------------------------------------------------------------
// MetadataLoader unit tests using pgxmock
// ---------------------------------------------------------------------------

func TestLoadMetadata_Success(t *testing.T) {
	ctx := context.Background()

	// Create mock pool
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Temp dir for schema files
	dir := t.TempDir()

	// Prepare mock rows for schema registry
	rows := pgxmock.NewRows([]string{"schema_name", "schema_id"}).
		AddRow("alpha", int16(1)).
		AddRow("beta", int16(2))
	mock.ExpectQuery(`SELECT schema_name, schema_id FROM test_registry`).WillReturnRows(rows)

	// Create attribute file for alpha only
	alphaAttrs := map[string]any{
		"name": map[string]any{
			"attributeID": float64(10),
			"valueType":   "text",
		},
	}
	writeJSONFile(t, filepath.Join(dir, "alpha_attributes.json"), alphaAttrs)

	// Create orphan attribute file (no registry entry)
	writeJSONFile(t, filepath.Join(dir, "orphan_attributes.json"), map[string]any{})

	loader := NewMetadataLoader(mock, "test_registry", dir)
	cache, err := loader.LoadMetadata(ctx)
	require.NoError(t, err)

	// Verify schema mappings
	assert.Equal(t, int16(1), cache.schemaNameToID["alpha"])
	assert.Equal(t, int16(2), cache.schemaNameToID["beta"])

	// Verify attributes loaded for alpha
	require.Contains(t, cache.attributeMetadata, int16(1))
	assert.Contains(t, cache.attributeMetadata[int16(1)], "name")

	// beta has no attribute file, so empty map
	assert.NotContains(t, cache.attributeMetadata, int16(2))

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadMetadata_SchemaRegistryQueryError(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT schema_name, schema_id FROM tbl`).WillReturnError(errors.New("db error"))

	loader := NewMetadataLoader(mock, "tbl", t.TempDir())
	_, err = loader.LoadMetadata(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load schema registry")
	assert.Contains(t, err.Error(), "db error")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadMetadata_NoSchemasError(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows([]string{"schema_name", "schema_id"})
	mock.ExpectQuery(`SELECT schema_name, schema_id FROM empty`).WillReturnRows(rows)

	loader := NewMetadataLoader(mock, "empty", t.TempDir())
	_, err = loader.LoadMetadata(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no schemas found")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadMetadata_AttributeFileReadError(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	dir := t.TempDir()

	rows := pgxmock.NewRows([]string{"schema_name", "schema_id"}).AddRow("bad", int16(1))
	mock.ExpectQuery(`SELECT schema_name, schema_id FROM reg`).WillReturnRows(rows)

	// Create a directory instead of file to cause read error
	require.NoError(t, os.Mkdir(filepath.Join(dir, "bad_attributes.json"), 0o755))

	loader := NewMetadataLoader(mock, "reg", dir)
	_, err = loader.LoadMetadata(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load attribute metadata")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadMetadata_InvalidJSON(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	dir := t.TempDir()

	rows := pgxmock.NewRows([]string{"schema_name", "schema_id"}).AddRow("inv", int16(1))
	mock.ExpectQuery(`SELECT schema_name, schema_id FROM reg`).WillReturnRows(rows)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "inv_attributes.json"), []byte("{invalid"), 0o644))

	loader := NewMetadataLoader(mock, "reg", dir)
	_, err = loader.LoadMetadata(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse attributes file")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadMetadata_ParseAttributeError(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	dir := t.TempDir()

	rows := pgxmock.NewRows([]string{"schema_name", "schema_id"}).AddRow("bad", int16(1))
	mock.ExpectQuery(`SELECT schema_name, schema_id FROM reg`).WillReturnRows(rows)

	// Missing required attributeID
	badAttrs := map[string]any{
		"foo": map[string]any{
			"valueType": "text",
		},
	}
	writeJSONFile(t, filepath.Join(dir, "bad_attributes.json"), badAttrs)

	loader := NewMetadataLoader(mock, "reg", dir)
	_, err = loader.LoadMetadata(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "attributeID")

	require.NoError(t, mock.ExpectationsWereMet())
}

// helper to write JSON to file
func writeJSONFile(t *testing.T, path string, v any) {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o644))
}
