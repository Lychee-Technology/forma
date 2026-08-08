package schemameta

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlutil"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sanitizeIdentifier(name string) string {
	return sqlutil.SanitizeIdentifier(name)
}

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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT schema_name, schema_id FROM ` + sanitizeIdentifier("test_registry"))).WillReturnRows(rows)

	// Create attribute files for each registered schema.
	alphaAttrs := map[string]any{
		"name": map[string]any{
			"attributeID": float64(10),
			"valueType":   "text",
		},
	}
	betaAttrs := map[string]any{
		"status": map[string]any{
			"attributeID": float64(20),
			"valueType":   "text",
		},
	}
	writeJSONFile(t, filepath.Join(dir, "alpha_attributes.json"), alphaAttrs)
	writeJSONFile(t, filepath.Join(dir, "beta_attributes.json"), betaAttrs)

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

	require.Contains(t, cache.attributeMetadata, int16(2))
	assert.Contains(t, cache.attributeMetadata[int16(2)], "status")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadMetadata_SchemaRegistryQueryError(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT schema_name, schema_id FROM ` + sanitizeIdentifier("tbl"))).WillReturnError(errors.New("db error"))

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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT schema_name, schema_id FROM ` + sanitizeIdentifier("empty"))).WillReturnRows(rows)

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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT schema_name, schema_id FROM ` + sanitizeIdentifier("reg"))).WillReturnRows(rows)

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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT schema_name, schema_id FROM ` + sanitizeIdentifier("reg"))).WillReturnRows(rows)

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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT schema_name, schema_id FROM ` + sanitizeIdentifier("reg"))).WillReturnRows(rows)

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

func TestLoadMetadata_MissingAttributesFileFails(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows([]string{"schema_name", "schema_id"}).AddRow("missing", int16(1))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT schema_name, schema_id FROM ` + sanitizeIdentifier("tenant.schema_registry"))).WillReturnRows(rows)

	loader := NewMetadataLoader(mock, "tenant.schema_registry", t.TempDir())
	_, err = loader.LoadMetadata(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "attributes file not found for schema missing")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadMetadata_DuplicateSchemaIDFails(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	dir := t.TempDir()

	rows := pgxmock.NewRows([]string{"schema_name", "schema_id"}).
		AddRow("alpha", int16(1)).
		AddRow("beta", int16(1))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT schema_name, schema_id FROM ` + sanitizeIdentifier("dup_reg"))).WillReturnRows(rows)

	writeJSONFile(t, filepath.Join(dir, "alpha_attributes.json"), map[string]any{
		"name": map[string]any{"attributeID": float64(10), "valueType": "text"},
	})
	writeJSONFile(t, filepath.Join(dir, "beta_attributes.json"), map[string]any{
		"status": map[string]any{"attributeID": float64(20), "valueType": "text"},
	})

	loader := NewMetadataLoader(mock, "dup_reg", dir)
	_, err = loader.LoadMetadata(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate schema id")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadMetadata_DuplicateAttributeIDFails(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	dir := t.TempDir()

	rows := pgxmock.NewRows([]string{"schema_name", "schema_id"}).AddRow("dupattrs", int16(1))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT schema_name, schema_id FROM ` + sanitizeIdentifier("dup_attr_reg"))).WillReturnRows(rows)

	writeJSONFile(t, filepath.Join(dir, "dupattrs_attributes.json"), map[string]any{
		"first":  map[string]any{"attributeID": float64(7), "valueType": "text"},
		"second": map[string]any{"attributeID": float64(7), "valueType": "text"},
	})

	loader := NewMetadataLoader(mock, "dup_attr_reg", dir)
	_, err = loader.LoadMetadata(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate attribute id")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadMetadata_DuplicateColumnBindingFails(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	dir := t.TempDir()

	rows := pgxmock.NewRows([]string{"schema_name", "schema_id"}).AddRow("dupbinding", int16(1))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT schema_name, schema_id FROM ` + sanitizeIdentifier("dup_binding_reg"))).WillReturnRows(rows)

	writeJSONFile(t, filepath.Join(dir, "dupbinding_attributes.json"), map[string]any{
		"first": map[string]any{
			"attributeID": float64(7),
			"valueType":   "text",
			"column_binding": map[string]any{
				"col_name": "text_01",
			},
		},
		"second": map[string]any{
			"attributeID": float64(8),
			"valueType":   "text",
			"column_binding": map[string]any{
				"col_name": "text_01",
			},
		},
	})

	loader := NewMetadataLoader(mock, "dup_binding_reg", dir)
	_, err = loader.LoadMetadata(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate column binding")

	require.NoError(t, mock.ExpectationsWereMet())
}

// helper to write JSON to file
func writeJSONFile(t *testing.T, path string, v any) {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o644))
}

func TestRegisterSchemaCopiesInput(t *testing.T) {
	mc := NewMetadataCache()
	original := forma.SchemaAttributeCache{
		"name": {AttributeID: 5, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_01")}},
	}
	require.NoError(t, mc.RegisterSchema("s", 1, original))

	// Caller-side mutations must not reach the registered snapshot (#142).
	original["injected"] = forma.AttributeMetadata{AttributeID: 99}
	original["name"].ColumnBinding.ColumnName = forma.MainColumn("text_09")

	snap, ok := mc.GetSchemaCacheByID(1)
	require.True(t, ok)
	require.NotContains(t, snap, "injected")
	require.Equal(t, forma.MainColumn("text_01"), snap["name"].ColumnBinding.ColumnName)
}

// TestLoadMetadataCacheFieldsAreIndependent pins that the file path stores
// attributeMetadata and schemaCaches on two independent maps, deep copy
// included: the two fields are handed to different consumers, so a mutation
// reaching one must never be observable through the other.
func TestLoadMetadataCacheFieldsAreIndependent(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	dir := t.TempDir()
	rows := pgxmock.NewRows([]string{"schema_name", "schema_id"}).AddRow("user", int16(1))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT schema_name, schema_id FROM ` + sanitizeIdentifier("test_registry"))).WillReturnRows(rows)
	writeJSONFile(t, filepath.Join(dir, "user_attributes.json"), map[string]any{
		"name": map[string]any{
			"attributeID":    float64(1),
			"valueType":      "text",
			"column_binding": map[string]any{"col_name": "text_01"},
		},
	})

	cache, err := NewMetadataLoader(mock, "test_registry", dir).LoadMetadata(context.Background())
	require.NoError(t, err)

	cache.attributeMetadata[1]["injected"] = forma.AttributeMetadata{AttributeID: 99}
	cache.attributeMetadata[1]["name"].ColumnBinding.ColumnName = forma.MainColumn("text_09")

	schemaCache, ok := cache.GetSchemaCacheByID(1)
	require.True(t, ok)
	assert.NotContains(t, schemaCache, "injected", "the two fields must not share one map")
	assert.Equal(t, forma.MainColumn("text_01"), schemaCache["name"].ColumnBinding.ColumnName,
		"the column binding must be deep-copied, not shared by pointer")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSchemaFingerprintTracksContent(t *testing.T) {
	mcA := NewMetadataCache()
	require.NoError(t, mcA.RegisterSchema("s", 1, forma.SchemaAttributeCache{
		"age": {AttributeID: 3, ValueType: forma.ValueTypeInteger},
	}))
	fpA, ok := mcA.SchemaFingerprint(1)
	require.True(t, ok)
	require.NotEmpty(t, fpA)

	// Equal content (fresh map) yields the same fingerprint.
	mcB := NewMetadataCache()
	require.NoError(t, mcB.RegisterSchema("s", 1, forma.SchemaAttributeCache{
		"age": {AttributeID: 3, ValueType: forma.ValueTypeInteger},
	}))
	fpB, _ := mcB.SchemaFingerprint(1)
	require.Equal(t, fpA, fpB)

	// Different content yields a different fingerprint.
	mcC := NewMetadataCache()
	require.NoError(t, mcC.RegisterSchema("s", 1, forma.SchemaAttributeCache{
		"age": {AttributeID: 3, ValueType: forma.ValueTypeBigInt},
	}))
	fpC, _ := mcC.SchemaFingerprint(1)
	require.NotEqual(t, fpA, fpC)

	_, ok = mcA.SchemaFingerprint(42)
	require.False(t, ok)
}
