package schemameta

import (
	"context"
	"path/filepath"
	"regexp"
	"testing"

	forma "github.com/lychee-technology/forma"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateSchemaAttributeCache_RetiredIDReuseRejected(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"old_col":  {AttributeName: "old_col", AttributeID: 3, ValueType: forma.ValueTypeText, Retired: true},
		"nickname": {AttributeName: "nickname", AttributeID: 3, ValueType: forma.ValueTypeText},
	}
	err := validateSchemaAttributeCache("user", cache)
	require.NotNil(t, err, "expected reuse error for retired attribute id 3")

	errStr := err.Error()
	for _, want := range []string{"attribute id 3", "retired", "old_col", "text", "nickname"} {
		assert.Contains(t, errStr, want, "error does not name %q", want)
	}
}

func TestValidateSchemaAttributeCache_RetiredColumnBindingReuseRejected(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"old_col": {AttributeName: "old_col", AttributeID: 3, ValueType: forma.ValueTypeText, Retired: true,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: "text_01"}},
		"nickname": {AttributeName: "nickname", AttributeID: 4, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: "text_01"}},
	}
	err := validateSchemaAttributeCache("user", cache)
	require.NotNil(t, err, "expected reuse error for retired column binding text_01")

	errStr := err.Error()
	for _, want := range []string{"text_01", "retired", "old_col", "nickname"} {
		assert.Contains(t, errStr, want, "error does not name %q", want)
	}
}

func TestValidateSchemaAttributeCache_DistinctRetiredIDStillValid(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"old_col":  {AttributeName: "old_col", AttributeID: 3, ValueType: forma.ValueTypeText, Retired: true},
		"nickname": {AttributeName: "nickname", AttributeID: 4, ValueType: forma.ValueTypeText},
	}
	err := validateSchemaAttributeCache("user", cache)
	require.NoError(t, err, "distinct ids must validate")
}

// ---------------------------------------------------------------------------
// Active-cache stripping (#342 Task 3): retired entries are a validation-only
// ledger; no stored or registered cache may expose them.
// ---------------------------------------------------------------------------

func TestDirectoryRegistry_RetiredExcludedFromActiveCache(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "user.json"), map[string]any{
		"type":       "object",
		"properties": map[string]any{"name": map[string]any{"type": "string"}},
	})
	writeJSONFile(t, filepath.Join(dir, "user_attributes.json"), map[string]any{
		"name":    map[string]any{"attributeID": float64(1), "valueType": "text"},
		"old_col": map[string]any{"attributeID": float64(3), "valueType": "text", "retired": true},
	})

	registry, err := NewFileSchemaRegistryFromDirectory(dir)
	require.NoError(t, err, "load directory registry")

	schemaID, cache, err := registry.GetSchemaAttributeCacheByName("user")
	require.NoError(t, err, "get cache")
	assert.NotContains(t, cache, "old_col", "retired attribute old_col must not appear in the active cache")
	assert.Contains(t, cache, "name", "active attribute name missing")

	// The id index must not resolve the retired id either — this is what
	// keeps the #294 read path skipping attrID 3.
	byIDName, byIDCache, err := registry.GetSchemaAttributeCacheByID(schemaID)
	require.NoError(t, err, "get cache by id")
	assert.Equal(t, "user", byIDName)
	assert.NotContains(t, byIDCache, "old_col", "retired attribute must not appear in the by-id cache")

	_, idToName, err := GetSchemaMetadata(registry, schemaID)
	require.NoError(t, err, "GetSchemaMetadata")
	assert.NotContains(t, idToName, int16(3), "retired attribute id 3 must not be resolvable")
	assert.Equal(t, "name", idToName[int16(1)])
}

func TestDirectoryRegistry_RetiredIDReuseFailsLoad(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "user.json"), map[string]any{
		"type":       "object",
		"properties": map[string]any{"nickname": map[string]any{"type": "string"}},
	})
	writeJSONFile(t, filepath.Join(dir, "user_attributes.json"), map[string]any{
		"nickname": map[string]any{"attributeID": float64(3), "valueType": "text"},
		"old_col":  map[string]any{"attributeID": float64(3), "valueType": "text", "retired": true},
	})

	_, err := NewFileSchemaRegistryFromDirectory(dir)
	require.Error(t, err, "expected load failure for retired id reuse")
	assert.Contains(t, err.Error(), "retired attribute old_col", "error does not carry the reuse diagnosis")
}

// TestDirectoryRegistry_RetiredReservedFoldedColumnStillRejected pins the
// ordering invariant: the strip must run strictly AFTER
// validateSchemaAttributeCache, so retired entries still reach
// sqlgen.ValidateParquetAttrColumns. A retired attribute's folded column is
// still present in already-flushed parquet files (#342, #260).
func TestDirectoryRegistry_RetiredReservedFoldedColumnStillRejected(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "rowid.json"), map[string]any{"type": "object"})
	writeJSONFile(t, filepath.Join(dir, "rowid_attributes.json"), map[string]any{
		"row.id": map[string]any{"attributeID": float64(1), "valueType": "text", "retired": true},
	})

	_, err := NewFileSchemaRegistryFromDirectory(dir)
	require.Error(t, err, "retired attribute folding onto a reserved column must still be rejected")
	assert.Contains(t, err.Error(), "row.id")
	assert.Contains(t, err.Error(), "reserved")
}

func TestMetadataCacheRegisterSchema_StripsRetired(t *testing.T) {
	mc := NewMetadataCache()
	err := mc.RegisterSchema("user", 100, forma.SchemaAttributeCache{
		"name":    {AttributeName: "name", AttributeID: 1, ValueType: forma.ValueTypeText},
		"old_col": {AttributeName: "old_col", AttributeID: 3, ValueType: forma.ValueTypeText, Retired: true},
	})
	require.NoError(t, err, "RegisterSchema")

	cache, ok := mc.GetSchemaCacheByID(100)
	require.True(t, ok, "GetSchemaCacheByID")
	assert.NotContains(t, cache, "old_col", "retired attribute must be stripped on RegisterSchema")
	assert.Contains(t, cache, "name")
	assert.NotContains(t, mc.attributeMetadata[100], "old_col", "retired attribute must be stripped from attributeMetadata")
}

// TestMetadataCacheRegisterSchema_RejectsRetiredIDReuse pins that the strip
// never swallows a collision: RegisterSchema validates the FULL cache before
// stripping, so a retired/active id clash is an error rather than a silent
// drop that would rebind the retired id to the active attribute (#342).
func TestMetadataCacheRegisterSchema_RejectsRetiredIDReuse(t *testing.T) {
	mc := NewMetadataCache()
	err := mc.RegisterSchema("user", 100, forma.SchemaAttributeCache{
		"nickname": {AttributeName: "nickname", AttributeID: 3, ValueType: forma.ValueTypeText},
		"old_col":  {AttributeName: "old_col", AttributeID: 3, ValueType: forma.ValueTypeText, Retired: true},
	})
	require.Error(t, err, "retired id reuse must not register silently")
	assert.Contains(t, err.Error(), "retired attribute old_col", "error does not carry the reuse diagnosis")

	_, ok := mc.GetSchemaCacheByID(100)
	assert.False(t, ok, "a rejected schema must not be registered")
	assert.NotContains(t, mc.schemaNameToID, "user", "a rejected schema must not appear in the name index")
}

// TestLoadMetadata_StripsRetiredFromFileCache covers the loader's file path
// (loadAttributeMetadataFromFiles), the fourth storage point.
func TestLoadMetadata_StripsRetiredFromFileCache(t *testing.T) {
	ctx := context.Background()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	dir := t.TempDir()
	rows := pgxmock.NewRows([]string{"schema_name", "schema_id"}).AddRow("user", int16(1))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT schema_name, schema_id FROM ` + sanitizeIdentifier("test_registry"))).WillReturnRows(rows)

	writeJSONFile(t, filepath.Join(dir, "user_attributes.json"), map[string]any{
		"name":    map[string]any{"attributeID": float64(1), "valueType": "text"},
		"old_col": map[string]any{"attributeID": float64(3), "valueType": "text", "retired": true},
	})

	loader := NewMetadataLoader(mock, "test_registry", dir)
	cache, err := loader.LoadMetadata(ctx)
	require.NoError(t, err)

	assert.NotContains(t, cache.attributeMetadata[int16(1)], "old_col", "retired attribute must not reach attributeMetadata")
	assert.Contains(t, cache.attributeMetadata[int16(1)], "name")

	schemaCache, ok := cache.GetSchemaCacheByID(1)
	require.True(t, ok)
	assert.NotContains(t, schemaCache, "old_col", "retired attribute must not reach schemaCaches")
	assert.Contains(t, schemaCache, "name")

	require.NoError(t, mock.ExpectationsWereMet())
}
