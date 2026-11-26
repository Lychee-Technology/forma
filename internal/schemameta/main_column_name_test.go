package schemameta

import (
	"context"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

// #470: the logical attribute namespace and the physical entity_main column
// namespace must not overlap. These tests cover the admission paths; the rule
// itself, per column family and spelling, is pinned in sqlgen
// (main_column_names_test.go).

// writeMainColumnNameSchema writes a "lead" schema whose attributes file holds
// attrs, beside a document declaring each as a string property.
func writeMainColumnNameSchema(t *testing.T, dir string, attrs map[string]any) {
	t.Helper()
	props := make(map[string]any, len(attrs))
	for name := range attrs {
		props[name] = map[string]any{"type": "string"}
	}
	writeJSONFile(t, filepath.Join(dir, "lead.json"), map[string]any{"type": "object", "properties": props})
	writeJSONFile(t, filepath.Join(dir, "lead_attributes.json"), attrs)
}

var mainColumnNameCases = []struct {
	name   string
	attrs  map[string]any
	column string
}{
	{"eav attribute named text_01", map[string]any{
		"text_01": map[string]any{"attributeID": float64(1), "valueType": "text"},
	}, "text_01"},
	{"integer_02 bound to integer_03", map[string]any{
		"integer_02": map[string]any{"attributeID": float64(1), "valueType": "integer",
			"column_binding": map[string]any{"col_name": "integer_03"}},
	}, "integer_02"},
	{"case variant of uuid_01", map[string]any{
		"UUID_01": map[string]any{"attributeID": float64(1), "valueType": "uuid"},
	}, "uuid_01"},
}

func TestValidateSchemaAttributeCache_RefusesEntityMainColumnName(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"text_02": {AttributeName: "text_02", AttributeID: 4, ValueType: forma.ValueTypeText},
	}
	for _, checkBindings := range []bool{true, false} {
		err := validateSchemaAttributeCacheOpts("lead", cache, checkBindings)
		require.Error(t, err, "checkBindings=%v", checkBindings)
		assert.Contains(t, err.Error(), "schema lead")
		assert.Contains(t, err.Error(), `attribute "text_02" is named after entity_main column "text_02"`)
	}
}

func TestRegisterSchema_RefusesEntityMainColumnName(t *testing.T) {
	mc := NewMetadataCache()
	err := mc.RegisterSchema("lead", 7, forma.SchemaAttributeCache{
		"double_01": {AttributeName: "double_01", AttributeID: 4, ValueType: forma.ValueTypeNumeric},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "register schema id 7")
	assert.Contains(t, err.Error(), `entity_main column "double_01"`)
	_, registered := mc.GetSchemaCache("lead")
	assert.False(t, registered, "a refused schema must not be registered")
}

// The file registry admits every schema through admitSchema, from the
// directory and from the registry table alike; a stored schema that breaks
// the rule fails registry construction, with no fallback.
func TestDirectoryRegistry_RefusesEntityMainColumnName(t *testing.T) {
	for _, tc := range mainColumnNameCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeMainColumnNameSchema(t, dir, tc.attrs)
			registry, err := NewFileSchemaRegistryFromDirectory(dir)
			require.Error(t, err)
			assert.Nil(t, registry)
			assert.Contains(t, err.Error(), "lead_attributes.json")
			assert.Contains(t, err.Error(), `entity_main column "`+tc.column+`"`)
		})
	}
}

// A correctly named attribute keeps its physical binding.
func TestDirectoryRegistry_AdmitsAttributeBoundToEntityMainColumn(t *testing.T) {
	dir := t.TempDir()
	writeMainColumnNameSchema(t, dir, map[string]any{
		"status": map[string]any{"attributeID": float64(1), "valueType": "text",
			"column_binding": map[string]any{"col_name": "text_01"}},
		"score": map[string]any{"attributeID": float64(2), "valueType": "integer",
			"column_binding": map[string]any{"col_name": "integer_03"}},
	})
	registry, err := NewFileSchemaRegistryFromDirectory(dir)
	require.NoError(t, err)
	_, cache, err := registry.GetSchemaAttributeCacheByName("lead")
	require.NoError(t, err)
	require.NotNil(t, cache["status"].ColumnBinding)
	assert.Equal(t, forma.MainColumnText01, cache["status"].ColumnBinding.ColumnName)
	require.NotNil(t, cache["score"].ColumnBinding)
	assert.Equal(t, forma.MainColumnInteger03, cache["score"].ColumnBinding.ColumnName)
}

// The metadata loader refuses the schema on the runtime path and under
// DeferColumnBindingCheck (validate-schema-consistency): deferral postpones
// the binding checks only, never the name rule.
func TestLoadMetadata_RefusesEntityMainColumnNameEvenWhenDeferred(t *testing.T) {
	registryQuery := regexp.QuoteMeta(`SELECT schema_name, schema_id FROM ` + sanitizeIdentifier("reg"))
	for _, tc := range mainColumnNameCases {
		for _, deferred := range []bool{false, true} {
			dir := t.TempDir()
			writeJSONFile(t, filepath.Join(dir, "lead_attributes.json"), tc.attrs)
			pool, err := pgxmock.NewPool()
			require.NoError(t, err)
			pool.ExpectQuery(registryQuery).
				WillReturnRows(pgxmock.NewRows([]string{"schema_name", "schema_id"}).AddRow("lead", int16(7)))

			loader := NewMetadataLoader(pool, "reg", dir)
			if deferred {
				loader = loader.DeferColumnBindingCheck()
			}
			cache, err := loader.LoadMetadata(context.Background())
			require.Error(t, err, "%s, deferred=%v", tc.name, deferred)
			assert.Nil(t, cache)
			assert.Contains(t, err.Error(), "lead_attributes.json")
			assert.Contains(t, err.Error(), `entity_main column "`+tc.column+`"`)
			pool.Close()
		}
	}
}

// A retired entry named after a column is a ledger entry no condition, sort
// or projection reaches, so it does not fail the load (#549's rule for
// reserved names), and it stays out of the active cache.
func TestDirectoryRegistry_RetiredEntityMainColumnNameLoads(t *testing.T) {
	dir := t.TempDir()
	writeMainColumnNameSchema(t, dir, map[string]any{
		"name":    map[string]any{"attributeID": float64(1), "valueType": "text"},
		"text_04": map[string]any{"attributeID": float64(2), "valueType": "text", "retired": true},
	})
	registry, err := NewFileSchemaRegistryFromDirectory(dir)
	require.NoError(t, err)
	_, cache, err := registry.GetSchemaAttributeCacheByName("lead")
	require.NoError(t, err)
	assert.NotContains(t, cache, "text_04")
	assert.Contains(t, cache, "name")
}
