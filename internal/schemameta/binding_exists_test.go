package schemameta

import (
	"context"
	"errors"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// #557: a binding names a column entity_main does not have. ColumnType()
// classifies "foo" and "text_99" as text, so the #459 matrix alone admits
// them and the mistake used to surface only on the first write.
func TestValidateColumnBinding_RefusesUnknownColumn(t *testing.T) {
	d := forma.MainColumnEncodingDefault
	cases := []struct {
		name string
		meta forma.AttributeMetadata
	}{
		{"unknown name", bindingMeta(forma.ValueTypeText, "foo", d)},
		{"typo inside a family", bindingMeta(forma.ValueTypeText, "text_99", d)},
		{"wrong case", bindingMeta(forma.ValueTypeText, "TEXT_01", d)},
		{"unknown name, uuid valueType", bindingMeta(forma.ValueTypeUUID, "uuid_9", d)},
		// Declared as forma.MainColumnBigint04, but the writer's allowlist and
		// the read projection (internal/model) stop at bigint_03: every write
		// to it fails with "unsupported column", so registration refuses it.
		{"declared constant outside the runtime set", bindingMeta(forma.ValueTypeBigInt, forma.MainColumnBigint04, d)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateColumnBinding("leadId", tc.meta)
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrUnknownMainColumn), "%v", err)
			col := string(tc.meta.ColumnBinding.ColumnName)
			assert.Contains(t, err.Error(), "attribute leadId (valueType "+string(tc.meta.ValueType)+")")
			assert.Contains(t, err.Error(), "unknown main column "+col)
			// The admitted set is spelled out: first and last data column, plus
			// a system alias.
			for _, want := range []string{"text_01", "uuid_02", "ltbase_row_id", "col_name must be one of"} {
				assert.Contains(t, err.Error(), want)
			}
		})
	}
}

// Every column the runtime writes, projects and scans (model.
// EntityMainColumnDescriptors) is admitted with a valueType of its own kind:
// the existence check and the round-trip matrix agree on the whole set.
func TestValidateColumnBinding_AdmitsEveryRuntimeColumn(t *testing.T) {
	for _, desc := range model.EntityMainColumnDescriptors {
		meta := bindingMeta(model.ColumnKindToValueType(desc.Kind), forma.MainColumn(desc.Name), forma.MainColumnEncodingDefault)
		assert.NoError(t, ValidateColumnBinding("a", meta), "column %s", desc.Name)
	}
}

func TestValidateSchemaAttributeCache_RejectsUnknownColumn(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"leadId": {AttributeName: "leadId", AttributeID: 4, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: "text_99"}},
	}
	err := validateSchemaAttributeCache("log", cache)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnknownMainColumn), "%v", err)
	for _, want := range []string{"schema log", "leadId", "text_99"} {
		assert.Contains(t, err.Error(), want)
	}
}

// A retired entry is a ledger of what the column used to be, never a write
// destination, so its col_name is not checked either (#342).
func TestValidateSchemaAttributeCache_RetiredUnknownColumnNotJudged(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"old": {AttributeName: "old", AttributeID: 4, ValueType: forma.ValueTypeText, Retired: true,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: "text_99"}},
	}
	require.NoError(t, validateSchemaAttributeCache("log", cache))
}

func TestRegisterSchema_RejectsUnknownColumn(t *testing.T) {
	mc := NewMetadataCache()
	err := mc.RegisterSchema("log", 7, forma.SchemaAttributeCache{
		"leadId": {AttributeName: "leadId", AttributeID: 4, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: "foo"}},
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnknownMainColumn), "%v", err)
	assert.Contains(t, err.Error(), "register schema id 7")
	_, registered := mc.GetSchemaCache("log")
	assert.False(t, registered, "a refused schema must not be registered")
}

func TestDirectoryRegistry_RejectsUnknownColumn(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "log.json"), map[string]any{
		"type": "object", "properties": map[string]any{"leadId": map[string]any{"type": "string"}},
	})
	writeJSONFile(t, filepath.Join(dir, "log_attributes.json"), map[string]any{
		"leadId": map[string]any{"attributeID": float64(4), "valueType": "text",
			"column_binding": map[string]any{"col_name": "foo"}},
	})
	_, err := NewFileSchemaRegistryFromDirectory(dir)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnknownMainColumn), "%v", err)
	assert.Contains(t, err.Error(), "log_attributes.json")
	assert.Contains(t, err.Error(), "unknown main column foo")
}

// The runtime loader refuses the binding; validate-schema-consistency loads
// through DeferColumnBindingCheck and reports it itself.
func TestLoadMetadata_UnknownColumnRejectedUnlessDeferred(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "log_attributes.json"), map[string]any{
		"leadId": map[string]any{"attributeID": float64(4), "valueType": "text",
			"column_binding": map[string]any{"col_name": "text_99"}},
	})
	registryQuery := regexp.QuoteMeta(`SELECT schema_name, schema_id FROM ` + sanitizeIdentifier("reg"))

	strict, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer strict.Close()
	strict.ExpectQuery(registryQuery).
		WillReturnRows(pgxmock.NewRows([]string{"schema_name", "schema_id"}).AddRow("log", int16(7)))
	_, err = NewMetadataLoader(strict, "reg", dir).LoadMetadata(ctx)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnknownMainColumn), "%v", err)
	assert.Contains(t, err.Error(), "text_99")

	lenient, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer lenient.Close()
	lenient.ExpectQuery(registryQuery).
		WillReturnRows(pgxmock.NewRows([]string{"schema_name", "schema_id"}).AddRow("log", int16(7)))
	cache, err := NewMetadataLoader(lenient, "reg", dir).DeferColumnBindingCheck().LoadMetadata(ctx)
	require.NoError(t, err)
	schemaCache, ok := cache.GetSchemaCache("log")
	require.True(t, ok)
	assert.Contains(t, schemaCache, "leadId")
}
