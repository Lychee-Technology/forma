package schemameta

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

func bindingMeta(vt forma.ValueType, col forma.MainColumn, enc forma.MainColumnEncoding) forma.AttributeMetadata {
	return forma.AttributeMetadata{AttributeID: 1, ValueType: vt,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: col, Encoding: enc}}
}

// #459: the registration matrix admits exactly the (valueType, column type,
// encoding) triples the write path, the Postgres read path and the CDC export
// round-trip. The DuckDB hot-leg projection of date/datetime → text (rfc3339)
// is unverified; see #555.
func TestValidateColumnBinding_Matrix(t *testing.T) {
	d, u, i, b, bt := forma.MainColumnEncodingDefault, forma.MainColumnEncodingUnixMs,
		forma.MainColumnEncodingRFC3339, forma.MainColumnEncodingBoolInt, forma.MainColumnEncodingBoolText
	ok := []forma.AttributeMetadata{
		bindingMeta(forma.ValueTypeText, forma.MainColumnText01, d),
		bindingMeta(forma.ValueTypeText, forma.MainColumnText01, ""), // empty encoding == default
		bindingMeta(forma.ValueTypeUUID, forma.MainColumnUUID01, d),
		bindingMeta(forma.ValueTypeSmallInt, forma.MainColumnSmallint01, d),
		bindingMeta(forma.ValueTypeInteger, forma.MainColumnInteger01, d),
		bindingMeta(forma.ValueTypeBigInt, forma.MainColumnBigint01, d),
		bindingMeta(forma.ValueTypeNumeric, forma.MainColumnDouble01, d),
		bindingMeta(forma.ValueTypeNumeric, forma.MainColumnSmallint01, d), // width enforced at write
		bindingMeta(forma.ValueTypeBigInt, forma.MainColumnInteger01, d),   // width enforced at write
		bindingMeta(forma.ValueTypeDate, forma.MainColumnBigint01, u),
		bindingMeta(forma.ValueTypeDate, forma.MainColumnBigint01, d),
		bindingMeta(forma.ValueTypeDateTime, forma.MainColumnText01, i),
		bindingMeta(forma.ValueTypeBool, forma.MainColumnSmallint01, b),
		bindingMeta(forma.ValueTypeBool, forma.MainColumnText01, bt),
		bindingMeta(forma.ValueTypeDate, forma.MainColumnCreatedAt, u),
		bindingMeta(forma.ValueTypeUUID, forma.MainColumnRowID, d),
		{AttributeID: 1, ValueType: forma.ValueTypeText}, // unbound
	}
	for _, m := range ok {
		assert.NoError(t, ValidateColumnBinding("a", m), "%+v", *bindingOrZero(m))
	}
	bad := []struct {
		meta forma.AttributeMetadata
		want string
	}{
		{bindingMeta(forma.ValueTypeText, forma.MainColumnUUID02, d), "cannot round-trip through main column uuid_02"},
		{bindingMeta(forma.ValueTypeText, forma.MainColumnSmallint01, d), "cannot round-trip through main column smallint_01"},
		{bindingMeta(forma.ValueTypeUUID, forma.MainColumnText01, d), "uuid columns"},
		{bindingMeta(forma.ValueTypeNumeric, forma.MainColumnText01, d), "smallint, integer, bigint or double columns"},
		{bindingMeta(forma.ValueTypeDate, forma.MainColumnText01, d), "rfc3339"},
		{bindingMeta(forma.ValueTypeDate, forma.MainColumnBigint01, i), "bigint columns"},
		{bindingMeta(forma.ValueTypeBool, forma.MainColumnSmallint01, d), "bool_smallint"},
		{bindingMeta(forma.ValueTypeBool, forma.MainColumnText01, b), "bool_text"},
		{bindingMeta(forma.ValueTypeList, forma.MainColumnText01, d), "list"},
		{bindingMeta(forma.ValueTypeText, forma.MainColumnText01, "base64"), "unknown encoding"},
		{bindingMeta(forma.ValueType("money"), forma.MainColumnDouble01, d), "unknown valueType"},
		{bindingMeta(forma.ValueTypeText, forma.MainColumnRowID, d), "cannot round-trip through main column ltbase_row_id"},
		{bindingMeta(forma.ValueTypeDateTime, forma.MainColumnText01, forma.MainColumnEncodingLegacyISO8601), "migrate-rfc3339"},
	}
	for _, tc := range bad {
		err := ValidateColumnBinding("leadId", tc.meta)
		require.Error(t, err, "%+v", *tc.meta.ColumnBinding)
		assert.Contains(t, err.Error(), "attribute leadId (valueType "+string(tc.meta.ValueType)+")")
		assert.Contains(t, err.Error(), tc.want)
	}
}

func bindingOrZero(m forma.AttributeMetadata) *forma.MainColumnBinding {
	if m.ColumnBinding == nil {
		return &forma.MainColumnBinding{}
	}
	return m.ColumnBinding
}

func TestValidateSchemaAttributeCache_RejectsIncompatibleBinding(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"leadId": {AttributeName: "leadId", AttributeID: 4, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnUUID02}},
	}
	err := validateSchemaAttributeCache("log", cache)
	require.Error(t, err)
	for _, want := range []string{"schema log", "leadId", "text", "uuid_02"} {
		assert.Contains(t, err.Error(), want)
	}
}

// Retired entries are an attributeID/column ledger, never a write
// destination, so their binding is not judged (#342).
func TestValidateSchemaAttributeCache_RetiredBindingNotJudged(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"old": {AttributeName: "old", AttributeID: 4, ValueType: forma.ValueTypeText, Retired: true,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnUUID02}},
	}
	require.NoError(t, validateSchemaAttributeCache("log", cache))
}

func TestDirectoryRegistry_RejectsIncompatibleBinding(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "log.json"), map[string]any{
		"type": "object", "properties": map[string]any{"leadId": map[string]any{"type": "string"}},
	})
	writeJSONFile(t, filepath.Join(dir, "log_attributes.json"), map[string]any{
		"leadId": map[string]any{"attributeID": float64(4), "valueType": "text",
			"column_binding": map[string]any{"col_name": "uuid_02"}},
	})
	_, err := NewFileSchemaRegistryFromDirectory(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "log_attributes.json")
	assert.Contains(t, err.Error(), "uuid_02")
}

// TestValidateColumnBinding_RefusesLegacyISO8601 (#842): the renamed
// encoding has no alias. A declaration still naming iso8601 is refused,
// whatever its column, with the rename and the migration tool named, so
// startup stops until the schema is migrated.
func TestValidateColumnBinding_RefusesLegacyISO8601(t *testing.T) {
	for _, col := range []forma.MainColumn{forma.MainColumnText01, forma.MainColumnBigint01} {
		err := ValidateColumnBinding("due", bindingMeta(forma.ValueTypeDate, col, forma.MainColumnEncodingLegacyISO8601))
		require.Error(t, err)
		msg := err.Error()
		assert.Contains(t, msg, "attribute due (valueType date) binds main column "+string(col)+" with encoding iso8601")
		assert.Contains(t, msg, "renamed to rfc3339")
		assert.Contains(t, msg, "forma-tools migrate-rfc3339")
		assert.Contains(t, msg, "apply-schema")
	}
}
