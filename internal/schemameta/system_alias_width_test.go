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
)

var systemTimestampColumnList = []forma.MainColumn{
	forma.MainColumnCreatedAt, forma.MainColumnUpdatedAt, forma.MainColumnDeletedAt,
}

// #745: a smallint or integer cannot hold a system timestamp's epoch millis,
// whatever the encoding, so registration refuses the binding with the
// attribute, the column and what it may declare or bind to instead.
func TestValidateColumnBinding_RefusesNarrowSystemTimestampAlias(t *testing.T) {
	for _, vt := range []forma.ValueType{forma.ValueTypeSmallInt, forma.ValueTypeInteger} {
		for _, col := range systemTimestampColumnList {
			for _, enc := range []forma.MainColumnEncoding{"", forma.MainColumnEncodingDefault, forma.MainColumnEncodingUnixMs} {
				err := ValidateColumnBinding("stamp", bindingMeta(vt, col, enc))
				require.Error(t, err, "%s/%s/%s", vt, col, enc)
				require.ErrorIs(t, err, ErrNarrowSystemAlias)
				msg := err.Error()
				assert.Contains(t, msg, "attribute stamp (valueType "+string(vt)+") binds system column "+string(col))
				assert.Contains(t, msg, "declare it bigint, numeric, date or datetime")
				assert.Contains(t, msg, "bind it to a smallint, integer, bigint or double column")
			}
		}
	}
}

// The refusal is the timestamps' alone: the wider types still bind them, and
// ltbase_schema_id holds a smallint, which both integer widths can hold.
func TestValidateColumnBinding_AdmitsWideSystemAliases(t *testing.T) {
	d, u := forma.MainColumnEncodingDefault, forma.MainColumnEncodingUnixMs
	for _, col := range systemTimestampColumnList {
		for _, meta := range []forma.AttributeMetadata{
			bindingMeta(forma.ValueTypeBigInt, col, d),
			bindingMeta(forma.ValueTypeNumeric, col, d),
			bindingMeta(forma.ValueTypeDate, col, u),
			bindingMeta(forma.ValueTypeDateTime, col, d),
		} {
			assert.NoError(t, ValidateColumnBinding("stamp", meta), "%s/%s", meta.ValueType, col)
		}
	}
	for _, vt := range []forma.ValueType{forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt} {
		assert.NoError(t, ValidateColumnBinding("kind", bindingMeta(vt, forma.MainColumnSchemaID, d)), "%s", vt)
	}
	// A user bigint column is not a system column: the write funnel judges
	// the declared width there (checkStorageFit).
	assert.NoError(t, ValidateColumnBinding("n", bindingMeta(forma.ValueTypeInteger, forma.MainColumnBigint01, d)))
}

// Registration (the directory registry) refuses the alias with the schema
// and its attributes file named.
func TestDirectoryRegistry_RefusesNarrowSystemTimestampAlias(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "log.json"), map[string]any{
		"type": "object", "properties": map[string]any{"name": map[string]any{"type": "string"}},
	})
	writeJSONFile(t, filepath.Join(dir, "log_attributes.json"), map[string]any{
		"created": map[string]any{"attributeID": float64(4), "valueType": "integer",
			"column_binding": map[string]any{"col_name": "ltbase_created_at"}},
	})
	_, err := NewFileSchemaRegistryFromDirectory(dir)
	require.ErrorIs(t, err, ErrNarrowSystemAlias)
	for _, want := range []string{"log_attributes.json", "schema log", "attribute created (valueType integer)", "ltbase_created_at"} {
		assert.Contains(t, err.Error(), want)
	}
}

// DEC-PREPROD-COMPAT is no-legacy-by-default: a registered schema that
// already carries the alias is not grandfathered, so loading it fails the
// same way, before the generation gate is consulted.
func TestLoadMetadata_RefusesNarrowSystemTimestampAlias(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	dir := t.TempDir()
	rows := pgxmock.NewRows([]string{"schema_name", "schema_id"}).AddRow("visit", int16(3))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT schema_name, schema_id FROM ` + sanitizeIdentifier("reg"))).WillReturnRows(rows)
	writeJSONFile(t, filepath.Join(dir, "visit_attributes.json"), map[string]any{
		"touched": map[string]any{"attributeID": float64(2), "valueType": "smallint",
			"column_binding": map[string]any{"col_name": "ltbase_updated_at"}},
	})

	_, err = NewMetadataLoader(mock, "reg", dir).LoadMetadata(context.Background())
	require.ErrorIs(t, err, ErrNarrowSystemAlias)
	for _, want := range []string{"schema visit", "attribute touched (valueType smallint)", "ltbase_updated_at", "declare it bigint"} {
		assert.Contains(t, err.Error(), want)
	}
	require.NoError(t, mock.ExpectationsWereMet())
}

// A retired entry is a ledger, never read, so its binding is not judged
// (#342), this one included.
func TestValidateSchemaAttributeCache_RetiredNarrowSystemAliasNotJudged(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"created": {AttributeName: "created", AttributeID: 4, ValueType: forma.ValueTypeInteger, Retired: true,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnCreatedAt}},
	}
	require.NoError(t, validateSchemaAttributeCache("log", cache))
}

// A caller's own registry is no admission's: the guard refuses the alias at
// startup and on every later read, so the read decode never meets one.
func TestGuardReservedAttrNames_RefusesNarrowSystemTimestampAlias(t *testing.T) {
	narrow := forma.SchemaAttributeCache{
		"name":    {AttributeName: "name", AttributeID: 1, ValueType: forma.ValueTypeText},
		"created": {AttributeName: "created", AttributeID: 2, ValueType: forma.ValueTypeInteger, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnCreatedAt}},
	}
	_, err := GuardReservedAttrNames(&stubRegistry{cache: narrow})
	require.ErrorIs(t, err, ErrNarrowSystemAlias)
	assert.Contains(t, err.Error(), "failed to check the attributes of schema lead")
	assert.Contains(t, err.Error(), "attribute created (valueType integer) binds system column ltbase_created_at")

	wide := forma.SchemaAttributeCache{
		"created": {AttributeName: "created", AttributeID: 2, ValueType: forma.ValueTypeBigInt, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnCreatedAt}},
		"kind":    {AttributeName: "kind", AttributeID: 3, ValueType: forma.ValueTypeSmallInt, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnSchemaID}},
	}
	stub := &stubRegistry{cache: wide}
	guarded, err := GuardReservedAttrNames(stub)
	require.NoError(t, err)

	stub.cache = narrow
	_, _, err = guarded.GetSchemaAttributeCacheByName("lead")
	require.True(t, errors.Is(err, ErrNarrowSystemAlias), "by name: %v", err)
	assert.Contains(t, err.Error(), "schema lead served by the configured SchemaRegistry")
	_, _, err = guarded.GetSchemaAttributeCacheByID(7)
	require.True(t, errors.Is(err, ErrNarrowSystemAlias), "by id: %v", err)
	assert.Contains(t, err.Error(), "schema lead (id 7) served by the configured SchemaRegistry")
}
