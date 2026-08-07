package schemameta

import (
	"testing"

	forma "github.com/lychee-technology/forma"
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
