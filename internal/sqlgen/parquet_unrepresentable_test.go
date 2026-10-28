package sqlgen

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// TestParquetUnrepresentableReasonNamesAnObjectArrayMember pins #704's test:
// an attribute with an array ancestor, a list among them, makes the schema
// unrepresentable, and the reason names the first such attribute in name
// order and its outermost array.
func TestParquetUnrepresentableReasonNamesAnObjectArrayMember(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"name":             {AttributeID: 1, ValueType: forma.ValueTypeText, ArrayAncestors: []string{}},
		"tags":             {AttributeID: 2, ValueType: forma.ValueTypeList, ArrayAncestors: []string{}},
		"orders.lines.qty": {AttributeID: 3, ValueType: forma.ValueTypeNumeric, ArrayAncestors: []string{"orders", "orders.lines"}},
		"orders.tags":      {AttributeID: 4, ValueType: forma.ValueTypeList, ArrayAncestors: []string{"orders"}},
	}

	reason := ParquetUnrepresentableReason(cache)
	require.Contains(t, reason, `attribute "orders.lines.qty" lies inside the array of objects "orders"`)
	require.EqualError(t, ValidateParquetRepresentable(cache), reason)

	delete(cache, "orders.lines.qty")
	require.Contains(t, ParquetUnrepresentableReason(cache), `attribute "orders.tags" lies inside the array of objects "orders"`,
		"a list inside an array of objects cannot be held either")
}

// TestParquetUnrepresentableReasonAcceptsFlatAndUnderivedSchemas: lists and
// nested objects outside any array are representable, and a cache without
// derived ancestry (the metadata loader's, or an underived schema, #705)
// reports nothing.
func TestParquetUnrepresentableReasonAcceptsFlatAndUnderivedSchemas(t *testing.T) {
	for name, cache := range map[string]forma.SchemaAttributeCache{
		"derived": {
			"contact.name": {AttributeID: 1, ValueType: forma.ValueTypeText, ArrayAncestors: []string{}},
			"tags":         {AttributeID: 2, ValueType: forma.ValueTypeList, ArrayAncestors: []string{}},
		},
		"underived": {
			"items.sku": {AttributeID: 1, ValueType: forma.ValueTypeText},
		},
	} {
		require.Empty(t, ParquetUnrepresentableReason(cache), name)
		require.NoError(t, ValidateParquetRepresentable(cache), name)
	}
}
