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

// TestParquetUnrepresentableReasonIgnoresObjectOrArrayUnions pins #696
// review R4-F1: an ancestor declared as an object or an array of objects
// does not make the schema unrepresentable, since a row writing it as an
// object holds one value per row; an array the schema declares does, also
// above or below a union. UnionArrayFormCondition answers the records only
// the union's array form stores: any index of a scalar, and of a list an
// element's second index or the empty-list marker's index.
func TestParquetUnrepresentableReasonIgnoresObjectOrArrayUnions(t *testing.T) {
	union := forma.SchemaAttributeCache{
		"note":           {AttributeID: 1, ValueType: forma.ValueTypeText, ArrayAncestors: []string{}},
		"office.city":    {AttributeID: 2, ValueType: forma.ValueTypeText, ArrayAncestors: []string{"office"}, UnionArrayAncestors: []string{"office"}},
		"office.tags":    {AttributeID: 3, ValueType: forma.ValueTypeList, ArrayAncestors: []string{"office"}, UnionArrayAncestors: []string{"office"}},
		"office.in.city": {AttributeID: 4, ValueType: forma.ValueTypeText, ArrayAncestors: []string{"office", "office.in"}, UnionArrayAncestors: []string{"office", "office.in"}},
	}
	require.Empty(t, ParquetUnrepresentableReason(union))
	require.NoError(t, ValidateParquetRepresentable(union))

	union["office.lines.sku"] = forma.AttributeMetadata{AttributeID: 5, ValueType: forma.ValueTypeText,
		ArrayAncestors: []string{"office", "office.lines"}, UnionArrayAncestors: []string{"office"}}
	require.Contains(t, ParquetUnrepresentableReason(union), `attribute "office.lines.sku" lies inside the array of objects "office.lines"`)

	require.Equal(t, "attr_id = 2 AND array_indices <> ''", UnionArrayFormCondition(union["office.city"]))
	require.Equal(t, "attr_id = 3 AND array_indices <> '' AND (array_indices LIKE '%,%' OR (value_text IS NULL AND value_numeric IS NULL))",
		UnionArrayFormCondition(union["office.tags"]))
	require.Empty(t, UnionArrayFormCondition(union["note"]))
	bound := union["office.city"]
	bound.ColumnBinding = &forma.MainColumnBinding{ColumnName: forma.MainColumnText01}
	require.Empty(t, UnionArrayFormCondition(bound), "the export reads a bound attribute from its column, not eav_data")
}
