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

// TestParquetUnrepresentableReasonAcceptsADerivedFlatSchema: lists and
// nested objects outside any array are representable once the registry
// derived their ancestry.
func TestParquetUnrepresentableReasonAcceptsADerivedFlatSchema(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"contact.name": {AttributeID: 1, ValueType: forma.ValueTypeText, ArrayAncestors: []string{}},
		"tags":         {AttributeID: 2, ValueType: forma.ValueTypeList, ArrayAncestors: []string{}},
	}
	require.Empty(t, ParquetUnrepresentableReason(cache))
	require.NoError(t, ValidateParquetRepresentable(cache))
}

// TestParquetUnrepresentableReasonRefusesAnUnderivedSchema pins #937: nil
// ancestry says nothing about arrays of objects, so a schema holding an
// attribute without it is refused, a flat one included. The reason names the
// attribute the registry could not place, not the first one in name order;
// a registry that marks none is answered with the first attribute without
// ancestry.
func TestParquetUnrepresentableReasonRefusesAnUnderivedSchema(t *testing.T) {
	marked := forma.SchemaAttributeCache{
		"items.sku":     {AttributeID: 1, ValueType: forma.ValueTypeText},
		"name":          {AttributeID: 2, ValueType: forma.ValueTypeText},
		"other.x":       {AttributeID: 3, ValueType: forma.ValueTypeText, ArrayAncestryUnresolved: true},
		"other.y.z":     {AttributeID: 4, ValueType: forma.ValueTypeText, ArrayAncestryUnresolved: true},
		"unmarked.list": {AttributeID: 5, ValueType: forma.ValueTypeList},
	}
	reason := ParquetUnrepresentableReason(marked)
	require.Contains(t, reason, `could not place attribute "other.x"`)
	require.Contains(t, reason, "(#937)")
	require.EqualError(t, ValidateParquetRepresentable(marked), reason)

	unmarked := forma.SchemaAttributeCache{
		"name":    {AttributeID: 1, ValueType: forma.ValueTypeText},
		"contact": {AttributeID: 2, ValueType: forma.ValueTypeText},
	}
	require.Contains(t, ParquetUnrepresentableReason(unmarked), `did not derive the array ancestry of attribute "contact"`)

	partial := forma.SchemaAttributeCache{
		"name":  {AttributeID: 1, ValueType: forma.ValueTypeText, ArrayAncestors: []string{}},
		"other": {AttributeID: 2, ValueType: forma.ValueTypeText},
	}
	require.Contains(t, ParquetUnrepresentableReason(partial), `attribute "other"`,
		"one attribute without ancestry is enough to refuse the schema")
}

// TestParquetUnrepresentableReasonPrefersAKnownObjectArrayMember: a cache
// that places one attribute inside an array of objects and leaves another
// underived is answered with the known #704 reason.
func TestParquetUnrepresentableReasonPrefersAKnownObjectArrayMember(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"a.x":       {AttributeID: 1, ValueType: forma.ValueTypeText, ArrayAncestryUnresolved: true},
		"items.sku": {AttributeID: 2, ValueType: forma.ValueTypeText, ArrayAncestors: []string{"items"}},
	}
	require.Contains(t, ParquetUnrepresentableReason(cache), `attribute "items.sku" lies inside the array of objects "items"`)
}

// TestParquetUnrepresentableReasonAcceptsAnEmptySchema: a schema without
// attributes has nothing a parquet row could collapse.
func TestParquetUnrepresentableReasonAcceptsAnEmptySchema(t *testing.T) {
	require.Empty(t, ParquetUnrepresentableReason(forma.SchemaAttributeCache{}))
}
