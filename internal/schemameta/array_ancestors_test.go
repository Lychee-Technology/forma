package schemameta

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

// The file registry derives each attribute's array ancestors from the schema
// document (#623). The shipped lead nests an object inside an array of
// objects (propertyInterests[].snapshot), which is what the rebuild needs
// them for; visit reaches its contact through another file's $ref, which the
// document parser does not resolve, so none of visit's are derived.
func TestFileRegistryDerivesArrayAncestors(t *testing.T) {
	registry, err := NewFileSchemaRegistryFromDirectory("../../cmd/server/schemas")
	require.NoError(t, err)

	_, lead, err := registry.GetSchemaAttributeCacheByName("lead")
	require.NoError(t, err)
	for name, want := range map[string][]string{
		"stage":                            {},
		"contact.phones":                   {},
		"requirement.areas.city":           {"requirement.areas"},
		"propertyInterests.propertyId":     {"propertyInterests"},
		"propertyInterests.snapshot.title": {"propertyInterests"},
	} {
		require.Equal(t, want, lead[name].ArrayAncestors, name)
	}
	for name, meta := range lead {
		require.NotNil(t, meta.ArrayAncestors, "every lead attribute is derived; %s is not", name)
	}

	_, visit, err := registry.GetSchemaAttributeCacheByName("visit")
	require.NoError(t, err)
	for name, meta := range visit {
		require.Nil(t, meta.ArrayAncestors, "visit's contact is behind another file's $ref; %s must not be derived", name)
	}
}

func TestArrayAncestorsOf(t *testing.T) {
	str := &forma.PropertySchema{Type: "string"}
	object := func(props map[string]*forma.PropertySchema) *forma.PropertySchema {
		return &forma.PropertySchema{Type: "object", Properties: props}
	}
	arrayOf := func(items *forma.PropertySchema) *forma.PropertySchema {
		return &forma.PropertySchema{Type: "array", Items: items}
	}
	properties := map[string]*forma.PropertySchema{
		"orders": arrayOf(object(map[string]*forma.PropertySchema{
			"items": arrayOf(object(map[string]*forma.PropertySchema{"qty": str})),
			"dims":  object(map[string]*forma.PropertySchema{"w": str}),
		})),
		// A union type is parsed without its "type", but its properties and
		// items are kept.
		"nullable": {Properties: map[string]*forma.PropertySchema{
			"rows": {Items: object(map[string]*forma.PropertySchema{"x": str})},
		}},
		"tags":    arrayOf(str),
		"ref":     {},
		"contact": object(map[string]*forma.PropertySchema{"phones": arrayOf(str)}),
	}

	cases := []struct {
		name string
		want []string
		ok   bool
	}{
		{name: "stage", want: []string{}, ok: true},
		{name: "contact.phones", want: []string{}, ok: true},
		{name: "orders.dims.w", want: []string{"orders"}, ok: true},
		{name: "orders.items.qty", want: []string{"orders", "orders.items"}, ok: true},
		{name: "nullable.rows.x", want: []string{"nullable.rows"}, ok: true},
		// An ancestor the document does not describe leaves the answer open.
		{name: "missing.x"},
		{name: "ref.x"},
		{name: "tags.x"},
		{name: "orders.items.qty.deeper"},
	}
	for _, tc := range cases {
		got, ok := arrayAncestorsOf(tc.name, properties)
		require.Equal(t, tc.ok, ok, tc.name)
		require.Equal(t, tc.want, got, tc.name)
	}
}

// One attribute the document does not resolve leaves every attribute of the
// schema underived, so no row mixes derived ancestors with inferred ones.
func TestFileRegistryDerivesAllOrNone(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600))
	}
	write("orders.json", `{"type":"object","properties":{
		"items":{"type":"array","items":{"type":"object","properties":{"sku":{"type":"string"}}}},
		"other":{"$ref":"elsewhere.json#/properties/other"}}}`)
	write("orders_attributes.json", `{
		"items.sku":{"attributeID":1,"valueType":"text"},
		"other.x":{"attributeID":2,"valueType":"text"}}`)

	registry, err := NewFileSchemaRegistryFromDirectory(dir)
	require.NoError(t, err)
	_, cache, err := registry.GetSchemaAttributeCacheByName("orders")
	require.NoError(t, err)
	require.Nil(t, cache["items.sku"].ArrayAncestors)
	require.Nil(t, cache["other.x"].ArrayAncestors)
}
