package schemameta

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

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

// The attribute caches the file registry hands out are copies: a caller that
// mutates the ancestry of one changes neither what a later getter answers nor
// what the rebuild reads (GetSchemaMetadata).
func TestFileRegistryAttributeCacheCopiesArrayAncestors(t *testing.T) {
	registry, err := NewFileSchemaRegistryFromDirectory("../../cmd/server/schemas")
	require.NoError(t, err)
	const name = "propertyInterests.snapshot.title"

	id, byName, err := registry.GetSchemaAttributeCacheByName("lead")
	require.NoError(t, err)
	byName[name].ArrayAncestors[0] = "mutated by name"
	_, byID, err := registry.GetSchemaAttributeCacheByID(id)
	require.NoError(t, err)
	require.Equal(t, []string{"propertyInterests"}, byID[name].ArrayAncestors)
	byID[name].ArrayAncestors[0] = "mutated by id"

	_, again, err := registry.GetSchemaAttributeCacheByName("lead")
	require.NoError(t, err)
	require.Equal(t, []string{"propertyInterests"}, again[name].ArrayAncestors)
	internal, _, err := GetSchemaMetadata(registry, id)
	require.NoError(t, err)
	require.Equal(t, []string{"propertyInterests"}, internal[name].ArrayAncestors)
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
		// An ancestor the document does not describe leaves the answer open,
		// with the arrays above it.
		{name: "missing.x", want: []string{}},
		{name: "ref.x", want: []string{}},
		{name: "tags.x", want: []string{}},
		{name: "orders.missing.x", want: []string{"orders"}},
		{name: "orders.items.qty.deeper", want: []string{"orders", "orders.items"}},
	}
	for _, tc := range cases {
		got, ok := arrayAncestorsOf(tc.name, properties)
		require.Equal(t, tc.ok, ok, tc.name)
		require.Equal(t, tc.want, got.all, tc.name)
		require.Empty(t, got.unions, tc.name)
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

// A schema left without array ancestry is logged as a warning when one of
// its attributes is known to lie inside an array of objects, as when a
// member of one is moved behind another file's $ref: its rows are the ones
// that read differently by row evidence. A schema without one, like visit,
// is logged as information.
func TestFileRegistryLogsUnderivedSchemaByItsArrays(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600))
	}
	write("orders.json", `{"type":"object","properties":{
		"items":{"type":"array","items":{"type":"object","properties":{
			"sku":{"type":"string"},
			"snapshot":{"$ref":"elsewhere.json#/properties/snapshot"}}}}}}`)
	write("orders_attributes.json", `{
		"items.sku":{"attributeID":1,"valueType":"text"},
		"items.snapshot.title":{"attributeID":2,"valueType":"text"}}`)
	write("people.json", `{"type":"object","properties":{
		"name":{"type":"string"},
		"contact":{"$ref":"elsewhere.json#/properties/contact"}}}`)
	write("people_attributes.json", `{
		"name":{"attributeID":1,"valueType":"text"},
		"contact.phone":{"attributeID":2,"valueType":"text"}}`)

	core, logs := observer.New(zapcore.InfoLevel)
	t.Cleanup(zap.ReplaceGlobals(zap.New(core)))
	_, err := NewFileSchemaRegistryFromDirectory(dir)
	require.NoError(t, err)

	levels := make(map[string]zapcore.Level)
	for _, entry := range logs.FilterMessageSnippet("array ancestors not derived").All() {
		levels[entry.ContextMap()["schema"].(string)] = entry.Level
	}
	require.Equal(t, map[string]zapcore.Level{"orders": zapcore.WarnLevel, "people": zapcore.InfoLevel}, levels)
}

// TestFileRegistryDerivesAnObjectOrArrayUnionAsAnArray pins #696 review
// R3-F2: a property admitting an object or an array of objects through a
// branch is an array ancestor of its members, so the read places a member
// written in the array form back inside the array (one index for it) and
// one written in the object form in the object (no index). Without, the
// registry derived it an object, and the read put the array around each
// member. That holds for anyOf, oneOf, allOf and if/then/else, through a
// local $ref, beside the property's own properties or without any, and
// for elements that name no properties of their own, whose members the
// property's own properties name. A branch of a primitive array, a branch
// typed other than an array, and any branch of a property typed an object
// make no array of objects.
func TestFileRegistryDerivesAnObjectOrArrayUnionAsAnArray(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600))
	}
	write("unions.json", `{"type":"object","$defs":{"offices":{"type":"array","items":{"type":"object","properties":{"city":{"type":"string"}}}}},
	"properties":{
		"beside":{"properties":{"city":{"type":"string"}},"anyOf":[{"type":"object"},
			{"type":"array","items":{"type":"object","properties":{"city":{"type":"string"}}}}]},
		"within":{"oneOf":[{"type":"object","properties":{"city":{"type":"string"}}},{"$ref":"#/$defs/offices"}]},
		"open":{"properties":{"city":{"type":"string"}},"anyOf":[{"type":"object"},{"type":"array","items":{"type":"object"}}]},
		"cond":{"properties":{"city":{"type":"string"}},"if":{"type":"array"},
			"then":{"items":{"type":"object","properties":{"city":{"type":"string"}}}}},
		"always":{"allOf":[{"type":"array","items":{"type":"object","properties":{"city":{"type":"string"}}}}]},
		"plain":{"properties":{"city":{"type":"string"}},"anyOf":[{"type":"object"},{"type":"array","items":{"type":"string"}}]},
		"typed":{"type":"object","properties":{"city":{"type":"string"}},
			"anyOf":[{"type":"array","items":{"type":"object","properties":{"city":{"type":"string"}}}}]},
		"objectBranch":{"properties":{"city":{"type":"string"}},
			"anyOf":[{"type":"object","items":{"type":"object","properties":{"city":{"type":"string"}}}}]},
		"typedArray":{"type":"array","anyOf":[{"items":{"type":"object","properties":{"city":{"type":"string"}}}}]}}}`)
	write("unions_attributes.json", `{
		"beside.city":{"attributeID":1,"valueType":"text"},"within.city":{"attributeID":2,"valueType":"text"},
		"open.city":{"attributeID":3,"valueType":"text"},"cond.city":{"attributeID":4,"valueType":"text"},
		"always.city":{"attributeID":5,"valueType":"text"},"plain.city":{"attributeID":6,"valueType":"text"},
		"typed.city":{"attributeID":7,"valueType":"text"},"objectBranch.city":{"attributeID":8,"valueType":"text"},
		"typedArray.city":{"attributeID":9,"valueType":"text"}}`)

	registry, err := NewFileSchemaRegistryFromDirectory(dir)
	require.NoError(t, err)
	_, cache, err := registry.GetSchemaAttributeCacheByName("unions")
	require.NoError(t, err)
	_, schema, err := registry.GetSchemaByName("unions")
	require.NoError(t, err)
	for _, union := range []string{"beside", "within", "open", "cond", "always"} {
		require.Equal(t, []string{union}, cache[union+".city"].ArrayAncestors, union)
		// R4-F1, R4-F2: the array form is a union's, apart from the arrays
		// the schema declares, and the property keeps no Items.
		require.Equal(t, []string{union}, cache[union+".city"].UnionArrayAncestors, union)
		require.Nil(t, schema.Properties[union].Items, union)
		require.NotNil(t, schema.Properties[union].BranchItems, union)
	}
	for _, object := range []string{"plain", "typed", "objectBranch"} {
		require.Equal(t, []string{}, cache[object+".city"].ArrayAncestors, "%s admits no array of objects", object)
		require.Empty(t, cache[object+".city"].UnionArrayAncestors, object)
	}
	// A property typed an array admits no object form: its branch's
	// elements are its Items, and it is a declared array, not a union.
	require.Equal(t, []string{"typedArray"}, cache["typedArray.city"].ArrayAncestors)
	require.Empty(t, cache["typedArray.city"].UnionArrayAncestors)
	require.NotNil(t, schema.Properties["typedArray"].Items)
}
