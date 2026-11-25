package schemameta

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

func writeSchemaArtifacts(t *testing.T, dir, name, schema, attributes string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name+".json"), []byte(schema), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, name+"_attributes.json"), []byte(attributes), 0o600))
}

// The file registry marks a primitive array the document declares under an
// element-typed (pre-#204) ledger entry as DeclaredArray, so an update may
// still send its array (#696). A scalar the document declares a single
// value, a list, and a primitive array declared a list are not marked, and a
// member of an array of objects is reached through its array.
func TestFileRegistryDerivesDeclaredArrays(t *testing.T) {
	dir := t.TempDir()
	writeSchemaArtifacts(t, dir, "legacy",
		`{"type":"object","properties":{"note":{"type":"string"},"tags":{"type":"array","items":{"type":"string"}},`+
			`"list":{"type":"array","items":{"type":"string"}},`+
			`"items":{"type":"array","items":{"type":"object","properties":{"codes":{"type":"array","items":{"type":"integer"}},"name":{"type":"string"}}}}}}`,
		`{"note":{"attributeID":1,"valueType":"text"},"tags":{"attributeID":2,"valueType":"text"},`+
			`"list":{"attributeID":3,"valueType":"list","items_type":"text"},`+
			`"items.codes":{"attributeID":4,"valueType":"integer"},"items.name":{"attributeID":5,"valueType":"text"}}`)
	registry, err := NewFileSchemaRegistryFromDirectory(dir)
	require.NoError(t, err)
	_, cache, err := registry.GetSchemaAttributeCacheByName("legacy")
	require.NoError(t, err)
	for name, want := range map[string]bool{"note": false, "tags": true, "list": false, "items.codes": true, "items.name": false} {
		require.Equal(t, want, cache[name].DeclaredArray, name)
	}
	require.Equal(t, []string{"items"}, cache["items.codes"].ArrayAncestors)
}

// The registry also records, attribute by attribute, the array positions the
// raw document settles (SettledArrayAncestors), which the shape rule judges
// an update's indices by (#696 review R2-F1, R2-F2). An unrelated path the
// lenient reading cannot resolve leaves ArrayAncestors underived for the
// whole schema but no settled position unrecorded; an object declared only
// by "properties" beside an anyOf admitting an array is not settled though
// the lenient reading gives it no array; a list records none; and a
// primitive array declared through a nullable union the parser drops is a
// DeclaredArray (R2-F3).
func TestFileRegistryDerivesSettledArrayPositions(t *testing.T) {
	dir := t.TempDir()
	writeSchemaArtifacts(t, dir, "doc",
		`{"type":"object","properties":{"note":{"type":"string"},`+
			`"tags":{"type":["array","null"],"items":{"type":"string"}},`+
			`"list":{"type":"array","items":{"type":"string"}},`+
			`"items":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string"}}}},`+
			`"office":{"properties":{"city":{"type":"string"}},"anyOf":[{"type":"object"},`+
			`{"type":"array","items":{"type":"object","properties":{"city":{"type":"string"}}}}]},`+
			`"meta":{"anyOf":[{"type":"object","properties":{"note":{"type":"string"}}}]}}}`,
		`{"note":{"attributeID":1,"valueType":"text"},"tags":{"attributeID":2,"valueType":"text"},`+
			`"list":{"attributeID":3,"valueType":"list","items_type":"text"},"items.name":{"attributeID":4,"valueType":"text"},`+
			`"office.city":{"attributeID":5,"valueType":"text"},"meta.note":{"attributeID":6,"valueType":"text"}}`)
	registry, err := NewFileSchemaRegistryFromDirectory(dir)
	require.NoError(t, err)
	_, cache, err := registry.GetSchemaAttributeCacheByName("doc")
	require.NoError(t, err)
	for name, meta := range cache {
		require.Nil(t, meta.ArrayAncestors, "meta.note leaves %s's ancestry underived", name)
	}
	settled := map[string][]string{"note": {}, "tags": {}, "items.name": {"items"}, "list": nil, "office.city": nil, "meta.note": nil}
	for name, want := range settled {
		require.Equal(t, want, cache[name].SettledArrayAncestors, name)
	}
	require.True(t, cache["tags"].DeclaredArray)
	require.False(t, cache["note"].DeclaredArray)
}

// DeclaredArray is judged on the attribute's own node, not on whether its
// ancestors settle its positions (#696 review R3-F1): a nullable primitive
// array beneath an object declared only by "properties", or beside an anyOf
// admitting an array, is still declared, though its indices are unsettled.
// A true scalar there, and a node whose type admits both an array and an
// object, are not marked.
func TestFileRegistryDerivesDeclaredArraysUnderUnsettledParents(t *testing.T) {
	dir := t.TempDir()
	writeSchemaArtifacts(t, dir, "legacy",
		`{"type":"object","properties":{`+
			`"profile":{"properties":{"note":{"type":"string"},"tags":{"type":["array","null"],"items":{"type":"string"}},`+
			`"mixed":{"type":["array","object"],"items":{"type":"string"}}}},`+
			`"office":{"properties":{"codes":{"type":["array","null"],"items":{"type":"integer"}}},"anyOf":[{"type":"object"},`+
			`{"type":"array","items":{"type":"object","properties":{"codes":{"type":["array","null"],"items":{"type":"integer"}}}}}]}}}`,
		`{"profile.note":{"attributeID":1,"valueType":"text"},"profile.tags":{"attributeID":2,"valueType":"text"},`+
			`"profile.mixed":{"attributeID":3,"valueType":"text"},"office.codes":{"attributeID":4,"valueType":"integer"}}`)
	registry, err := NewFileSchemaRegistryFromDirectory(dir)
	require.NoError(t, err)
	_, cache, err := registry.GetSchemaAttributeCacheByName("legacy")
	require.NoError(t, err)
	for name, want := range map[string]bool{"profile.note": false, "profile.tags": true, "profile.mixed": false, "office.codes": true} {
		require.Equal(t, want, cache[name].DeclaredArray, name)
		require.Nil(t, cache[name].SettledArrayAncestors, "%s's parent does not settle its positions", name)
	}
}

// DeriveArrayPositions reads what the document fixes (#696). A node whose
// type is object or array of objects settles its members' positions, even
// beside an anyOf, since JSON Schema is conjunctive. A node declared only by
// "properties", whose anyOf admits an array of objects, does not: an index
// below it may belong to that array. Nor does a $ref to another file, a type
// admitting an object and an array, or a leaf without a type.
func TestDeriveArrayPositions(t *testing.T) {
	dir := t.TempDir()
	writeSchemaArtifacts(t, dir, "doc",
		`{"type":"object","$defs":{"addr":{"type":"object","properties":{"zip":{"type":"string"}}}},"properties":{`+
			`"note":{"type":["string","null"]},`+
			`"phones":{"type":"array","items":{"type":"string"}},`+
			`"areas":{"type":"array","items":{"type":"object","properties":{"city":{"type":"string"}}}},`+
			`"home":{"$ref":"#/$defs/addr"},`+
			`"fixed":{"type":"object","properties":{"city":{"type":"string"}},"anyOf":[{"type":"array"}]},`+
			`"office":{"properties":{"city":{"type":"string"}},"anyOf":[{"type":"object"},{"type":"array","items":{"type":"object","properties":{"city":{"type":"string"}}}}]},`+
			`"either":{"type":["object","array"],"properties":{"city":{"type":"string"}}},`+
			`"stop":{"$ref":"other.json#/$defs/stop"},`+
			`"loose":{"type":"object","properties":{"any":{}}}}}`,
		`{}`)
	names := []string{"note", "phones", "areas.city", "home.zip", "fixed.city", "office.city", "either.city", "stop.name", "loose.any", "missing"}
	cache := make(forma.SchemaAttributeCache, len(names))
	for _, name := range names {
		cache[name] = forma.AttributeMetadata{AttributeName: name, ValueType: forma.ValueTypeText}
	}
	positions, err := DeriveArrayPositions(dir, "doc", cache)
	require.NoError(t, err)

	verified := map[string]ArrayPositions{
		"note":       {Ancestors: []string{}, Verified: true},
		"phones":     {Ancestors: []string{}, DeclaredArray: true, Verified: true},
		"areas.city": {Ancestors: []string{"areas"}, Verified: true},
		"home.zip":   {Ancestors: []string{}, Verified: true},
		"fixed.city": {Ancestors: []string{}, Verified: true},
	}
	for name, want := range verified {
		require.Equal(t, want, positions[name], name)
	}
	require.Equal(t, 1, positions["phones"].MaxIndices())
	require.Equal(t, 1, positions["areas.city"].MaxIndices())
	for _, name := range []string{"office.city", "either.city", "stop.name", "loose.any", "missing"} {
		require.False(t, positions[name].Verified, name)
	}

	none, err := DeriveArrayPositions(dir, "absent", cache)
	require.NoError(t, err)
	require.False(t, none["note"].Verified, "a schema without a document settles nothing")
}

// Combinators are read as JSON Schema reads them (#696 review R4-F1): items
// typed only through an anyOf of single types make a primitive array, as do
// a nullable array given by an anyOf and a list's attribute beside it; items
// that may be objects, or an anyOf admitting an object at the leaf, do not.
// A leaf typed only through such an anyOf is a single value.
func TestFileRegistryDerivesDeclaredArraysThroughCombinators(t *testing.T) {
	dir := t.TempDir()
	writeSchemaArtifacts(t, dir, "legacy",
		`{"type":"object","properties":{`+
			`"tags":{"type":"array","items":{"anyOf":[{"type":"string"},{"type":"integer"}]}},`+
			`"codes":{"anyOf":[{"type":"array","items":{"oneOf":[{"type":"string"},{"type":"null"}]}},{"type":"null"}]},`+
			`"ranked":{"type":"array","allOf":[{"items":{"type":"integer"}}]},`+
			`"mixed":{"type":"array","items":{"anyOf":[{"type":"string"},{"type":"object"}]}},`+
			`"either":{"anyOf":[{"type":"object"},{"type":"array","items":{"type":"string"}}]},`+
			`"note":{"anyOf":[{"type":"string"},{"type":"integer"}]}}}`,
		`{"tags":{"attributeID":1,"valueType":"text"},"codes":{"attributeID":2,"valueType":"text"},`+
			`"ranked":{"attributeID":3,"valueType":"integer"},"mixed":{"attributeID":4,"valueType":"text"},`+
			`"either":{"attributeID":5,"valueType":"text"},"note":{"attributeID":6,"valueType":"text"}}`)
	registry, err := NewFileSchemaRegistryFromDirectory(dir)
	require.NoError(t, err)
	_, cache, err := registry.GetSchemaAttributeCacheByName("legacy")
	require.NoError(t, err)
	for name, want := range map[string]bool{"tags": true, "codes": true, "ranked": true, "mixed": false, "either": false, "note": false} {
		require.Equal(t, want, cache[name].DeclaredArray, name)
	}
	require.Equal(t, []string{}, cache["note"].SettledArrayAncestors, "an anyOf of single types settles a single value")
	require.Nil(t, cache["mixed"].SettledArrayAncestors)
}

// Where the document does not settle which ancestors are arrays it may still
// bound how many there are, reading every alternative (#696 review R4-F2):
// office, an object or one array of objects, bounds office.city at one index
// and its declared array office.codes at two; deep, an object or an array of
// arrays of objects, at two. An ancestor whose array leaves its items open, a
// member an alternative leaves undeclared and a $ref cycle bound nothing, nor
// does a list. A settled attribute is bounded as it is settled.
func TestFileRegistryBoundsArrayIndicesBesideCombinators(t *testing.T) {
	dir := t.TempDir()
	writeSchemaArtifacts(t, dir, "doc",
		`{"type":"object","$defs":{"tree":{"anyOf":[{"type":"object","properties":{"name":{"type":"string"}}},`+
			`{"type":"array","items":{"$ref":"#/$defs/tree"}}]}},"properties":{`+
			`"office":{"properties":{"city":{"type":"string"},"codes":{"type":["array","null"],"items":{"type":"integer"}}},`+
			`"anyOf":[{"type":"object"},{"type":"array","items":{"type":"object","properties":{"city":{"type":"string"},`+
			`"codes":{"type":["array","null"],"items":{"type":"integer"}}}}}]},`+
			`"deep":{"anyOf":[{"type":"object","properties":{"city":{"type":"string"}}},`+
			`{"type":"array","items":{"type":"array","items":{"type":"object","properties":{"city":{"type":"string"}}}}}]},`+
			`"open":{"properties":{"city":{"type":"string"}},"anyOf":[{"type":"object"},{"type":"array"}]},`+
			`"loose":{"anyOf":[{"type":"object","properties":{"city":{"type":"string"}}},{"type":"object"}]},`+
			`"tree":{"$ref":"#/$defs/tree"},`+
			`"areas":{"type":"array","items":{"type":"object","properties":{"city":{"type":"string"}}}},`+
			`"list":{"type":"array","items":{"type":"string"}}}}`,
		`{"office.city":{"attributeID":1,"valueType":"text"},"office.codes":{"attributeID":2,"valueType":"integer"},`+
			`"deep.city":{"attributeID":3,"valueType":"text"},"open.city":{"attributeID":4,"valueType":"text"},`+
			`"loose.city":{"attributeID":5,"valueType":"text"},"tree.name":{"attributeID":6,"valueType":"text"},`+
			`"areas.city":{"attributeID":7,"valueType":"text"},"list":{"attributeID":8,"valueType":"list","items_type":"text"}}`)
	registry, err := NewFileSchemaRegistryFromDirectory(dir)
	require.NoError(t, err)
	_, cache, err := registry.GetSchemaAttributeCacheByName("doc")
	require.NoError(t, err)
	for name, want := range map[string]int{"office.city": 1, "office.codes": 2, "deep.city": 2, "areas.city": 1} {
		require.True(t, cache[name].ArrayIndicesBounded, name)
		require.Equal(t, want, cache[name].MaxArrayIndices, name)
	}
	require.Nil(t, cache["office.city"].SettledArrayAncestors)
	for _, name := range []string{"open.city", "loose.city", "tree.name", "list"} {
		require.False(t, cache[name].ArrayIndicesBounded, name)
	}
}

// The census reads a primitive array whose items are typed through an anyOf
// as the registry does (#696 review R4-F1): verified, with its own index.
func TestDeriveArrayPositionsReadsCombinatorItems(t *testing.T) {
	dir := t.TempDir()
	writeSchemaArtifacts(t, dir, "doc",
		`{"type":"object","properties":{"tags":{"type":"array","items":{"anyOf":[{"type":"string"},{"type":"integer"}]}},`+
			`"mixed":{"type":"array","items":{"anyOf":[{"type":"string"},{"type":"object"}]}}}}`, `{}`)
	cache := forma.SchemaAttributeCache{
		"tags":  {AttributeName: "tags", ValueType: forma.ValueTypeText},
		"mixed": {AttributeName: "mixed", ValueType: forma.ValueTypeText},
	}
	positions, err := DeriveArrayPositions(dir, "doc", cache)
	require.NoError(t, err)
	require.Equal(t, ArrayPositions{Ancestors: []string{}, DeclaredArray: true, Verified: true}, positions["tags"])
	require.False(t, positions["mixed"].Verified)
}
