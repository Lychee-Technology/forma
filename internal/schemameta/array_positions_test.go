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
