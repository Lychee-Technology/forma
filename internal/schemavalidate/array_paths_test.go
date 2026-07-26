package schemavalidate

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestArrayPathsFromShippedSchema pins the derivation against a real schema
// rather than an invented one. lead_full.json declares arrays at several depths
// and with both object and primitive items, and its attribute sidecar defines
// requirement.areas.city — the attribute whose dotted spelling cannot be
// expanded without contradicting the schema (#314).
func TestArrayPathsFromShippedSchema(t *testing.T) {
	dir := shippedSchemaDir(t)
	body, err := os.ReadFile(filepath.Join(dir, "lead_full.json"))
	require.NoError(t, err)

	v, err := New(registryWith(t, "lead_full", string(body), 100), dir)
	require.NoError(t, err)

	paths := v.ArrayPaths(100)
	for _, expected := range []string{
		"requirement.areas",    // items: object — the #314 case
		"requirement.mustHave", // items: string
		"contact.phones",       // nested under an object
		"tags",                 // root level
		"propertyInterests", "visits", "communications",
		"requirement.niceToHave", "requirement.unwantedConditions",
	} {
		require.Contains(t, paths, expected)
	}

	require.NotContains(t, paths, "contact", "an object is not an array")
	require.NotContains(t, paths, "requirement.areas.city", "a string member is not an array")
	require.NotContains(t, paths, "", "the root is never a path")
}

// TestArrayPathsCrossesOnlyProperPrefixes pins the predicate the normalizer
// depends on. A name that *is* the array path describes the array itself and must
// still be nested, or its value is never validated; only something *under* an
// array cannot be nested.
func TestArrayPathsCrossesOnlyProperPrefixes(t *testing.T) {
	paths := ArrayPaths{
		"requirement.areas": {},
		"tags":              {},
	}

	require.True(t, paths.Crosses("requirement.areas.city"))
	require.True(t, paths.Crosses("requirement.areas.deep.nested"))
	require.True(t, paths.Crosses("tags.anything"))

	require.False(t, paths.Crosses("requirement.areas"), "the array path itself does not cross")
	require.False(t, paths.Crosses("tags"))
	require.False(t, paths.Crosses("requirement.budget.max"))
	require.False(t, paths.Crosses("contact.email"))
	require.False(t, paths.Crosses("requirement.areasX.city"), "prefixes must end at a dot")
}

// TestArrayPathsNilIsSafe pins that absent knowledge reads as "nothing is an
// array", which leaves callers with pre-#314 behaviour instead of panicking.
func TestArrayPathsNilIsSafe(t *testing.T) {
	var absent ArrayPaths
	require.False(t, absent.Crosses("requirement.areas.city"))
	require.False(t, ArrayPaths{}.Crosses("requirement.areas.city"))

	var nilValidator *Validator
	require.Nil(t, nilValidator.ArrayPaths(100))

	v, err := New(registryWith(t, "s", `{"type":"object"}`, 7), t.TempDir())
	require.NoError(t, err)
	require.Nil(t, v.ArrayPaths(999), "unknown schema id")
	require.Nil(t, v.ArrayPaths(7), "a schema with no arrays declares no paths")
}

// TestArrayPathsFindsArraysUnderCombinators covers allOf/anyOf/oneOf, where a
// property's real shape is often declared. Missing one would make a dotted key
// under it expandable again, reintroducing the false rejection.
func TestArrayPathsFindsArraysUnderCombinators(t *testing.T) {
	schema := `{
	  "type": "object",
	  "properties": {
	    "outer": {
	      "allOf": [{"type": "object", "properties": {
	        "inner": {"type": "array", "items": {"type": "object",
	          "properties": {"leaf": {"type": "string"}}}}}}]
	    },
	    "either": {
	      "anyOf": [{"type": "string"}, {"type": "array", "items": {"type": "string"}}]
	    }
	  }
	}`

	v, err := New(registryWith(t, "combined", schema, 8), t.TempDir())
	require.NoError(t, err)

	paths := v.ArrayPaths(8)
	require.Contains(t, paths, "outer.inner")
	require.Contains(t, paths, "either")
	require.True(t, paths.Crosses("outer.inner.leaf"))
}

// TestArrayPathsKeepsParentPathForItems pins the attribute-name convention: an
// index is not part of a name, so a property of an array's items sits at the
// array's own path. Anything else would misalign the set with the attribute
// names the normalizer tests against.
func TestArrayPathsKeepsParentPathForItems(t *testing.T) {
	schema := `{
	  "type": "object",
	  "properties": {
	    "areas": {"type": "array", "items": {"type": "object", "properties": {
	      "sub": {"type": "array", "items": {"type": "string"}}}}}
	  }
	}`

	v, err := New(registryWith(t, "nested", schema, 9), t.TempDir())
	require.NoError(t, err)

	paths := v.ArrayPaths(9)
	require.Contains(t, paths, "areas")
	require.Contains(t, paths, "areas.sub", "the inner array keeps its parent's path, not an index")
}
