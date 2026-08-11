package internal

import (
	"testing"

	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/stretchr/testify/require"
)

// stripIndex loads the relation index over the shipped schemas, where
// visit.contactSnapshot is the only x-relation property in the repository.
func stripIndex(t *testing.T) *RelationIndex {
	t.Helper()
	registry, err := schemameta.NewFileSchemaRegistryFromDirectory(shippedSchemaDir)
	require.NoError(t, err)
	idx, err := LoadRelationIndex(registry)
	require.NoError(t, err)
	require.NotEmpty(t, idx.Relations("visit"), "visit must carry a relation for this test to mean anything")
	return idx
}

// TestStripRemovesRelationSubtree covers both spellings and depth.
func TestStripRemovesRelationSubtree(t *testing.T) {
	idx := stripIndex(t)

	out := idx.StripComputedFields("visit", map[string]any{
		"contactSnapshot":              map[string]any{"name": "Ada"},
		"contactSnapshot.name":         "Ada",
		"contactSnapshot.phones.0.tag": "home",
		"userId":                       "agent-1",
	})

	require.Equal(t, map[string]any{"userId": "agent-1"}, out)
}

// TestStripKeepsSamePrefixSibling is the boundary anchor for the "." in
// root+"." (relation_index.go, coversRelationSubtree). A bare HasPrefix would
// delete these ordinary attributes along with the subtree.
//
// It cannot fail against the pre-#318 exact-key code either, so it is a mutation
// anchor: it is red only against the wrong fix, which is verified by a mutation
// build rather than by this test alone.
func TestStripKeepsSamePrefixSibling(t *testing.T) {
	idx := stripIndex(t)

	in := map[string]any{
		"contactSnapshotX": "keep",
		"contactSnapshot2": "keep",
		"contactSnapshot":  map[string]any{"name": "drop"},
	}

	out := idx.StripComputedFields("visit", in)

	require.Equal(t, map[string]any{"contactSnapshotX": "keep", "contactSnapshot2": "keep"}, out)
}

// TestStripLeavesNothingCoveredForTheValidator is what the removal of the
// normalization carve-out rests on. #314 taught NormalizeDottedKeys to skip
// dotted keys beneath a relation root (shouldExpand, transform/normalize_keys.go)
// because the exact-key strip left them behind; the subtree strip leaves nothing
// for it to skip, so that skip is gone (#318). If this test ever fails, the skip
// has to come back before the strip is weakened.
func TestStripLeavesNothingCoveredForTheValidator(t *testing.T) {
	idx := stripIndex(t)
	roots := idx.RelationRoots("visit")
	require.NotEmpty(t, roots)

	out := idx.StripComputedFields("visit", map[string]any{
		"contactSnapshot":       map[string]any{"name": "Ada"},
		"contactSnapshot.name":  "Ada",
		"contactSnapshot.a.b.c": 1,
		"propertySnapshot.code": "P1",
	})

	for key := range out {
		require.False(t, roots.Covers(key), "key %q beneath a relation root reached the validator", key)
		require.NotContains(t, roots, key, "relation root %q reached the validator", key)
	}
	require.Contains(t, out, "propertySnapshot.code", "only the relation subtree is removed")
}

// TestRelationDeclarationsAreFoundOnlyInRootProperties pins where an x-relation
// marker is looked for, which is one place: the registered document's own
// top-level "properties" object (loadSchemaRelations).
//
// This is the feature quietly not applying, not the unwritable-entity hazard the
// startup guard exists for. A marker the walk never sees produces no descriptor,
// so nothing is stripped, nothing is derived on read, and the property behaves
// as an ordinary caller-written attribute — which is a working schema, just not
// the one its author wrote.
//
// Two shapes a schema author can reasonably expect to work and which do not:
//
//   - the property declared inside an "allOf" branch. The walk reads the root
//     map's "properties" key and does not compose the applicators, so the branch
//     is never opened;
//   - the property declared in a document reached by a root "$ref". Nothing here
//     follows a reference; only the bytes the registry serves under that name are
//     read.
//
// The third schema is the control, and the test is worthless without it: the
// same marker, in the same spelling, declared on a root property *is* found. So
// a failure to find it above is about position, not about a typo'd fixture.
func TestRelationDeclarationsAreFoundOnlyInRootProperties(t *testing.T) {
	const relationRoot = `{
		"$ref": "parent.json#/properties/contact",
		"x-relation": {"key_property": "parentId"}
	}`
	idx, err := LoadRelationIndex(newDocSchemaRegistry(map[string]string{
		"parent": `{"type":"object","properties":{"id":{"type":"string"},"contact":{"type":"object"}}}`,
		"in_allof": `{"type":"object",
			"properties":{"id":{"type":"string"},"parentId":{"type":"string"}},
			"allOf":[{"properties":{"contactSnapshot":` + relationRoot + `}}]}`,
		"behind_ref": `{"$ref":"declaring.json",
			"properties":{"id":{"type":"string"},"parentId":{"type":"string"}}}`,
		"declaring": `{"type":"object",
			"properties":{"id":{"type":"string"},"parentId":{"type":"string"},
				"contactSnapshot":` + relationRoot + `}}`,
	}))
	require.NoError(t, err)

	require.NotEmpty(t, idx.Relations("declaring"),
		"the control must be found, or this test proves nothing about position")

	for _, schema := range []string{"in_allof", "behind_ref"} {
		require.Empty(t, idx.Relations(schema), "%s declares its marker outside the root properties", schema)
		require.Nil(t, idx.RelationRootNames(schema))

		payload := map[string]any{"contactSnapshot": map[string]any{"name": "Ada"}, "contactSnapshot.name": "Ada"}
		require.Equal(t, payload, idx.StripComputedFields(schema, payload),
			"an undiscovered relation strips nothing: the property is written like any other")
	}
}

// TestStripIgnoresSchemasWithoutRelations covers the two pass-through guards: a
// schema declaring no x-relation, and a nil receiver. Both leave the payload
// untouched, dotted key included — the same key the strip removes under a schema
// that does declare the relation.
//
// The expectation is a separately built map, not in. The guards return the
// caller's own map, so comparing the result against in would compare a map with
// itself and stay green even if the strip began deleting keys from the caller's
// map in place.
func TestStripIgnoresSchemasWithoutRelations(t *testing.T) {
	idx := stripIndex(t)
	in := map[string]any{"contactSnapshot.name": "Ada"}
	want := map[string]any{"contactSnapshot.name": "Ada"}

	require.Equal(t, want, idx.StripComputedFields("lead", in))

	var nilIdx *RelationIndex
	require.Equal(t, want, nilIdx.StripComputedFields("visit", in))
}
