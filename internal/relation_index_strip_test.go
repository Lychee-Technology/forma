package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// stripIndex loads the relation index over the shipped schemas, where
// visit.contactSnapshot is the only x-relation property in the repository.
func stripIndex(t *testing.T) *RelationIndex {
	t.Helper()
	idx, err := LoadRelationIndex(shippedSchemaDir)
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
