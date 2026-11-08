package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// subtreePredicateNames are the boundary cases of the at-or-beneath rule around
// the root "contactSnapshot": the root itself, dotted descendants at depth,
// same-prefix siblings (the load-bearing "."), a prefix of the root, the root
// spelled beneath another property, and an unrelated name.
var subtreePredicateNames = []struct {
	name    string
	atOrIn  bool
	beneath bool
}{
	{"contactSnapshot", true, false},
	{"contactSnapshot.name", true, true},
	{"contactSnapshot.phones.0.tag", true, true},
	{"contactSnapshotX", false, false},
	{"contactSnapshotX.name", false, false},
	{"contactSnapshot_name", false, false},
	{"contact", false, false},
	{"contact.name", false, false},
	{"owner.contactSnapshot", false, false},
	{"owner.contactSnapshot.name", false, false},
	{"userId", false, false},
	{"", false, false},
}

// TestCoversSubtreeMatchesRootAndDescendantsOnly pins the one at-or-beneath
// definition (RelationDescriptor.coversSubtree) that both the payload strip and
// enrichDataRecords' requested-attribute filter ask (#390).
func TestCoversSubtreeMatchesRootAndDescendantsOnly(t *testing.T) {
	rel := RelationDescriptor{ChildPath: "contactSnapshot"}
	for _, tc := range subtreePredicateNames {
		require.Equal(t, tc.atOrIn, rel.coversSubtree(tc.name), "coversSubtree(%q)", tc.name)
	}
}

// TestCoversSubtreeAgreesWithStrictCoversBeneathRoot pins
// transform.RelationRoots.Covers as the strict-descendant variant of
// RelationDescriptor.coversSubtree: the two agree on every name except the bare
// root, which only coversSubtree matches. Covers cannot call coversSubtree (package
// transform cannot import this one), so this test is what fails when the two
// drift.
func TestCoversSubtreeAgreesWithStrictCoversBeneathRoot(t *testing.T) {
	idx := stripIndex(t)
	rels := idx.Relations("visit")
	roots := idx.RelationRoots("visit")

	for _, tc := range subtreePredicateNames {
		require.Equal(t, tc.beneath, roots.Covers(tc.name), "Covers(%q)", tc.name)
	}

	for _, rel := range rels {
		names := []string{rel.ChildPath, rel.ChildPath + ".a", rel.ChildPath + ".a.b", rel.ChildPath + "X", rel.ChildPath + "X.a"}
		for _, name := range names {
			strict := rel.coversSubtree(name) && name != rel.ChildPath
			require.Equal(t, strict, roots.Covers(name), "root %q, name %q", rel.ChildPath, name)
			require.Equal(t, rel.coversSubtree(name), idx.coversRelationSubtree("visit", name),
				"root %q, name %q", rel.ChildPath, name)
		}
	}
}
