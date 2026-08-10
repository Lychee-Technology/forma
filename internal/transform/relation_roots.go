package transform

// RelationRoots is the set of top-level property names a schema derives from a
// parent entity, i.e. the properties carrying `x-relation`.
//
// It is declared here, as a plain set, rather than by importing the relation
// index: the index lives in package internal, which already depends on this
// package. The set's element type is unnamed, so internal can build one and
// assign it without a conversion.
//
// The write path does not consult this set: it removes the whole relation
// subtree from the payload before validating (RelationIndex.StripComputedFields)
// because it is derived on read and never persisted. This set is what the *read*
// path consults, to skip required-policy enforcement beneath a relation root
// (AttributeConverter.FromEAVRecords, #315).
type RelationRoots map[string]struct{}

// Covers reports whether name lies strictly beneath a relation root.
//
// The question is asked about the absolute attribute name, with no positional
// prefix. Relation roots are top-level properties of the entity schema, and the
// names asked about are the metadata cache's absolute attribute names, so
// "beneath a relation root" is a property of the name alone.
//
// A name that *is* a relation root is not covered: this reports names strictly
// beneath one, so the root's own required policy stays enforced — pinned by
// TestFromEAVRecordsEnforcesRequiredPolicyOnRelationRootItself. The write path's
// strip predicate (RelationIndex.coversRelationSubtree) deliberately differs
// there and matches the bare root as well, because the root is the nested
// spelling the strip removes.
func (r RelationRoots) Covers(name string) bool {
	if len(r) == 0 {
		return false
	}
	for i, char := range name {
		if char != '.' {
			continue
		}
		if _, ok := r[name[:i]]; ok {
			return true
		}
	}
	return false
}

// RelationRootsLookup answers a schema's relation roots by schema name.
//
// It exists so package internal — which owns RelationIndex and is the only
// place that can build a RelationRoots set — can hand the set to this package
// without an import cycle, at the seams that only know a schema ID.
type RelationRootsLookup func(schemaName string) RelationRoots

// RelationRootsAware is implemented by the transformers in this package so the
// relation roots can be installed after construction.
//
// NewEntityManager loads the relation index itself, from config, and by then
// the transformer it was handed already exists — so the lookup is installed
// once at wiring time rather than injected at construction. Install before the
// transformer is used concurrently; nothing reads the field until then.
type RelationRootsAware interface {
	SetRelationRoots(lookup RelationRootsLookup)
}
