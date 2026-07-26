package transform

// RelationRoots is the set of top-level property names a schema derives from a
// parent entity, i.e. the properties carrying `x-relation`.
//
// It is declared here, as a plain set, rather than by importing the relation
// index: the index lives in package internal, which already depends on this
// package. The set's element type is unnamed, so internal can build one and
// assign it without a conversion.
//
// The write path removes these properties from the payload before validating
// (RelationIndex.StripComputedFields) because they are derived on read and never
// persisted. NormalizeDottedKeys needs the same set so it does not rebuild what
// the strip just removed.
type RelationRoots map[string]struct{}

// Covers reports whether name lies strictly beneath a relation root.
//
// The question is asked about the absolute attribute name, with no positional
// prefix — unlike ArrayPaths.CrossesBelow. Relation roots are top-level
// properties of the entity schema and the strip removes them at the document
// root, so "beneath a relation root" is a property of the name alone.
//
// A name that *is* a relation root is not covered. It never reaches this check
// in production (the strip deleted it) and it is not dotted, so expansion does
// not apply to it either way.
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
