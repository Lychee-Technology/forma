package transform

import "strings"

// rebuildRecord is one stored record resolved against its schema
// (FromAttributes): the attribute's path, whether the attribute is declared a
// list, and the record's parsed indices and value.
type rebuildRecord struct {
	segments []string
	list     bool
	indices  []int
	value    any
}

// parentOpen reports whether the record leaves open what its parent is. An
// indexed record of a nested non-list attribute does: its indices index
// either the parent, an array of objects, or the attribute itself, a
// primitive array stored under element-typed metadata before #204
// (contact.phones as text), and the attribute cache cannot tell the two
// apart. Every other record has an object for a parent, or none: it carries
// no indices, or it is a list's, whose indices the schema declares its own.
func (rec rebuildRecord) parentOpen() bool {
	return len(rec.indices) > 0 && !rec.list && len(rec.segments) > 1
}

// rebuildDocument places every record of one row in a new document. What
// each path holds is settled before anything is placed, by the record's own
// metadata and by objectPaths, which reads the whole row, so the document
// does not depend on the order the records arrive in (#619). Reading that
// off the part of the document already built placed a nested list that
// arrived before its first scalar sibling as an array of objects, which the
// sibling then replaced.
//
// All records read a path they share the same way, so no placement replaces
// what another built. That does not make every reading right. A record's
// indices are taken to index its attribute or its parent, never an ancestor
// above the parent, because the attribute cache does not record which
// ancestors are arrays. An object or array nested inside an array of objects
// therefore rebuilds, in every order, with that array read as an object and
// its members as arrays of their own (#623).
func rebuildDocument(records []rebuildRecord) map[string]any {
	objects := objectPaths(records)
	doc := make(map[string]any)
	for _, rec := range records {
		placeRecord(doc, rec, objects)
	}
	return doc
}

// objectPaths returns every path the row's placements read as an object:
// each record's ancestors, apart from the parent a record leaves open
// (parentOpen). placeRecord reads exactly these paths as objects, and an
// open parent as one only when it is here, because another record is placed
// through it.
//
// The set is what the placements assume, which is more than the row proves.
// An indexed record does not prove the ancestors above its parent are
// objects (#623), and a list's empty-list marker written inside an array of
// objects carries no index to say so. Both are still placed as though those
// ancestors were objects, and a record read against that would replace what
// they built or be replaced by it, whichever came later.
func objectPaths(records []rebuildRecord) map[string]struct{} {
	objects := make(map[string]struct{})
	for _, rec := range records {
		ancestors := len(rec.segments) - 1
		if rec.parentOpen() {
			ancestors--
		}
		for end := 1; end <= ancestors; end++ {
			objects[strings.Join(rec.segments[:end], ".")] = struct{}{}
		}
	}
	return objects
}

// placeRecord places one record in doc.
//
// A list's record with no indices, an empty-list marker or a scalar stored
// before the attribute became a list (#372), fills only an empty path, and an
// element replaces any non-array it finds: the elements win in every order,
// as they do in the stored order, where an empty array_indices sorts first.
//
// An indexed record's indices index either the attribute itself or its
// parent, an array of objects (ownsIndices).
func placeRecord(doc map[string]any, rec rebuildRecord, objects map[string]struct{}) {
	n := len(rec.segments)
	field := rec.segments[n-1]
	if len(rec.indices) == 0 {
		parent := objectAt(doc, rec.segments[:n-1])
		if _, set := parent[field]; set && rec.list {
			return
		}
		parent[field] = rec.value
		return
	}
	if ownsIndices(rec, objects) {
		parent := objectAt(doc, rec.segments[:n-1])
		parent[field] = setArrayValueRecursive(ensureArray(parent, field), rec.indices, rec.value)
		return
	}
	owner, arrayField := objectAt(doc, rec.segments[:n-2]), rec.segments[n-2]
	owner[arrayField] = setObjectArrayValue(ensureArray(owner, arrayField), rec.indices, field, rec.value)
}

// ownsIndices reports whether an indexed record's indices index the attribute
// itself rather than its parent. They do unless the record leaves its parent
// open (parentOpen), and then only when the row reads that parent as an
// object (objectPaths): the attribute is a primitive array under
// element-typed metadata, contact.phones as text beside contact.name or
// beside contact.addresses.city. With no such sibling in the row the parent
// reads as an array of objects, which is also how a primitive array of that
// kind stored alone is read.
func ownsIndices(rec rebuildRecord, objects map[string]struct{}) bool {
	if !rec.parentOpen() {
		return true
	}
	_, parentIsObject := objects[strings.Join(rec.segments[:len(rec.segments)-1], ".")]
	return parentIsObject
}

// objectAt returns the object at path, creating it and every object above it
// and replacing any non-object on the way.
func objectAt(doc map[string]any, path []string) map[string]any {
	current := doc
	for _, segment := range path {
		next, ok := current[segment].(map[string]any)
		if !ok || next == nil {
			next = make(map[string]any)
			current[segment] = next
		}
		current = next
	}
	return current
}
