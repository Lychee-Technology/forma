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

// rebuildDocument places every record of one row in a new document. Where a
// record goes is decided by its own metadata and by objectPaths, which reads
// the whole row before anything is placed, so the document does not depend on
// the order the records arrive in (#619). Reading that decision off the part
// of the document already built placed a nested list that arrived before its
// first scalar sibling as an array of objects, which the sibling then
// replaced.
//
// The one shape this cannot settle is an object or array nested inside an
// array of objects, whose indices index an ancestor above the parent: the
// attribute cache does not record which ancestors are arrays, and such rows
// do not rebuild correctly in any order (#623).
func rebuildDocument(records []rebuildRecord) map[string]any {
	objects := objectPaths(records)
	doc := make(map[string]any)
	for _, rec := range records {
		placeRecord(doc, rec, objects)
	}
	return doc
}

// objectPaths returns every path the row proves is an object. A record with
// no indices has no array on its path, and a list record's indices index the
// list itself, since a list never sits inside an array (populateTypedValue
// refuses a multi-dimensional list): the proper ancestors of both are objects.
// Any other indexed record proves nothing, because which segment its indices
// index is the open question.
func objectPaths(records []rebuildRecord) map[string]struct{} {
	objects := make(map[string]struct{})
	for _, rec := range records {
		if len(rec.indices) > 0 && !rec.list {
			continue
		}
		for end := 1; end < len(rec.segments); end++ {
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
// itself rather than its parent. They do for a list, which the schema
// declares, and for an attribute with no parent. Otherwise they do only when
// the row proves the parent an object: the attribute is then a primitive
// array stored under element-typed metadata before #204 (contact.phones as
// text beside contact.name), which the attribute cache cannot tell from a
// member of an array of objects.
func ownsIndices(rec rebuildRecord, objects map[string]struct{}) bool {
	n := len(rec.segments)
	if rec.list || n == 1 {
		return true
	}
	_, parentIsObject := objects[strings.Join(rec.segments[:n-1], ".")]
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
