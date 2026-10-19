package transform

import "strings"

// The rebuild of a row whose schema declares no array ancestry
// (rebuildDocument): each record's array ancestor is inferred from the row.

// parentOpen reports whether the record leaves open what its parent is. An
// indexed record of a nested non-list attribute does: its indices index
// either the parent, an array of objects, or the attribute itself, a
// primitive array stored under element-typed metadata before #204
// (contact.phones as text), and without ancestry nothing tells the two
// apart. Every other record has an object for a parent, or none: it carries
// no indices, or it is a list's, whose indices the schema declares its own.
func (rec rebuildRecord) parentOpen() bool {
	return len(rec.indices) > 0 && !rec.list && len(rec.segments) > 1
}

// rebuildInferred places every record taking its indices to index its
// attribute or its parent, never an ancestor above the parent. What each
// path holds is settled before anything is placed, by the record's own
// metadata and by objectPaths, which reads the whole row, so the document
// does not depend on the order the records arrive in (#619).
//
// All records read a path they share the same way, but an object or array
// nested inside an array of objects rebuilds, in every order, with that
// array read as an object and its members as arrays of their own (#623).
// A record with more indices than its parent takes lands beneath its own
// field's name, where the write cannot read it back; an update refuses to
// carry it rather than drop it (resolveStoredValues). An empty-list marker
// is written without indices for a schema read this way (emptyList), so one
// inside an array of objects reads that array as an object too. A marker
// with indices, which only a schema that declared array ancestors when it
// was written stores, names an element this reading cannot place: it is
// left out of the document and returned, so that an update refuses to
// delete it (MergeUpdate).
func rebuildInferred(records []rebuildRecord) (map[string]any, []rebuildRecord) {
	placed := make([]rebuildRecord, 0, len(records))
	var unplaced []rebuildRecord
	for _, rec := range records {
		if rec.marker && len(rec.indices) > 0 {
			unplaced = append(unplaced, rec)
			continue
		}
		placed = append(placed, rec)
	}
	objects := objectPaths(placed)
	doc := make(map[string]any)
	for _, rec := range placed {
		placeInferred(doc, rec, objects)
	}
	return doc, unplaced
}

// objectPaths returns every path the row's placements read as an object:
// each record's ancestors, apart from the parent a record leaves open
// (parentOpen). placeInferred reads exactly these paths as objects, and an
// open parent as one only when it is here, because another record is placed
// through it.
//
// The set is what the placements assume, which is more than the row proves:
// an indexed record does not prove the ancestors above its parent are
// objects (#623), yet it is placed as though they were.
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

// placeInferred places one record in doc.
//
// A list's record with no indices, an empty-list marker or a scalar stored
// before the attribute became a list (#372), fills only an empty path, and an
// element replaces any non-array it finds: the elements win in every order,
// as they do in the stored order, where an empty array_indices sorts first.
//
// An indexed record's indices index either the attribute itself or its
// parent, an array of objects (ownsIndices). An element of that array no
// record reaches reads as {} (expandObjectArray, #626).
func placeInferred(doc map[string]any, rec rebuildRecord, objects map[string]struct{}) {
	n := len(rec.segments)
	if len(rec.indices) == 0 || ownsIndices(rec, objects) {
		setField(objectAt(doc, rec.segments[:n-1]), rec.segments[n-1], rec, rec.indices)
		return
	}
	owner, arrayField := objectAt(doc, rec.segments[:n-2]), rec.segments[n-2]
	owner[arrayField] = setObjectArrayValue(ensureArray(owner, arrayField), rec.indices, rec.segments[n-1], rec.value)
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
