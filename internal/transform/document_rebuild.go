package transform

import (
	"strconv"
	"strings"
)

// rebuildRecord is one stored record resolved against its schema
// (FromAttributes): the attribute's path, whether the attribute is declared a
// list and the record is its empty-list marker, the attribute's array
// ancestors (forma.AttributeMetadata.ArrayAncestors, nil when the registry
// did not derive them), and the record's parsed indices and value.
type rebuildRecord struct {
	segments  []string
	list      bool
	marker    bool
	ancestors []string
	indices   []int
	value     any
}

// rebuildDocument places every record of one row in a new document, in a
// way that does not depend on the order the records arrive in (#619).
//
// A record's array_indices list one index per array the writer traversed,
// and its attribute's name says nothing about which ancestors those were.
// When every record's attribute declares its array ancestors, they say:
// each index lands in the ancestor it belongs to (rebuildByAncestry), so an
// object or array nested inside an array of objects rebuilds in the shape it
// was written in (#623). A row with any attribute that does not declare them
// is read as before, by inferring each record's array from the row
// (rebuildInferred); the two readings are never mixed within a row.
//
// It also returns the records the document leaves out that a reading by
// array ancestry places: the indexed empty-list markers a row read by
// inference holds, which an update refuses rather than deletes
// (MergeUpdate). The markers a reading by ancestry leaves out are not
// among them, since no reading places them.
func rebuildDocument(records []rebuildRecord) (map[string]any, []rebuildRecord) {
	for _, rec := range records {
		if rec.ancestors == nil {
			return rebuildInferred(records)
		}
	}
	return rebuildByAncestry(records), nil
}

// rebuildByAncestry places every record by its attribute's array ancestors.
// A path is an array of objects for every record of the row when any record's
// attribute declares it one, so all records read a path they share the same
// way and no placement replaces what another built.
//
// A record's indices fill its array ancestors outermost first. Indices past
// the ancestors index the attribute's own array: a list's elements, or a
// primitive array stored under element-typed metadata before #204
// (contact.phones as text). A record with fewer indices than ancestors was
// written where the first array it does not index was an object, or through
// a literal dotted key that crosses that array
// ({"orders": [{"items.qty": 1}]}), or is a row the federated reader
// collapsed to one value. Such a record reads that array as an object, which
// is what it read as without ancestry, unless the row holds elements of the
// array in the same element of its outer arrays. Then it goes under a literal
// key beside the array, which the write reads back at the indices it is
// stored under. The choice is made for all of an attribute's records with as
// many indices at once, so they share one key spelling, which is all a write
// keeps of an attribute (#312). An empty-list marker with no indices beside
// an array with elements is left out: one written before #623 carried none
// wherever its list was, so it names none of the elements.
func rebuildByAncestry(records []rebuildRecord) map[string]any {
	arrays := make(map[string]struct{})
	for _, rec := range records {
		for _, ancestor := range rec.ancestors {
			arrays[ancestor] = struct{}{}
		}
	}
	ends := make([][]int, len(records))
	filled := make(map[string]struct{})
	for i, rec := range records {
		ends[i] = arrayEnds(rec.segments, arrays)
		for j := 0; j < len(ends[i]) && j < len(rec.indices); j++ {
			filled[arrayContext(rec, ends[i], j)] = struct{}{}
		}
	}
	literal := make(map[string]bool)
	for i, rec := range records {
		if len(rec.indices) < len(ends[i]) {
			_, conflict := filled[arrayContext(rec, ends[i], len(rec.indices))]
			literal[spellingGroup(rec)] = literal[spellingGroup(rec)] || conflict
		}
	}
	doc := make(map[string]any)
	for i, rec := range records {
		placeByAncestry(doc, rec, ends[i], literal[spellingGroup(rec)])
	}
	return doc
}

// arrayEnds returns, outermost first, the length of every proper prefix of
// segments that names an array of objects.
func arrayEnds(segments []string, arrays map[string]struct{}) []int {
	var ends []int
	for end := 1; end < len(segments); end++ {
		if _, ok := arrays[strings.Join(segments[:end], ".")]; ok {
			ends = append(ends, end)
		}
	}
	return ends
}

// arrayContext names the record's j-th array ancestor inside the elements of
// the outer arrays its first j indices select.
func arrayContext(rec rebuildRecord, ends []int, j int) string {
	return strings.Join(rec.segments[:ends[j]], ".") + "@" + joinIndices(rec.indices[:j])
}

// spellingGroup names the record's attribute and index count.
func spellingGroup(rec rebuildRecord) string {
	return strings.Join(rec.segments, ".") + "#" + strconv.Itoa(len(rec.indices))
}

// placeByAncestry places one record whose array ancestors end at ends
// (rebuildByAncestry), under a literal key beside the first array it does
// not index when literal is set. An element of an array of objects no record
// reaches reads as {} (expandObjectArray, #626).
func placeByAncestry(doc map[string]any, rec rebuildRecord, ends []int, literal bool) {
	segments := rec.segments
	holder, from := doc, 0
	for j, end := range ends {
		if j == len(rec.indices) {
			if !literal {
				break
			}
			if rec.marker && j == 0 {
				return
			}
			setField(objectAt(holder, segments[from:end-1]), strings.Join(segments[end-1:], "."), rec, nil)
			return
		}
		holder = elementAt(objectAt(holder, segments[from:end-1]), segments[end-1], rec.indices[j])
		from = end
	}
	var own []int
	if len(rec.indices) > len(ends) {
		own = rec.indices[len(ends):]
	}
	if rec.marker && len(own) > 0 {
		// A list's row with no value at an element's index is no element,
		// and no marker either; the read has always left it out.
		return
	}
	n := len(segments)
	setField(objectAt(holder, segments[from:n-1]), segments[n-1], rec, own)
}

// setField sets the record's value at parent[field], own indexing the
// attribute's own array. A list's record with no own index, an empty-list
// marker or a scalar stored before the attribute became a list (#372), fills
// only an empty field, and an element replaces any non-array it finds: the
// elements win in every order, as they do in the stored order, where an
// empty array_indices sorts first.
func setField(parent map[string]any, field string, rec rebuildRecord, own []int) {
	if len(own) > 0 {
		parent[field] = setArrayValueRecursive(ensureArray(parent, field), own, rec.value)
		return
	}
	if _, set := parent[field]; set && rec.list {
		return
	}
	parent[field] = rec.value
}

// elementAt returns the object at index idx of the array of objects at
// holder[field], creating the array, replacing any non-array there and
// padding it with {} up to idx.
func elementAt(holder map[string]any, field string, idx int) map[string]any {
	arr := expandObjectArray(ensureArray(holder, field), idx)
	element, ok := arr[idx].(map[string]any)
	if !ok {
		element = make(map[string]any)
		arr[idx] = element
	}
	holder[field] = arr
	return element
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
