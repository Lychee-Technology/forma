package transform

import "strings"

// The shape rule's place in the document walk (scalarShapeRefusal, #696).

// rowArray is an array an update's merged document holds where a single
// value belongs, which resolveStoredValues found holding only the row's own
// records of the attribute, each where it was stored: a member of an array
// of objects that a row without array ancestry rebuilds as an array of its
// own (refuseShape). The merged write walks it as an array, so each record
// is written back where it was. A caller's value is never one, and the shape
// rule refuses a caller's array there.
type rowArray []any

// refuseShape records the shape rule's refusal of the container value at
// path (#696) and reports whether the walk stops there. The container is
// refused whole, before the walk descends and loses its shape, but it is
// still scanned under the nesting cap first, so a cyclic or too deep one is
// refused as such, and nothing recurses past the cap.
//
// In walkMergeDocument a container holding stored values is the stored
// row's, not the caller's, and the walk descends it as before. One that
// holds only records of the attribute, none with more array indices than
// the document declares for it, is a member of an array of objects that a
// row without array ancestry rebuilds as an array of its own
// (rebuildInferred), or the records of an attribute whose document does not
// settle its shape (recordsInPlace): the walk writes each record back where
// it was, nothing is refused, and the resolution marks the array as the
// row's (rowArray).
// Any other is answered apart (storedShapes) for the resolution to judge
// once it knows which stored values the write keeps.
func (w *documentWalk) refuseShape(path []string, value any, indices []int, err error) bool {
	held, other := w.scanRefused(path, value, indices)
	if w.tooDeep != nil {
		return true
	}
	name := strings.Join(path, ".")
	refusal := documentEntry{kind: entryRefusal, name: name, value: value, refusal: err}
	array, isArray := value.([]any)
	switch {
	case w.storedShapes == nil || len(held) == 0:
		w.entries = append(w.entries, refusal)
		return true
	case other || !isArray || !w.recordsInPlace(name, held):
		w.storedShapes = append(w.storedShapes, refusal)
	default:
		w.rowArrays[&array[0]] = struct{}{}
	}
	return false
}

// scanRefused descends a container the shape rule refuses as the walk
// would, under the nesting cap, and answers the stored values it holds and
// whether it holds any other value.
func (w *documentWalk) scanRefused(path []string, value any, indices []int) ([]*storedValue, bool) {
	var held []*storedValue
	other := false
	scan := func(childPath []string, child any, childIndices []int) {
		childHeld, childOther := w.scanRefused(childPath, child, childIndices)
		held, other = append(held, childHeld...), other || childOther
	}
	switch v := value.(type) {
	case *storedValue:
		return []*storedValue{v}, false
	case nil:
	case map[string]any:
		if !w.enter(path, indices) {
			return nil, false
		}
		for key, item := range v {
			if scan(append(path, key), item, indices); w.tooDeep != nil {
				return nil, false
			}
		}
	case []any:
		if !w.enter(path, indices) {
			return nil, false
		}
		for i, item := range v {
			if scan(path, item, append(indices, i)); w.tooDeep != nil {
				return nil, false
			}
		}
	default:
		other = true
	}
	return held, other
}

// indexCount answers how many array indices an array_indices value holds.
func indexCount(arrayIndices string) int {
	if arrayIndices == "" {
		return 0
	}
	return strings.Count(arrayIndices, ",") + 1
}

// recordsInPlace reports whether every held value is a record of the
// attribute name carrying no more array indices than the schema document
// declares for it under any reading (Declaration.Indices): a record carries
// nothing that tells its ancestry apart, and a stored one is not the
// caller's to judge by the strictest reading. Where the document does not settle
// how many it may carry (no document, an open declaration, an unresolved
// $ref), nothing shows the stored records malformed: they may be a
// primitive array recorded under its element type before #204. They are
// stored state, not the caller's, so the write puts them back where they
// were rather than refuse every update that keeps them; the caller's own
// array there is still refused (scalarShapeRefusal).
func (w *documentWalk) recordsInPlace(name string, held []*storedValue) bool {
	meta := w.cache[name]
	_, most, exact := w.shapes.Declared(name).Indices()
	for _, v := range held {
		if v.record.AttrID != meta.AttributeID || exact && indexCount(v.record.ArrayIndices) > most {
			return false
		}
	}
	return true
}
