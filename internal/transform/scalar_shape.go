package transform

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
)

// The shape rule (#696): what the caller may send at an attribute's position
// is read off the attribute's metadata, never off the validation mode. A list
// attribute takes an array of single values; every other value type takes one
// single value. An object or an array, empty or not, sent where a single
// value belongs is refused instead of being taken apart: {} and [] used to
// write nothing, which kept a bound column and removed an EAV record, and
// ["a"] stored an element record under a scalar attribute.
//
// Two places judge it, with the same predicate. The update intake judges the
// caller's Updates before any statement (CheckUpdateValues), and the merge
// judges the document the update writes (walkMergedDocument), where it can
// still tell the caller's containers from the stored row's. A stored row is
// written back in the shape it was read: a row read without array ancestry
// rebuilds the members of an array of objects as arrays of their own
// (rebuildInferred), and nothing short of the stored records tells such a
// row from one a report-only update corrupted before #696.
//
// Only the containers the walk decomposes, map[string]any and []any, are
// shapes. Any other Go value at a scalar, a []string or a struct included,
// is the conversion's to judge (populateTypedValue), as before.
//
// The rule is positional. An array at the path of an attribute's parent is
// an array of objects, not the attribute's value, so items: [{"name": "x"}]
// passes and items: [{"name": ["x"]}] is refused at items.name. A literal
// dotted key names the attribute itself, so {"items.name": ["x"]} is refused
// whatever items is, and {"items.tags": ["x"]} is a list's array.

// visitAt visits value at path, the position a map key leads to. Under the
// shape rule a container at an attribute's own position is judged by the
// attribute's shape; any other position is walked as before.
func (w *documentWalk) visitAt(path []string, value any, indices []int) {
	if !w.judgeShapes || w.scanning > 0 || !isContainer(value) {
		w.visit(path, value, indices)
		return
	}
	meta, ok := w.cache[strings.Join(path, ".")]
	if !ok {
		w.visit(path, value, indices)
		return
	}
	if list, isArray := value.([]any); isArray && meta.ValueType == forma.ValueTypeList {
		w.visitList(path, list, indices, meta)
		return
	}
	w.judgeContainer(path, value, indices, meta, false)
}

// visitList walks a list attribute's array as visitArray does, except that an
// object element is judged like an object at a scalar: a list's element is a
// single value.
func (w *documentWalk) visitList(path []string, v []any, indices []int, meta forma.AttributeMetadata) {
	if len(v) == 0 {
		w.visitArray(path, v, indices)
		return
	}
	if !w.enter(path, indices) {
		return
	}
	for i, item := range v {
		if w.tooDeep != nil {
			return
		}
		switch element := item.(type) {
		case nil:
			w.nullElement(path, i)
		case map[string]any:
			w.judgeContainer(path, element, append(indices, i), meta, true)
		default:
			w.visit(path, element, append(indices, i))
		}
	}
}

// judgeContainer judges value, a container at a position of the attribute
// meta that holds a single value: its whole value, or a list's element.
//
// The container is walked like any other, under the nesting cap, which also
// tells who put it there. One whose values are all stored ones is the stored
// row's, and what the walk found in it stands: the nulls and {} its rebuild
// pads an array with are not values. Any other is the caller's, an empty
// container or one of nulls included: what the walk added is dropped and the
// position is refused. A container past the cap is refused by the cap, as
// before, and a cyclic one cannot outrun it.
func (w *documentWalk) judgeContainer(path []string, value any, indices []int, meta forma.AttributeMetadata, element bool) {
	attrName, arrayIndices := strings.Join(path, "."), joinIndices(indices)
	start, claims, sent, held := len(w.entries), w.claims, w.sent, w.held
	w.scanning++
	w.visit(path, value, indices)
	w.scanning--
	if w.tooDeep != nil || (w.sent == sent && w.held > held) {
		return
	}
	w.entries, w.claims = w.entries[:start], claims
	w.entries = append(w.entries, documentEntry{
		kind:    entryRefusal,
		name:    attrName,
		refusal: containerError(attrName, meta, value, element, arrayIndices),
		shape:   true,
	})
}

func isContainer(value any) bool {
	switch value.(type) {
	case map[string]any, []any:
		return true
	}
	return false
}

// containerError publishes the refusal of a container the caller sent where
// the attribute holds a single value, in the conversion's form: the
// attribute, its declared type and what was sent instead.
func containerError(attrName string, meta forma.AttributeMetadata, value any, element bool, arrayIndices string) error {
	var reason string
	switch {
	case element:
		reason = fmt.Sprintf("list element at array indices %q takes a single %s value, got %s; a list's elements cannot be objects",
			arrayIndices, meta.EffectiveItemsType(), describeContainer(value))
	case meta.ValueType == forma.ValueTypeList:
		reason = fmt.Sprintf("declared type list takes an array of single %s values, got %s",
			meta.EffectiveItemsType(), describeContainer(value))
	default:
		reason = fmt.Sprintf("declared type %s takes a single value, got %s; an object or array, empty or not, "+
			"cannot be stored where a single value belongs (omit the key to keep the current value)",
			meta.ValueType, describeContainer(value))
	}
	return conversionRefusal(attrName, meta, fmt.Errorf("%s", reason))
}

// describeContainer names a container's kind for a refusal.
func describeContainer(value any) string {
	switch v := value.(type) {
	case map[string]any:
		if len(v) == 0 {
			return "an empty object"
		}
		return "an object"
	case []any:
		if len(v) == 0 {
			return "an empty array"
		}
		return "an array"
	}
	return fmt.Sprintf("%T", value)
}
