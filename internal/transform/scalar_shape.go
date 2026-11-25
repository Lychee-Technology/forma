package transform

import (
	"fmt"
	"slices"
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
//
// An array at a parent's path holds one index per array of objects, so where
// the schema document settles the attribute's arrays of objects, a single
// value the caller sends inside more arrays than that is refused too
// (excessIndicesError): {"items": [[{"name": "x"}]]} would store name at
// "0,0", which the read takes for an element of name's own array. A
// primitive array the schema document declares under an element-typed
// ledger (forma.AttributeMetadata.DeclaredArray) takes its array like a
// list, with one index more.
//
// The predicates below are the rule; the walk (visitAt) and the intake
// (copyAt) apply them to the same positions.

// arrayValued reports whether the attribute takes an array at its own
// position: a list, or a primitive array the schema document declares.
func arrayValued(meta forma.AttributeMetadata) bool {
	return meta.ValueType == forma.ValueTypeList || meta.DeclaredArray
}

// shapeAt judges a container sent at an attribute's own position: the array
// to walk element by element when the attribute takes one, or the refusal.
func shapeAt(attrName string, meta forma.AttributeMetadata, value any, indices []int) ([]any, error) {
	if array, isArray := value.([]any); isArray && arrayValued(meta) {
		return array, nil
	}
	return nil, containerError(attrName, meta, value, false, joinIndices(indices))
}

// elementShape is how an element of an array-valued attribute's array is
// judged (elementShapeOf).
type elementShape uint8

const (
	// elementSingle is walked as one value.
	elementSingle elementShape = iota
	// elementNested is a list's element that is itself an array, walked
	// element by element in turn: an [] there is emptyList's, and a value
	// there the conversion's (listElementMeta), as before #696.
	elementNested
	// elementRefused is a container where the element holds one value.
	elementRefused
)

// elementShapeOf judges one element of an array-valued attribute's array. An
// object is never a single value. An array is refused in a declared
// primitive array; in a list it is walked on, so an object anywhere inside a
// list's value is refused rather than written as nothing ({"tags": [[{}]]}).
func elementShapeOf(meta forma.AttributeMetadata, item any) elementShape {
	switch item.(type) {
	case map[string]any:
		return elementRefused
	case []any:
		if meta.ValueType == forma.ValueTypeList {
			return elementNested
		}
		return elementRefused
	}
	return elementSingle
}

// excessIndicesError refuses a single value the caller sent inside more
// arrays than the schema document settles for the attribute
// (forma.AttributeMetadata.SettledArrayAncestors): one index per array of
// objects around it, and one more for a declared primitive array's element.
// Where the document does not settle them, as beside an anyOf that may admit
// an array, nothing proves an index wrong, and the value is written as
// before #696. The registry's array ancestry is not that proof: it is
// derived leniently, and for every attribute of a schema or for none. A
// list's indices are its conversion's (listElementMeta).
func excessIndicesError(attrName string, meta forma.AttributeMetadata, indices []int) error {
	if meta.SettledArrayAncestors == nil || meta.ValueType == forma.ValueTypeList {
		return nil
	}
	admitted := len(meta.SettledArrayAncestors)
	if meta.DeclaredArray {
		admitted++
	}
	if len(indices) <= admitted {
		return nil
	}
	ancestors := "none"
	if len(meta.SettledArrayAncestors) > 0 {
		ancestors = strings.Join(meta.SettledArrayAncestors, ", ")
	}
	return conversionRefusal(attrName, meta, fmt.Errorf(
		"a single %s value at array indices %q is nested in more arrays than the attribute's arrays of objects (%s); "+
			"an array cannot be stored where a single value belongs",
		meta.ValueType, joinIndices(indices), ancestors))
}

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
	if array, err := shapeAt(strings.Join(path, "."), meta, value, indices); err == nil {
		w.visitElements(path, array, indices, meta)
		return
	}
	w.judgeContainer(path, value, indices, meta, false)
}

// visitElements walks an array-valued attribute's array as visitArray does,
// except that each element is judged by elementShapeOf: a refused one like
// a container at a scalar.
func (w *documentWalk) visitElements(path []string, v []any, indices []int, meta forma.AttributeMetadata) {
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
		at := append(slices.Clip(indices), i)
		switch {
		case item == nil:
			w.nullElement(path, i)
		case elementShapeOf(meta, item) == elementRefused:
			w.judgeContainer(path, item, at, meta, true)
		case elementShapeOf(meta, item) == elementNested:
			w.visitElements(path, item.([]any), at, meta)
		default:
			w.visit(path, item, at)
		}
	}
}

// refuseShape records the shape rule's refusal at attrName.
func (w *documentWalk) refuseShape(attrName string, err error) {
	w.entries = append(w.entries, documentEntry{kind: entryRefusal, name: attrName, refusal: err, shape: true})
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
	w.refuseShape(attrName, containerError(attrName, meta, value, element, arrayIndices))
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
	case element && meta.ValueType == forma.ValueTypeList:
		reason = fmt.Sprintf("list element at array indices %q takes a single %s value, got %s; a list's elements cannot be objects",
			arrayIndices, meta.EffectiveItemsType(), describeContainer(value))
	case element:
		reason = fmt.Sprintf("array element at array indices %q takes a single %s value, got %s; "+
			"the elements of an array the schema declares of single values cannot be objects or arrays",
			arrayIndices, meta.ValueType, describeContainer(value))
	case meta.DeclaredArray:
		reason = fmt.Sprintf("the schema declares an array of single %s values, got %s", meta.ValueType, describeContainer(value))
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
