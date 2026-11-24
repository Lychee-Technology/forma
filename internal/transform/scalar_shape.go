package transform

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
)

// An attribute's own position holds one value of its type, or for a list an
// array of them. The write path decomposes a JSON object or array into the
// positions beneath it, so a container sent where a single value belongs
// loses its shape: {} and [] write no record at all (an EAV attribute
// vanishes, a bound column keeps its old value), and a non-empty array
// writes element records under an attribute that has no elements (#696).
// That is a storage invariant, not a schema constraint: it holds whether
// JSON Schema validation is strict, report-only or not configured, and it is
// judged before the walk descends, where the container is still whole.
//
// The rule is positional. A path is an attribute's position when its dotted
// name is an attribute of the schema, whichever spelling reached it (nested
// keys or a literal dotted key) and however many arrays of objects lie
// around it: items[0].name is one, items is not. An element of an array at
// that position is the same path one array index deeper. Which indices are
// the arrays of objects around the attribute and which its own value's is
// the walk's, by the position the value takes: where a parent is an object
// in one alternative and an array of objects in another, items: {phones:
// [["1"]]} meets phones under no array of objects, so ["1"] is an element of
// phones, not phones itself (#696 review).
//
//   - An object, empty or not, is refused at every attribute's position: no
//     value type, list elements included, stores an object there.
//   - An array, empty or not, is refused at a non-list attribute's position
//     unless its schema document declares an array there
//     (SchemaShapes.Declared), in an array of objects or not. Metadata alone
//     cannot tell a single value from a primitive array recorded under its
//     element type before #204 (contact.phones as text), which stores its
//     elements at array indices and is still read and written that way. So
//     an attribute the document declares an array, in any branch of an
//     anyOf or oneOf, keeps its own array, and each element is judged at
//     its own position: an array where the document declares the element a
//     single value is refused there (phones: [["1"]] and phones: [[]]).
//     Where there is no document, or it does not reach the position or
//     leaves it open, nothing shows the attribute holds an array, and the
//     metadata says it holds one value: the array is refused too, so an
//     uncertain declaration never lets a scalar store array elements. A
//     pre-#204 attribute written through a registry without the document
//     needs its document served or its entry regenerated as a list. Its
//     stored records are not refused (recordsInPlace): an update that keeps
//     them writes them back. A list's own array and its elements keep their
//     rules (populateTypedValue, emptyList).
//
// Only the JSON shapes the walk decomposes (map[string]any and []any) are
// containers here. Any other Go value goes to the scalar conversion as
// before, which accepts what it supports ([]byte as text, a fmt.Stringer)
// and refuses the rest.

// scalarShapeRefusal answers the published refusal of value at path and
// indices when value is a JSON object or array the attribute at path cannot
// store there, or nil (see above). The first ancestors of indices are those
// of the arrays of objects the walk passed on its way to path; the rest
// index the attribute's own value.
func scalarShapeRefusal(path []string, value any, indices []int, ancestors int, cache forma.SchemaAttributeCache, shapes SchemaShapes) error {
	var isArray bool
	switch value.(type) {
	case map[string]any:
	case []any:
		isArray = true
	default:
		return nil
	}
	name := strings.Join(path, ".")
	meta, ok := cache[name]
	if !ok {
		return nil
	}
	expected := fmt.Sprintf("declared type %s takes a single value", meta.ValueType)
	switch {
	case meta.ValueType == forma.ValueTypeList:
		if isArray {
			return nil
		}
		expected = fmt.Sprintf("declared type list takes an array of %s values", meta.EffectiveItemsType())
	case isArray:
		declared := shapes.Declared(name)
		switch at := declared.At(ancestors, len(indices)-ancestors); {
		case at == ShapeArray:
			return nil
		case declared.At(ancestors, 0) == ShapeArray:
			// The attribute's own array is legal; this is one of its
			// elements.
			expected += fmt.Sprintf(" at array indices %q", joinIndices(indices))
		case at == ShapeUnresolved:
			expected += " (the schema document declares no array here)"
		}
	}
	return conversionRefusal(name, meta, fmt.Errorf(
		"%s, got %s; an object or array, empty or not, cannot be stored where a single value belongs "+
			"(to keep the current value, omit the key)",
		expected, containerKind(value)))
}

// containerKind describes a JSON object or array.
func containerKind(value any) string {
	switch v := value.(type) {
	case map[string]any:
		if len(v) == 0 {
			return "an empty object"
		}
		return "an object"
	case []any:
		switch len(v) {
		case 0:
			return "an empty array"
		case 1:
			return "an array of 1 element"
		default:
			return fmt.Sprintf("an array of %d elements", len(v))
		}
	default:
		return fmt.Sprintf("%T", value)
	}
}
