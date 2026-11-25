package transform

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/lychee-technology/forma"
)

// The scalar-shape storage invariant (#924): an update never gives an
// attribute whose value type is a scalar an object or an array. The document
// walk takes such a container apart rather than storing it, so what a
// report-only update stored depended on the binding: {} and [] kept a
// column-bound value and removed an EAV one (and released its unique-key
// entry), ["a"] stored element 0 under a scalar, {"x": …} was refused as an
// undefined attribute. Strict validation refuses all of them, but update
// validation is report-only by default and absent without a validator, so the
// write path enforces the shape itself, whatever the validation mode.
//
// The rule reads the registry's attribute metadata only (ValueType,
// ArrayAncestors): it does not parse or consult the JSON Schema document.
// Where the metadata cannot place a value, the position is left as it was
// (scalarShapePlaced), and #923 owns those cases.

// scalarShapeRefusal answers the refusal of value at the attribute name
// addresses, or nil when the position is not a scalar attribute holding a
// container the metadata places. arrays are the dotted paths of the arrays
// the document crossed to reach the position, outermost first.
func scalarShapeRefusal(name string, value any, arrays []string, cache forma.SchemaAttributeCache) error {
	meta, ok := cache[name]
	if !ok || meta.ValueType == forma.ValueTypeList {
		return nil
	}
	kind, ok := containerKind(value)
	if !ok || !scalarShapePlaced(name, meta, arrays) {
		return nil
	}
	return forma.InvalidInputf(
		"invalid value for attribute '%s' (attrID=%d): expected a scalar of type %s, got %s; an object or array is never stored at a scalar attribute, and an empty one does not clear it",
		name, meta.AttributeID, meta.ValueType, kind)
}

// scalarShapePlaced reports whether the metadata says a value reached across
// arrays is the attribute's own value rather than one filling an array level
// above it. The walk gives the i-th array it crosses the i-th index, and the
// read places that index in the attribute's i-th declared array of objects
// (ArrayAncestors). So a value is the attribute's own once every ancestor is
// filled, each by one array crossed, and by no more: the arrays crossed are
// as many as the ancestors, and each is the ancestor it fills or, in a dotted
// spelling, a key beneath it. {"items.sku": [...]} crosses an array named
// items.sku, whose index fills items, so the array itself fills the ancestor
// and is not placed, but {"items.sku": [[...]]}'s inner array is items.sku's
// own value. An array crossed above its ancestor ([[{"sku": ...}]], a level
// the schema does not declare) places nothing beneath it, and neither does
// any position of an attribute whose ArrayAncestors the registry did not
// derive, short of a top-level attribute reached across no array, which has
// no ancestor to place.
func scalarShapePlaced(name string, meta forma.AttributeMetadata, arrays []string) bool {
	if meta.ArrayAncestors == nil {
		return len(arrays) == 0 && !strings.Contains(name, ".")
	}
	if len(arrays) != len(meta.ArrayAncestors) {
		return false
	}
	for i, crossed := range arrays {
		ancestor := meta.ArrayAncestors[i]
		if crossed != ancestor && !strings.HasPrefix(crossed, ancestor+".") {
			return false
		}
	}
	return true
}

// containerKind reports whether value is an object or an array, and names it
// for the refusal. map[string]any and []any are the JSON forms. Any other Go
// map, slice or array is a container too, except the representations the
// write converts as one scalar: []byte itself, which is text or a UUID's
// bytes, and a value that formats itself (a fmt.Stringer, such as uuid.UUID
// or net.IP, an error, or a fmt.Formatter), whose own text is what is
// stored. Any other byte slice or array, json.RawMessage or [16]byte, is
// formatted element by element ("[91 34 97 34 93]"), so it is a container,
// as a [2]string is.
func containerKind(value any) (string, bool) {
	switch v := value.(type) {
	case map[string]any:
		return sizedKind("object", len(v)), true
	case []any:
		return sizedKind("array", len(v)), true
	case []byte, fmt.Stringer, error, fmt.Formatter:
		return "", false
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Map:
		return sizedKind("object", rv.Len()), true
	case reflect.Slice, reflect.Array:
		return sizedKind("array", rv.Len()), true
	default:
		return "", false
	}
}

func sizedKind(kind string, n int) string {
	if n == 0 {
		return "an empty " + kind
	}
	return "an " + kind
}

// checkMergedScalarShapes refuses, as caller input, the first position of an
// update's merged document, in sorted key order, where a scalar attribute
// holds an object or an array the update brought (scalarShapeRefusal).
// resolveStoredValues runs it once its walk has bounded the document and
// before any stored value is decoded, so nothing is written. The same rule
// has already run on Updates at intake (CheckUpdateValues); this is the
// guarantee for what reaches the persisted document by any route.
//
// A container holding a stored value is the stored row's, not the update's:
// the rebuild produces one under a scalar only for records written that way
// before this rule (a pre-#204 attributes file, or report-only validation
// admitting ["a"]), and an update that does not address it writes it back as
// stored, as it always has. A caller's value replaces such a container whole
// (mergeMaps), so a container is never partly the caller's and partly the
// stored row's. Finding and repairing those rows is #922's.
func checkMergedScalarShapes(data map[string]any, cache forma.SchemaAttributeCache) error {
	return findScalarShape(nil, data, nil, cache)
}

// findScalarShape is checkMergedScalarShapes' walk, visiting data in
// walkDocument's order.
func findScalarShape(path []string, value any, arrays []string, cache forma.SchemaAttributeCache) error {
	if len(path) > 0 {
		if err := scalarShapeRefusal(strings.Join(path, "."), value, arrays, cache); err != nil && !holdsStoredValue(value) {
			return err
		}
	}
	switch v := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if err := findScalarShape(append(path[:len(path):len(path)], key), v[key], arrays, cache); err != nil {
				return err
			}
		}
	case []any:
		crossed := append(arrays[:len(arrays):len(arrays)], strings.Join(path, "."))
		for _, item := range v {
			if err := findScalarShape(path, item, crossed, cache); err != nil {
				return err
			}
		}
	}
	return nil
}

// holdsStoredValue reports whether a stored value is held anywhere in value.
func holdsStoredValue(value any) bool {
	switch v := value.(type) {
	case *storedValue:
		return true
	case map[string]any:
		for _, item := range v {
			if holdsStoredValue(item) {
				return true
			}
		}
	case []any:
		for _, item := range v {
			if holdsStoredValue(item) {
				return true
			}
		}
	}
	return false
}
