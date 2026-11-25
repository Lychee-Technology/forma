package transform

import (
	"encoding"
	"fmt"
	"reflect"
	"strings"

	"github.com/lychee-technology/forma"
)

// The shape of a written value is a storage invariant, enforced whatever the
// validation mode (#696). An attribute stores single values: one, or, for a
// list, one per element, each as a record with one array index per array it
// sits in. Two rules follow.
//
//   - An object at an attribute's own position is never stored: the walk
//     takes its keys for attributes beneath it, which the schema does not
//     define, or, when it is empty, writes nothing at all. So an object
//     there, empty or not, directly or inside an array, is refused
//     (objectAtAttributeError). The ledger alone says so, and the write and
//     the update intake both apply it, so it holds for every document the
//     write stores.
//   - A value inside arrays is stored at their indices, which is right only
//     where the schema declares those arrays: arrays of objects around it,
//     a list's own array, or a primitive array the schema declares under an
//     element-typed (pre-#204) ledger entry. Which arrays the schema declares
//     is the schema's to say, and where storage reads the value back from is
//     the read's: the array-placement rule (value_placement.go) judges both,
//     on each value with the arrays the caller put around it, at the
//     segment where the caller put them, whatever the spelling. The update
//     intake and an update's write both apply it.
//
// A Go map, slice or array the caller sends where a single value belongs is
// an object or array too, though the walk does not take it apart: the
// conversion refuses it (goContainerKind) rather than store its formatted
// image. A Go value that is a single value in a container's clothing is
// kept: a []byte is text, a uuid.UUID is a UUID, and any fmt.Stringer or
// encoding.TextMarshaler converts as it always has.
//
// An attribute whose name is also the parent of another attribute is left to
// the walk as before: its position holds an object legitimately.

// judgeObject refuses an object at an attribute's own position.
func (in *updateIntake) judgeObject(path []string, src map[string]any) {
	if len(path) == 0 {
		return
	}
	name := strings.Join(path, ".")
	if meta, ok := singleValueAttribute(name, in.cache); ok {
		in.refuse(objectAtAttributeError(name, meta, src))
	}
}

// judgeArray judges an array at an attribute's own position by the
// array-placement rule (placementJudge.array).
func (in *updateIntake) judgeArray(path []string, src []any, at docPosition) {
	if meta, ok := singleValueAttribute(strings.Join(path, "."), in.cache); ok {
		if err := in.placement.array(path, meta, src, at); err != nil {
			in.refuse(err)
		}
	}
}

// singleValueAttribute answers the attribute at name when its position takes
// single values only: an attribute of the schema that is no other attribute's
// parent.
func singleValueAttribute(name string, cache forma.SchemaAttributeCache) (forma.AttributeMetadata, bool) {
	meta, ok := cache[name]
	if !ok {
		return forma.AttributeMetadata{}, false
	}
	prefix := name + "."
	for other := range cache {
		if strings.HasPrefix(other, prefix) {
			return forma.AttributeMetadata{}, false
		}
	}
	return meta, true
}

// objectAtAttributeError refuses an object at attribute name's own position.
func objectAtAttributeError(name string, meta forma.AttributeMetadata, value map[string]any) error {
	return forma.InvalidInputf(
		"invalid value for attribute '%s' (attrID=%d): declared type %s takes %s, got %s; an object, empty or not, cannot be stored where a single value belongs (omit the key to keep the current value)",
		name, meta.AttributeID, meta.ValueType, takes(meta), containerKind(value))
}

// takes says what an attribute's position takes.
func takes(meta forma.AttributeMetadata) string {
	if meta.ValueType == forma.ValueTypeList {
		return fmt.Sprintf("an array of single %s values", meta.EffectiveItemsType())
	}
	return "a single value"
}

// containerKind names a container for a message; a typed nil one is the
// empty container the walk takes it for.
func containerKind(value any) string {
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
	return "a value"
}

// goContainerKind names the container a Go value is, when it is one: a map,
// or a slice or array, other than a []byte (text) and anything that renders
// itself as a single value (fmt.Stringer, encoding.TextMarshaler, as a
// uuid.UUID does). A pointer is judged by what it points to. []any and
// map[string]any never reach it: the walk takes them apart.
func goContainerKind(value any) (string, bool) {
	if value == nil {
		return "", false
	}
	switch value.(type) {
	case fmt.Stringer, encoding.TextMarshaler:
		return "", false
	}
	v := reflect.ValueOf(value)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return "", false
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Map:
		return "map", true
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return "", false
		}
		return "slice", true
	case reflect.Array:
		return "array", true
	}
	return "", false
}

// goContainerError refuses a Go container where meta takes single values.
func goContainerError(meta forma.AttributeMetadata, kind string, value any) error {
	return fmt.Errorf("declared type %s takes %s, got a Go %s (%T); an object or array, empty or not, cannot be stored where a single value belongs (send a list's elements as []any)",
		meta.ValueType, takes(meta), kind, value)
}
