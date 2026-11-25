package transform

import (
	"fmt"
	"slices"
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
//     is the schema's to say, so an update's intake asks its container
//     skeleton (schemavalidate.ShapeSchema) about each value it writes, with
//     the arrays the caller put around it, at the segment where the caller
//     put them, whatever the spelling (judgeArrays). An empty array is
//     judged as the arrays around a value inside it. Without a skeleton
//     nothing declares an array, and an array at an attribute that is not a
//     list is refused (arrayAtSingleValueError).
//
// An attribute whose name is also the parent of another attribute is left to
// the walk as before: its position holds an object legitimately.

// intakePosition is where the update intake is in a document: the array
// indices around it, and per key of its path the arrays around that key's
// value.
type intakePosition struct {
	indices []int
	arrays  []int
}

// under answers the position of a value one key below p.
func (p intakePosition) under() intakePosition {
	return intakePosition{indices: p.indices, arrays: append(slices.Clone(p.arrays), 0)}
}

// inside answers the position of element i of the array at p.
func (p intakePosition) inside(i int) intakePosition {
	arrays := slices.Clone(p.arrays)
	if len(arrays) > 0 {
		arrays[len(arrays)-1]++
	}
	return intakePosition{indices: append(slices.Clone(p.indices), i), arrays: arrays}
}

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

// judgeArray judges an array at an attribute's own position. With a
// skeleton an empty one is judged as the arrays around a value inside it,
// and a non-empty one by the values it holds. Without one, nothing declares
// an array at an attribute that is not a list.
func (in *updateIntake) judgeArray(path []string, src []any, at intakePosition) {
	name := strings.Join(path, ".")
	meta, ok := singleValueAttribute(name, in.cache)
	if !ok {
		return
	}
	if in.shape == nil {
		if meta.ValueType != forma.ValueTypeList {
			in.refuse(arrayAtSingleValueError(name, meta, src))
		}
		return
	}
	if len(src) == 0 {
		in.judgeArrays(path, meta, at.inside(0), sentAt(containerKind(src), at.indices))
	}
}

// judgeArrays refuses a value of the attribute at path, sitting at at,
// where the skeleton admits no value inside the arrays the caller put around
// it, there. sent says what the caller sent, for the message. A value in no
// array is a single value, which the skeleton never refuses.
func (in *updateIntake) judgeArrays(path []string, meta forma.AttributeMetadata, at intakePosition, sent string) {
	if in.shape == nil || len(at.indices) == 0 {
		return
	}
	segments, arrays := arrangementOf(path, at.arrays)
	key := fmt.Sprint(segments, arrays)
	verdict, seen := in.judged[key]
	if !seen {
		verdict = in.shape.AdmitsArrangement(segments, arrays)
		in.judged[key] = verdict
	}
	if verdict != nil {
		in.refuse(arrayPlacementError(strings.Join(path, "."), meta, sent, verdict))
	}
}

// sentAt names a value the caller sent at indices, for a message.
func sentAt(kind string, indices []int) string {
	if len(indices) == 0 {
		return kind
	}
	return fmt.Sprintf("%s at array indices %q", kind, joinIndices(indices))
}

// arrangementOf answers the dotted segments of path, whose keys may be
// literal dotted names, and the arrays around each segment's value: a key's
// arrays sit around its last segment, since a literal key "a.b" holding an
// array places it where {"a": {"b": [...]}} does.
func arrangementOf(path []string, arrays []int) ([]string, []int) {
	var segments []string
	var around []int
	for i, key := range path {
		parts := strings.Split(key, ".")
		segments = append(segments, parts...)
		around = append(around, make([]int, len(parts)-1)...)
		around = append(around, arrays[i])
	}
	return segments, around
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

// arrayAtSingleValueError refuses an array at an attribute that is not a
// list, where no skeleton says the schema declares one.
func arrayAtSingleValueError(name string, meta forma.AttributeMetadata, value []any) error {
	return forma.InvalidInputf(
		"invalid value for attribute '%s' (attrID=%d): declared type %s takes %s, got %s; an array, empty or not, cannot be stored where a single value belongs (omit the key to keep the current value)",
		name, meta.AttributeID, meta.ValueType, takes(meta), containerKind(value))
}

// arrayPlacementError refuses a value inside arrays the schema's skeleton
// does not admit around it. The verdict names schema locations and is an
// operator's detail.
func arrayPlacementError(name string, meta forma.AttributeMetadata, sent string, verdict error) error {
	return forma.WithOperatorDetail(forma.InvalidInputf(
		"invalid value for attribute '%s' (attrID=%d): declared type %s takes %s, got %s, inside arrays the schema does not declare there; an object or array, empty or not, cannot be stored where a single value belongs (omit the key to keep the current value)",
		name, meta.AttributeID, meta.ValueType, takes(meta), sent),
		fmt.Errorf("container skeleton verdict: %w", verdict))
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
