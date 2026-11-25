package transform

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemavalidate"
)

// The array-placement rule (#696): where a value may sit inside arrays. The
// update intake applies it to Updates before any statement
// (CheckUpdateValues), and an update's write applies it to the document it
// persists (MergeUpdate), so the two refuse the same arrays whatever the
// validation mode, with or without a validator. A value is judged where the
// caller put it: per key of its path, the arrays around that key's value. A
// value in no array is a single value, which no rule refuses.
//
// Two rules hold together.
//
//   - Storage must read the value back where it was written. A record keeps
//     one array index per array around its value, and nothing of which key
//     each sat at, so the read assigns them: to the attribute's array
//     ancestors, outermost first, then to the attribute's own array
//     (rebuildByAncestry), or, without ancestry, to the attribute or its
//     parent. Two arrays at one key are therefore never read back, and
//     where the registry derives the ancestry, the arrays around the path's
//     keys must be its first array ancestors, in order, with an array
//     around the attribute itself only once every ancestor has one
//     (representable), with or without a skeleton.
//   - The schema must declare those arrays there. Its container skeleton
//     (schemavalidate.ShapeSchema) says so when the validator keeps one.
//     Without one, the ledger is all there is: an attribute that is not a
//     list stores single values, so an array around it is refused, even a
//     primitive array the schema declares under an element-typed (pre-#204)
//     ledger entry, which only the schema could admit.
//
// An empty array at a list is its empty-list marker, which the write stores
// with the indices of the arrays around the list, not inside it: the list is
// judged where it sits, and an [] inside the list's own value is refused
// (nestedEmptyListError). Elsewhere an empty array, and an array whose
// elements are all null, is judged as the arrays around a value inside it,
// since no element of it reaches the rule on its own (#696 review R1-F6).

// docPosition is where a walk of a document is: the array indices around it,
// and per key of its path the arrays around that key's value.
type docPosition struct {
	indices []int
	arrays  []int
}

// under answers the position of a value one key below p.
func (p docPosition) under() docPosition {
	return docPosition{indices: p.indices, arrays: append(slices.Clone(p.arrays), 0)}
}

// inside answers the position of element i of the array at p.
func (p docPosition) inside(i int) docPosition {
	arrays := slices.Clone(p.arrays)
	if len(arrays) > 0 {
		arrays[len(arrays)-1]++
	}
	return docPosition{indices: append(slices.Clone(p.indices), i), arrays: arrays}
}

// placementJudge applies the rule for one schema. shape is its skeleton, nil
// without a validator; verdicts of the skeleton are memoized per
// arrangement.
type placementJudge struct {
	cache  forma.SchemaAttributeCache
	shape  *schemavalidate.ShapeSchema
	judged map[string]error
}

func newPlacementJudge(cache forma.SchemaAttributeCache, shape *schemavalidate.ShapeSchema) *placementJudge {
	return &placementJudge{cache: cache, shape: shape, judged: make(map[string]error)}
}

// value judges a single value of the attribute at path, sitting at at. sent
// says what the caller sent there, for the message.
func (j *placementJudge) value(path []string, meta forma.AttributeMetadata, at docPosition, sent string) error {
	if len(at.indices) == 0 {
		return nil
	}
	segments, arrays := arrangementOf(path, at.arrays)
	name := strings.Join(path, ".")
	if detail := representable(segments, arrays, meta); detail != nil {
		return arrayStorageError(name, meta, sent, detail)
	}
	if j.shape == nil {
		if arrays[len(arrays)-1] > 0 && meta.ValueType != forma.ValueTypeList {
			return arrayPlacementError(name, meta, sent,
				errors.New("no container skeleton: the ledger declares a single value, and an array around it is refused"))
		}
		return nil
	}
	key := arrangementKey(segments, arrays)
	verdict, seen := j.judged[key]
	if !seen {
		verdict = j.shape.AdmitsArrangement(segments, arrays)
		j.judged[key] = verdict
	}
	if verdict != nil {
		return arrayPlacementError(name, meta, sent, fmt.Errorf("container skeleton verdict: %w", verdict))
	}
	return nil
}

// array judges an array at the position of an attribute that takes single
// values (singleValueAttribute), before its elements are visited.
func (j *placementJudge) array(path []string, meta forma.AttributeMetadata, src []any, at docPosition) error {
	kind := sentAt(containerKind(src), at.indices)
	if meta.ValueType == forma.ValueTypeList {
		if len(src) > 0 {
			return nil
		}
		if at.arrays[len(at.arrays)-1] > 0 {
			return nestedEmptyListError(strings.Join(path, "."), meta, at.indices)
		}
		return j.value(path, meta, at, kind)
	}
	if !allNil(src) {
		// Each element reaches the rule, or the object rule, on its own.
		return nil
	}
	return j.value(path, meta, at.inside(0), kind)
}

// allNil reports an array none of whose elements is a value: empty, or
// nulls only.
func allNil(src []any) bool {
	for _, item := range src {
		if item != nil {
			return false
		}
	}
	return true
}

// arrangementKey names an arrangement unambiguously for the memo: each
// segment quoted, since a segment may hold a space, a dot or a bracket of
// its own (#696 review R2-F2: ["a", "b c"] and ["a b", "c"] are two
// paths).
func arrangementKey(segments []string, arrays []int) string {
	var b strings.Builder
	for i, segment := range segments {
		b.WriteString(strconv.Quote(segment))
		b.WriteByte(':')
		b.WriteString(strconv.Itoa(arrays[i]))
		b.WriteByte(' ')
	}
	return b.String()
}

// representable answers why storage would not read the value back where it
// sits, or nil (see the rule above). segments and arrays are its dotted path
// and the arrays around each segment's value.
//
// Two arrays at one key are never read back. Where the registry derives the
// attribute's array ancestry, the read places the record's indices by it
// (rebuildByAncestry), so the arrays around the path's keys must be its
// first array ancestors, in order, and an array around the attribute itself
// needs one around every ancestor. That holds with a skeleton as without
// one: the skeleton says what the schema declares, and this says what the
// read places back (#696 review R3-F2: an object-or-array union the registry
// derived as an object took its array form, which then read back with the
// array around each member). The registry derives such a union an array of
// objects, so its array form is placed back inside it, and its object form,
// whose record has fewer indices than ancestors, reads that array as an
// object (#696 review R2-F3). Without ancestry the read infers each
// record's array from the row, and nothing is held to it here.
func representable(segments []string, arrays []int, meta forma.AttributeMetadata) error {
	for i, n := range arrays {
		if n > 1 {
			return fmt.Errorf("%d arrays around %q: a record keeps one index per array and the read takes each for a different array",
				n, strings.Join(segments[:i+1], "."))
		}
	}
	if meta.ArrayAncestors == nil {
		return nil
	}
	filled := 0
	for i, n := range arrays[:len(arrays)-1] {
		if n == 0 {
			continue
		}
		at := strings.Join(segments[:i+1], ".")
		if filled >= len(meta.ArrayAncestors) || meta.ArrayAncestors[filled] != at {
			return fmt.Errorf("an array around %q: the read places the record's indices in the array ancestors %v, outermost first",
				at, meta.ArrayAncestors)
		}
		filled++
	}
	if arrays[len(arrays)-1] > 0 && filled < len(meta.ArrayAncestors) {
		return fmt.Errorf("an array around the attribute beneath %q, which is not one: the read places the record's first index there",
			meta.ArrayAncestors[filled])
	}
	return nil
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

// arrayPlacementError refuses a value inside arrays the schema does not
// declare around it. The verdict names schema locations and is an
// operator's detail.
func arrayPlacementError(name string, meta forma.AttributeMetadata, sent string, verdict error) error {
	return forma.WithOperatorDetail(forma.InvalidInputf(
		"invalid value for attribute '%s' (attrID=%d): declared type %s takes %s, got %s, inside arrays the schema does not declare there; an object or array, empty or not, cannot be stored where a single value belongs (omit the key to keep the current value)",
		name, meta.AttributeID, meta.ValueType, takes(meta), sent), verdict)
}

// arrayStorageError refuses a value inside arrays storage would not read it
// back from.
func arrayStorageError(name string, meta forma.AttributeMetadata, sent string, detail error) error {
	return forma.WithOperatorDetail(forma.InvalidInputf(
		"invalid value for attribute '%s' (attrID=%d): declared type %s takes %s, got %s, inside arrays its stored record would not read back from; an object or array, empty or not, cannot be stored where a single value belongs (omit the key to keep the current value)",
		name, meta.AttributeID, meta.ValueType, takes(meta), sent), detail)
}
