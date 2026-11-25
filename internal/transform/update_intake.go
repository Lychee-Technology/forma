package transform

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// CheckUpdateValues judges the values an update's Updates write with the
// write's own conversion (populateTypedValue): a malformed UUID, a fraction
// or an integer past the width of an integer type, a date outside the range
// its slot keeps, text PostgreSQL cannot hold (#679), a scalar sent for a
// list. An update converts its merged document only after it has locked and
// read the row, so a refusal there comes after SQL and poisons a Tx
// (docs/transactions.md). Each value in Updates reaches the merged document
// unchanged and at the same path, and its conversion depends only on the
// value and its attribute, so judging Updates first finds before any
// statement what the merged write would refuse after one, and nothing it
// would accept (#772).
//
// Two refusals come back, because they differ in when the caller raises
// them. The error is text PostgreSQL cannot hold, which an update has
// refused at intake since #679, whatever the row. CheckedUpdates.Refusal is
// the first other conversion refusal: the caller raises it where it knows
// the update writes Updates to a row, still before any write, and not where
// no row is written with them (an UpdateWhere or Claim taking no row, an
// Upsert that creates), which succeeded before #772 and still does. Both are
// the first in the write's sorted key order of their class.
//
// Only conversion moves here, and the shape rule (#696), which judges a
// value against its attribute alone just as conversion does: an object or
// array at an attribute holding a single value, a container as an element
// of an attribute's array (copyAt), or a single value inside more arrays
// than its declared array ancestry admits (convertLeaf) is a refusal of the
// second class. Any other position
// the walk refuses rather than claims keeps its stage, after the
// precondition and the row's existence are judged: an undefined attribute, a
// null, an [] nested in a list, the nesting cap. So does the required policy, which the merged write judges
// before it converts and which needs the row. The caller strips relation
// subtrees first, as the merge does, since they are never written.
//
// What passes is answered as the document to write in data's place (#773):
// a copy of data, which is never mutated, in which each value stored as text
// whose image came from a method call (carryTextImage) holds the image that
// was judged. The merged write then stores that image instead of taking it
// again, so a fmt.Stringer whose String() changes between calls is stored as
// checked and cannot be refused after the row lock. A value json.Marshal
// refuses is the exception: carryTextImage keeps it as sent, so with no
// validator, or under report-only validation, the write takes its image
// again after the lock and a changing image can still be refused there or
// stored as a different string. Every other conversion is a function of the
// value alone, so every other position holds the value as sent, a refused
// one included. A document past the nesting cap is answered unchanged: the
// merged write refuses it.
func CheckUpdateValues(data map[string]any, cache forma.SchemaAttributeCache) (CheckedUpdates, error) {
	if _, err := walkDocument(0, uuid.Nil, data, cache); err != nil {
		// The nesting cap, which the merged write refuses at its own stage.
		return CheckedUpdates{Values: data}, nil
	}
	// The walk has bounded the document, so the copy below terminates.
	in := &updateIntake{cache: cache}
	values, err := in.copyMap(nil, data, nil)
	if err != nil {
		return CheckedUpdates{}, err
	}
	return CheckedUpdates{Values: values, Refusal: in.refusal}, nil
}

// CheckedUpdates is what CheckUpdateValues answers for Updates that hold no
// unstorable text.
type CheckedUpdates struct {
	// Values is the document to write in Updates' place.
	Values map[string]any
	// Refusal is the first conversion refusal of a value in Updates, an
	// ErrInvalidInput, or nil. The caller raises it before it writes a row
	// with Values.
	Refusal error
}

// updateIntake copies a document the way walkDocument visits it, converting
// each position the walk claims and carrying the text image of each one
// stored as text. Keys are visited in the walk's sorted order, so the first
// refusal of each class is the one the write's conversion order meets first.
// refusal holds the first conversion refusal that is not unstorable text;
// the copy goes on past it, looking for unstorable text.
type updateIntake struct {
	cache   forma.SchemaAttributeCache
	refusal error
}

// copyValue dispatches one position of the copy. Errors pass up the copy
// unwrapped: convertLeaf names the attribute, which is the whole path, so a
// prefix per level would only repeat it.
func (in *updateIntake) copyValue(path []string, value any, indices []int) (any, error) {
	switch v := value.(type) {
	case map[string]any:
		return in.copyMap(path, v, indices)
	case []any:
		return in.copyArray(path, v, indices)
	default:
		return in.convertLeaf(path, v, indices)
	}
}

func (in *updateIntake) copyMap(path []string, src map[string]any, indices []int) (map[string]any, error) {
	if src == nil {
		return nil, nil
	}
	keys := make([]string, 0, len(src))
	for key := range src {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make(map[string]any, len(src))
	for _, key := range keys {
		value := src[key]
		if value == nil {
			// The write refuses a null at its own stage, or skips it.
			out[key] = nil
			continue
		}
		at := append(path, key)
		copied, err := in.copyAt(at, value, indices)
		if err != nil {
			return nil, err
		}
		out[key] = copied
	}
	return out, nil
}

// copyAt copies the value at path, the position a map key leads to, judging
// a container there by the walk's shape rule (documentWalk.visitAt): an
// object or array where the attribute holds a single value, or a container
// as an element of its array (elementShapeOf), is refused. Like a
// conversion, the refusal depends only on the value and its attribute, so it
// is a refusal of the second class (#696). The copy still descends into the
// container, so text PostgreSQL cannot hold inside it is refused as
// unconditionally as before (#679).
func (in *updateIntake) copyAt(path []string, value any, indices []int) (any, error) {
	if !isContainer(value) {
		return in.copyValue(path, value, indices)
	}
	name := strings.Join(path, ".")
	meta, ok := in.cache[name]
	if !ok {
		return in.copyValue(path, value, indices)
	}
	array, err := shapeAt(name, meta, value, indices)
	if err != nil {
		in.refuse(err)
		return in.copyValue(path, value, indices)
	}
	return in.copyElements(path, array, indices, meta)
}

// copyElements copies an array-valued attribute's array as copyArray does,
// judging each element as the walk does (documentWalk.visitElements). A nil
// array stays nil, which a JSON Schema validator reads as null, not as the
// empty-list marker an empty one writes.
func (in *updateIntake) copyElements(path []string, src []any, indices []int, meta forma.AttributeMetadata) ([]any, error) {
	if src == nil {
		return nil, nil
	}
	name := strings.Join(path, ".")
	out := make([]any, len(src))
	for i, item := range src {
		if item == nil {
			continue
		}
		at := append(slices.Clip(indices), i)
		var copied any
		var err error
		switch elementShapeOf(meta, item) {
		case elementRefused:
			in.refuse(containerError(name, meta, item, true, joinIndices(at)))
			copied, err = in.copyValue(path, item, at)
		case elementNested:
			copied, err = in.copyElements(path, item.([]any), at, meta)
		default:
			copied, err = in.copyValue(path, item, at)
		}
		if err != nil {
			return nil, err
		}
		out[i] = copied
	}
	return out, nil
}

// refuse records err unless an earlier refusal of its class is recorded.
func (in *updateIntake) refuse(err error) {
	if in.refusal == nil {
		in.refusal = err
	}
}

// copyArray copies an array element by element. An empty one is the
// empty-list marker, or an [] the walk refuses or ignores: none is a value
// to convert.
func (in *updateIntake) copyArray(path []string, src []any, indices []int) ([]any, error) {
	if src == nil {
		return nil, nil
	}
	out := make([]any, len(src))
	for i, item := range src {
		if item == nil {
			continue
		}
		copied, err := in.copyValue(path, item, append(indices, i))
		if err != nil {
			return nil, err
		}
		out[i] = copied
	}
	return out, nil
}

// convertLeaf converts a scalar the walk claims (visitLeaf) as the write
// converts it (flattenToAttributes), and answers what the copy holds in its
// place. A scalar at an undefined attribute is the walk's refusal, left to
// the write. Text PostgreSQL cannot hold is the error; any other refusal is
// recorded once, the first, and the value kept as sent.
func (in *updateIntake) convertLeaf(path []string, value any, indices []int) (any, error) {
	name := strings.Join(path, ".")
	meta, ok := in.cache[name]
	if !ok {
		return value, nil
	}
	if err := excessIndicesError(name, meta, indices); err != nil {
		in.refuse(err)
	}
	record := model.EAVRecord{ArrayIndices: joinIndices(indices)}
	_, err := populateTypedValue(&record, name, value, meta)
	text := storedAsText(meta, record.ArrayIndices, value) && record.ValueText != nil
	if err != nil {
		err = fmt.Errorf("convert value for attribute '%s': %w", name, err)
		if text && sqlutil.CheckText(*record.ValueText) != nil {
			return nil, err
		}
		in.refuse(err)
		return value, nil
	}
	if !text {
		return value, nil
	}
	return carryTextImage(value, *record.ValueText), nil
}

// storedAsText reports whether the write stores a claim at arrayIndices,
// which it has converted, as its text image (toString): a text attribute or
// a text list's element, unless bound to a system column, which the write
// drops.
func storedAsText(meta forma.AttributeMetadata, arrayIndices string, value any) bool {
	if meta.ValueType == forma.ValueTypeList {
		elemMeta, err := listElementMeta(arrayIndices, value, meta)
		if err != nil {
			return false
		}
		meta = elemMeta
	}
	return meta.ValueType == forma.ValueTypeText && !boundToSystemColumn(meta)
}
