package transform

import (
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// CheckUpdateValues refuses a value an update's Updates write that the write
// cannot convert, with the refusal the write makes (populateTypedValue): a
// malformed UUID, a fraction or an integer past the width of an integer
// type, a date outside the range its slot keeps, text PostgreSQL cannot hold
// (#679), a scalar sent for a list. An update converts its merged document
// only after it has locked and read the row, so a refusal there comes after
// SQL and poisons a Tx (docs/transactions.md). Each value in Updates reaches
// the merged document unchanged and at the same path, and its conversion
// depends only on the value and its attribute, so judging Updates first
// refuses before any statement what the merged write would refuse after one,
// and refuses nothing it would accept (#772).
//
// Only conversion moves here. A position the walk refuses rather than
// claims keeps its stage, after the precondition and the row's existence are
// judged: an undefined attribute, a null, an [] nested in a list, the
// nesting cap. So does the required policy, which the merged write judges
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
// value alone, so every other position holds the value as sent. A document
// past the nesting cap is answered unchanged: the merged write refuses it.
func CheckUpdateValues(data map[string]any, cache forma.SchemaAttributeCache) (map[string]any, error) {
	if _, err := walkDocument(0, uuid.Nil, data, cache); err != nil {
		// The nesting cap, which the merged write refuses at its own stage.
		return data, nil
	}
	// The walk has bounded the document, so the copy below terminates.
	return updateIntake{cache: cache}.copyMap(nil, data, nil)
}

// updateIntake copies a document the way walkDocument visits it, converting
// each position the walk claims and carrying the text image of each one
// stored as text. Keys are visited in the walk's sorted order, so the first
// refusal is the one the write's conversion order meets first.
type updateIntake struct {
	cache forma.SchemaAttributeCache
}

func (in updateIntake) copyValue(path []string, value any, indices []int) (any, error) {
	switch v := value.(type) {
	case map[string]any:
		return in.copyMap(path, v, indices)
	case []any:
		return in.copyArray(path, v, indices)
	default:
		return in.convertLeaf(path, v, indices)
	}
}

func (in updateIntake) copyMap(path []string, src map[string]any, indices []int) (map[string]any, error) {
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
		copied, err := in.copyValue(append(path, key), value, indices)
		if err != nil {
			return nil, err
		}
		out[key] = copied
	}
	return out, nil
}

// copyArray copies an array element by element. An empty one is the
// empty-list marker, or an [] the walk refuses or ignores: none is a value
// to convert.
func (in updateIntake) copyArray(path []string, src []any, indices []int) ([]any, error) {
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
// the write.
func (in updateIntake) convertLeaf(path []string, value any, indices []int) (any, error) {
	name := strings.Join(path, ".")
	meta, ok := in.cache[name]
	if !ok {
		return value, nil
	}
	record := model.EAVRecord{ArrayIndices: joinIndices(indices)}
	if _, err := populateTypedValue(&record, name, value, meta); err != nil {
		return nil, fmt.Errorf("convert value for attribute '%s': %w", name, err)
	}
	if !storedAsText(meta, record.ArrayIndices, value) || record.ValueText == nil {
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
