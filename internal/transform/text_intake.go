package transform

import (
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// CheckTextValues refuses a text value data writes that PostgreSQL text
// cannot hold (sqlutil.CheckText), a text attribute's or a text list's
// element, with the refusal the write makes (populateTypedValue). An update
// converts its merged document only after it has locked and read the row, so
// a refusal there comes after SQL and poisons a Tx (docs/transactions.md).
// Each value in Updates reaches the merged document unchanged and at the
// same path, so judging Updates first refuses before any statement what the
// merged write would refuse after one (#679).
//
// Only that rule moves here. Every other refusal the write makes on Updates
// keeps its stage, after the precondition and the row's existence are
// judged, so an update refused for another reason keeps its outcome: a
// scalar or a nested array sent for a list (listElementMeta), the nesting
// cap, an undefined attribute, a null. A position the write does not store as
// text is not judged: another value type, or an alias of a system column,
// which the write drops. The caller strips relation subtrees first, as the
// merge does, since they are never written.
//
// The rule is judged on the value's text image (toString), and what passes
// is answered as the document to write in data's place (#773): a copy of
// data, which is never mutated, in which each judged value whose image came
// from a method call (carryTextImage) holds the image that was judged. The
// merged write then stores that image instead of taking it again, so a
// fmt.Stringer whose String() changes between calls is stored as checked
// and cannot be refused after the row lock. A document past the nesting cap
// is answered unchanged: the merged write refuses it.
func CheckTextValues(data map[string]any, cache forma.SchemaAttributeCache) (map[string]any, error) {
	if _, err := walkDocument(0, uuid.Nil, data, cache); err != nil {
		// The nesting cap, which the merged write refuses at its own stage.
		return data, nil
	}
	// The walk has bounded the document, so the copy below terminates.
	return textIntake{cache: cache}.copyMap(nil, data, nil)
}

// textIntake copies a document the way walkDocument visits it, judging and
// carrying each position the write stores as text. Keys are visited in the
// walk's sorted order, so the first refusal is the one the walk's order
// meets first.
type textIntake struct {
	cache forma.SchemaAttributeCache
}

func (in textIntake) copyValue(path []string, value any, indices []int) (any, error) {
	switch v := value.(type) {
	case map[string]any:
		return in.copyMap(path, v, indices)
	case []any:
		return in.copyArray(path, v, indices)
	default:
		return in.checkLeaf(path, v, indices)
	}
}

func (in textIntake) copyMap(path []string, src map[string]any, indices []int) (map[string]any, error) {
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

func (in textIntake) copyArray(path []string, src []any, indices []int) ([]any, error) {
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

// checkLeaf judges a scalar the walk would claim (visitLeaf) when the write
// stores it as text, and answers what the copy holds in its place.
func (in textIntake) checkLeaf(path []string, value any, indices []int) (any, error) {
	name := strings.Join(path, ".")
	meta, ok := in.cache[name]
	if !ok {
		return value, nil
	}
	textMeta, storedAsText := textSlotMeta(meta, joinIndices(indices), value)
	if !storedAsText {
		return value, nil
	}
	text, err := toString(value)
	if err != nil {
		return value, nil
	}
	if err := sqlutil.CheckText(text); err != nil {
		return nil, fmt.Errorf("convert value for attribute '%s': %w", name, conversionRefusal(name, textMeta, err))
	}
	return carryTextImage(value, text), nil
}

// textSlotMeta answers the metadata a claim at arrayIndices is converted by
// when the write stores it as text, and false for any other claim.
func textSlotMeta(meta forma.AttributeMetadata, arrayIndices string, value any) (forma.AttributeMetadata, bool) {
	if meta.ValueType == forma.ValueTypeList {
		elemMeta, err := listElementMeta(arrayIndices, value, meta)
		if err != nil {
			return meta, false
		}
		meta = elemMeta
	}
	if meta.ValueType != forma.ValueTypeText || boundToSystemColumn(meta) {
		return meta, false
	}
	return meta, true
}
