package transform

import (
	"fmt"

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
// The rule is judged on the value's text image (toString). The merged write
// takes that image again, so the two agree for any value whose image is a
// function of the value; a fmt.Stringer whose String() changes between calls
// is the exception (#773).
func CheckTextValues(data map[string]any, cache forma.SchemaAttributeCache) error {
	entries, err := walkDocument(0, uuid.Nil, data, cache)
	if err != nil {
		// The nesting cap, which the merged write refuses at its own stage.
		return nil
	}
	for _, entry := range entries {
		if entry.kind != entryClaim {
			continue
		}
		if err := checkClaimText(entry); err != nil {
			return fmt.Errorf("convert value for attribute '%s': %w", entry.name, err)
		}
	}
	return nil
}

// checkClaimText applies the text rule to a claim the write stores as text,
// and to nothing else.
func checkClaimText(entry documentEntry) error {
	meta := entry.meta
	if meta.ValueType == forma.ValueTypeList {
		elemMeta, err := listElementMeta(entry.tagged.record.ArrayIndices, entry.value, meta)
		if err != nil {
			return nil
		}
		meta = elemMeta
	}
	if meta.ValueType != forma.ValueTypeText || boundToSystemColumn(meta) {
		return nil
	}
	text, err := toString(entry.value)
	if err != nil {
		return nil
	}
	if err := sqlutil.CheckText(text); err != nil {
		return conversionRefusal(entry.name, meta, err)
	}
	return nil
}
