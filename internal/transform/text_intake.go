package transform

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// CheckTextValues converts every text value data writes, a text attribute's
// or a text list's element, exactly as the write converts it
// (populateTypedValue), and returns the first refusal. An update converts its
// merged document only after it has locked and read the row, so a refusal
// there comes after SQL and poisons a Tx (docs/transactions.md). Each value
// in Updates reaches the merged document unchanged and at the same path, and
// its conversion depends on nothing but the value and its attribute, so
// judging Updates first refuses before any statement what the merged write
// would refuse after one (#679).
//
// Only text is judged here: a text value PostgreSQL cannot store is the one
// refusal that used to reach the database. Other conversions keep their
// place in the merged write. Positions the write refuses for another reason
// (an undefined attribute) are left to it. The caller strips relation
// subtrees first, as the merge does, since they are never written.
func CheckTextValues(data map[string]any, cache forma.SchemaAttributeCache) error {
	entries, err := walkDocument(0, uuid.Nil, data, cache)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.kind != entryClaim || !writesText(entry.meta) {
			continue
		}
		record := entry.tagged.record
		if _, err := populateTypedValue(&record, entry.name, entry.value, entry.meta); err != nil {
			return fmt.Errorf("convert value for attribute '%s': %w", entry.name, err)
		}
	}
	return nil
}

// writesText reports the attributes whose claims convert to a caller's text:
// text itself and a list whose elements are text.
func writesText(meta forma.AttributeMetadata) bool {
	if meta.ValueType == forma.ValueTypeList {
		return meta.EffectiveItemsType() == forma.ValueTypeText
	}
	return meta.ValueType == forma.ValueTypeText
}
