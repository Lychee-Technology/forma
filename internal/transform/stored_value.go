package transform

import (
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// storedValue is a stored attribute value an update's merge carries without
// decoding it. The merge base holds one per stored record that has a value;
// only the ones the written row keeps are decoded (resolveStoredValues), so a
// stored value the read refuses, such as a bigint image past int64 (#590),
// blocks the updates that keep it and never the one that replaces it.
//
// It is a leaf to everything that handles the merge base: mergeMaps,
// StripComputedFields and the document walk place it, move it, or drop it
// like any scalar, and it never leaves the transform package outside that
// base.
type storedValue struct {
	record    model.EAVRecord
	valueType forma.ValueType
	attrName  string
}

// decode converts the stored value exactly as the read path does, errors
// included: FromPersistentRecord over the same row raises the same error.
func (v *storedValue) decode() (any, error) {
	return decodeRecordValue(v.record, v.attrName, v.valueType)
}

// hasStoredValue reports whether a record carries a value to decode. A record
// with every value column NULL decodes to nil for every value type, so there
// is nothing to hold: it is an empty-list marker or nothing, on read and on
// the merge base alike.
func hasStoredValue(record model.EAVRecord) bool {
	return record.ValueText != nil || record.ValueNumeric != nil || record.ValueInt64 != nil
}

// decodeEAVRecord is the read path's value decode: an extraction failure is a
// consistency error, plain and operator-visible, named with the record's full
// identity (#405), since this is the hop that holds the record.
func decodeEAVRecord(record model.EAVRecord, valueType forma.ValueType) (any, error) {
	value, err := extractValueFromEAVRecord(record, valueType)
	if err != nil {
		return nil, fmt.Errorf("record %s: %w", eavRecordIdentity(record), err)
	}
	return value, nil
}
