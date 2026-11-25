package transform

import (
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// StoredAttributeValue answers the value record holds for the scalar
// attribute meta describes, decoded exactly as a read decodes it: from its
// bound column, encoding included, or from its EAV record. It answers nil
// when the record holds no value, and an error when the attribute's values
// are array elements, which have no single value. The unique-key write path
// (#651) computes key values with it, so a key compares what a read of the
// row would answer. The error for array elements is an *ArrayElementsError,
// so a write can tell the caller which value it cannot key (#696 review
// R4-F2).
func StoredAttributeValue(record *model.PersistentRecord, meta forma.AttributeMetadata) (any, error) {
	if record == nil {
		return nil, fmt.Errorf("record cannot be nil")
	}
	if meta.ColumnBinding != nil {
		// The main-column readers keep no state of their own.
		attr, err := (&persistentRecordTransformer{}).readFromMainColumn(record, meta, meta.ColumnBinding)
		if err != nil {
			return nil, fmt.Errorf("read attribute %d of row %s from %s: %w", meta.AttributeID, record.RowID, meta.ColumnBinding.ColumnName, err)
		}
		if attr == nil {
			return nil, nil
		}
		return decodeEAVRecord(*attr, meta.ValueType)
	}
	var value *model.EAVRecord
	for i, attr := range record.OtherAttributes {
		if attr.AttrID != meta.AttributeID {
			continue
		}
		if attr.ArrayIndices != "" {
			return nil, &ArrayElementsError{AttrID: meta.AttributeID, Row: record.RowID.String(), Indices: attr.ArrayIndices}
		}
		value = &record.OtherAttributes[i]
	}
	if value == nil {
		return nil, nil
	}
	return decodeEAVRecord(*value, meta.ValueType)
}

// ArrayElementsError is StoredAttributeValue's answer for an attribute whose
// record carries array indices: it holds one value per element, not one per
// row. A unique key over a member of an object-or-array union meets it when
// the row writes the union in its array form.
type ArrayElementsError struct {
	AttrID  int16
	Row     string
	Indices string
}

func (e *ArrayElementsError) Error() string {
	return fmt.Sprintf("attribute %d of row %s holds array elements (at %s), not one value", e.AttrID, e.Row, e.Indices)
}
