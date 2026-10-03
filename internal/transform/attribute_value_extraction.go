package transform

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// extractValueFromEAVRecord and its storage-mismatch error live here; the
// converter methods remain in attribute_converter.go (pure move, #204 review).

func extractValueFromEAVRecord(record model.EAVRecord, valueType forma.ValueType) (any, error) {
	switch valueType {
	case forma.ValueTypeText:
		if record.ValueNumeric != nil {
			return nil, storageTypeMismatchError(valueType, "value_numeric", "value_text")
		}
		if record.ValueText == nil {
			return nil, nil
		}
		return *record.ValueText, nil

	case forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeNumeric:
		return numericFromEAVRecord(record, valueType)

	case forma.ValueTypeBigInt:
		return bigintFromEAVRecord(record)

	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		return dateFromEAVRecord(record, valueType)

	case forma.ValueTypeUUID:
		if record.ValueNumeric != nil {
			return nil, storageTypeMismatchError(valueType, "value_numeric", "value_text")
		}
		if record.ValueText == nil {
			return nil, nil
		}
		uuidVal, err := uuid.Parse(*record.ValueText)
		if err != nil {
			return nil, fmt.Errorf("parse uuid: %w", err)
		}
		return uuidVal, nil

	case forma.ValueTypeBool:
		if record.ValueText != nil {
			return nil, storageTypeMismatchError(valueType, "value_text", "value_numeric")
		}
		if record.ValueNumeric == nil {
			return nil, nil
		}
		// Read-side rule (#404): a persisted image is the nearest of 0/1,
		// never the strict write funnel — a stored 0.3 is float noise, not
		// caller input to reject. Non-finite still has no truth value (#322).
		if err := finiteBoolInput(*record.ValueNumeric); err != nil {
			return nil, err
		}
		return float64ToBool(*record.ValueNumeric), nil

	default:
		// Fallback: try text first, then numeric
		if record.ValueText != nil {
			return *record.ValueText, nil
		}
		if record.ValueNumeric != nil {
			return *record.ValueNumeric, nil
		}
		return nil, nil
	}
}

// numericFromEAVRecord reads the smallint, integer and numeric arms. A stored
// NaN or infinity names no value of any of them: it used to read as an absent
// attribute, since Postgres renders it as a JSON string the float64 decode
// skipped, and an update then erased it. It is now a read-path consistency
// error (#592). Fractions and out-of-range images keep their conversions
// (#384).
func numericFromEAVRecord(record model.EAVRecord, valueType forma.ValueType) (any, error) {
	if record.ValueText != nil {
		return nil, storageTypeMismatchError(valueType, "value_text", "value_numeric")
	}
	if record.ValueNumeric == nil {
		return nil, nil
	}
	image := *record.ValueNumeric
	if isNonFinite(image) {
		return nil, fmt.Errorf("stored %s image %s of attribute %d in value_numeric has no finite float64 value",
			valueType, storedImageText(record), record.AttrID)
	}
	switch valueType {
	case forma.ValueTypeSmallInt:
		return int16(image), nil
	case forma.ValueTypeInteger:
		return int32(image), nil
	}
	return image, nil
}

// dateFromEAVRecord prefers the exact ValueInt64 when the record carries one
// (a main bigint column). A persisted float64 image is read by the contract
// of the float64 destinations (instantOfStoredImage, #592).
func dateFromEAVRecord(record model.EAVRecord, valueType forma.ValueType) (any, error) {
	if record.ValueText != nil {
		return nil, storageTypeMismatchError(valueType, "value_text", "value_numeric")
	}
	if record.ValueInt64 != nil {
		return unixMillisToTimeUTC(*record.ValueInt64), nil
	}
	if record.ValueNumeric == nil {
		return nil, nil
	}
	timeVal, err := instantOfStoredImage(&record)
	if err != nil {
		return nil, fmt.Errorf("%s value of attribute %d in value_numeric: %w", valueType, record.AttrID, err)
	}
	return timeVal, nil
}

// storedImageText quotes a stored image as the read query emitted it when the
// record carries the token, as its float64 otherwise.
func storedImageText(record model.EAVRecord) string {
	if record.ValueNumericRaw != "" {
		return record.ValueNumericRaw
	}
	return formatFitValue(*record.ValueNumeric)
}

// bigintFromEAVRecord prefers the exact ValueInt64 when the record carries one
// (a main bigint column). A persisted image (eav_data, a double_* column)
// converts back only when it names an int64 (#590, bigint_image.go).
func bigintFromEAVRecord(record model.EAVRecord) (any, error) {
	if record.ValueText != nil {
		return nil, storageTypeMismatchError(forma.ValueTypeBigInt, "value_text", "value_numeric")
	}
	if record.ValueInt64 != nil {
		return *record.ValueInt64, nil
	}
	if record.ValueNumeric == nil {
		return nil, nil
	}
	return int64FromBigintImage(*record.ValueNumeric)
}

func storageTypeMismatchError(valueType forma.ValueType, populatedColumn, expectedColumn string) error {
	return fmt.Errorf(
		"storage type mismatch for %s: %s should not be populated (expected %s)",
		valueType,
		populatedColumn,
		expectedColumn,
	)
}
