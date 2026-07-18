package transform

import (
	"fmt"
	"time"

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

	case forma.ValueTypeSmallInt:
		if record.ValueText != nil {
			return nil, storageTypeMismatchError(valueType, "value_text", "value_numeric")
		}
		if record.ValueNumeric == nil {
			return nil, nil
		}
		return int16(*record.ValueNumeric), nil

	case forma.ValueTypeInteger:
		if record.ValueText != nil {
			return nil, storageTypeMismatchError(valueType, "value_text", "value_numeric")
		}
		if record.ValueNumeric == nil {
			return nil, nil
		}
		return int32(*record.ValueNumeric), nil

	case forma.ValueTypeBigInt:
		if record.ValueText != nil {
			return nil, storageTypeMismatchError(valueType, "value_text", "value_numeric")
		}
		if record.ValueInt64 != nil {
			return *record.ValueInt64, nil
		}
		if record.ValueNumeric == nil {
			return nil, nil
		}
		return int64(*record.ValueNumeric), nil

	case forma.ValueTypeNumeric:
		if record.ValueText != nil {
			return nil, storageTypeMismatchError(valueType, "value_text", "value_numeric")
		}
		if record.ValueNumeric == nil {
			return nil, nil
		}
		return *record.ValueNumeric, nil

	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		if record.ValueText != nil {
			return nil, storageTypeMismatchError(valueType, "value_text", "value_numeric")
		}
		if record.ValueInt64 != nil {
			return time.UnixMilli(*record.ValueInt64).UTC(), nil
		}
		if record.ValueNumeric == nil {
			return nil, nil
		}
		timeVal := unixMillisFloat64ToTimeUTC(*record.ValueNumeric)
		return timeVal, nil

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
		return toBoolForEAV(record.ValueNumeric)

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

func storageTypeMismatchError(valueType forma.ValueType, populatedColumn, expectedColumn string) error {
	return fmt.Errorf(
		"storage type mismatch for %s: %s should not be populated (expected %s)",
		valueType,
		populatedColumn,
		expectedColumn,
	)
}
