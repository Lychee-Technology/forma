package transform

import (
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/numutil"
)

func populateTypedValue(attr *model.EAVRecord, attrName string, value any, meta forma.AttributeMetadata) (bool, error) {
	handleConversionError := func(err error) (bool, error) {
		return false, fmt.Errorf(
			"invalid value for attribute '%s' (attrID=%d): %w",
			attrName,
			meta.AttributeID,
			fmt.Errorf("%w: %v", forma.ErrInvalidInput, err),
		)
	}

	switch meta.ValueType {
	case forma.ValueTypeUUID:
		uuidVal, isUUID := toUUID(value)
		if !isUUID {
			return handleConversionError(fmt.Errorf("invalid UUID value: %v", value))
		}
		strVal := uuidVal.String()
		attr.ValueText = &strVal
	case forma.ValueTypeText:
		strVal, err := toString(value)
		if err != nil {
			return handleConversionError(err)
		}
		attr.ValueText = &strVal
	case forma.ValueTypeNumeric, forma.ValueTypeBigInt, forma.ValueTypeInteger, forma.ValueTypeSmallInt:
		numVal, err := numutil.Float64(value)
		if err != nil {
			return handleConversionError(err)
		}
		attr.ValueNumeric = &numVal
		if meta.ValueType == forma.ValueTypeBigInt {
			if exact, ok := numutil.Int64Exact(value); ok {
				attr.ValueInt64 = &exact
			}
		}
	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		timeVal, err := toTime(value)
		if err != nil {
			return handleConversionError(err)
		}
		unixMillis := timeToUnixMillisFloat64(timeVal)
		attr.ValueNumeric = &unixMillis
		exactMs := timeVal.UnixMilli()
		attr.ValueInt64 = &exactMs
	case forma.ValueTypeBool:
		boolVal, err := toBool(value)
		if err != nil {
			return handleConversionError(err)
		}
		floatBool := boolToFloat64(boolVal)
		attr.ValueNumeric = &floatBool
	default:
		return handleConversionError(fmt.Errorf("unsupported value type '%s'", meta.ValueType))
	}

	return true, nil
}

func toString(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case fmt.Stringer:
		return v.String(), nil
	default:
		return fmt.Sprintf("%v", value), nil
	}
}

// toInt64ExactForEAV mirrors numutil.Int64Exact but also accepts the pointer
// shapes ToEAVRecord tolerates for numeric values.
func toInt64ExactForEAV(value any) (int64, bool) {
	if p, ok := value.(*int64); ok {
		if p == nil {
			return 0, false
		}
		return *p, true
	}
	return numutil.Int64Exact(value)
}
