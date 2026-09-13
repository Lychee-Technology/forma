package transform

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lychee-technology/forma/internal/numutil"
)

// Scalar coercion helpers behind AttributeConverter.ToEAVRecord and
// populateTypedValue (typed_value.go).
//
// The numeric coercion helpers (toFloat64ForEAV, parseTrimmedFloat64) live in
// numeric_conversion.go alongside the finiteForEAV guard they feed.

func toTimeForEAV(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case *time.Time:
		timeValue, err := derefPointer(v, "time")
		if err != nil {
			return time.Time{}, err
		}
		return timeValue, nil
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", value)
	}
}

func toBoolForEAV(value any) (bool, error) {
	switch v := value.(type) {
	case string:
		return isTrueStringForEAV(v), nil
	case *string:
		str, err := derefPointer(v, "string")
		if err != nil {
			return false, err
		}
		return isTrueStringForEAV(str), nil
	case bool:
		return v, nil
	case *bool:
		booleanValue, err := derefPointer(v, "bool")
		if err != nil {
			return false, err
		}
		return booleanValue, nil
	case int:
		return boolFromPositive(v), nil
	case *int:
		num, err := derefPointer(v, "int")
		if err != nil {
			return false, err
		}
		return boolFromPositive(num), nil
	case int16:
		return boolFromPositive(v), nil
	case *int16:
		num, err := derefPointer(v, "int16")
		if err != nil {
			return false, err
		}
		return boolFromPositive(num), nil
	case int32:
		return boolFromNonZero(v), nil
	case *int32:
		num, err := derefPointer(v, "int32")
		if err != nil {
			return false, err
		}
		return boolFromPositive(num), nil
	case int64:
		return boolFromNonZero(v), nil
	case *int64:
		num, err := derefPointer(v, "int64")
		if err != nil {
			return false, err
		}
		return boolFromPositive(num), nil
	case float32:
		return finiteFloat64ToBool(float64(v))
	case *float32:
		num, err := derefPointer(v, "float32")
		if err != nil {
			return false, err
		}
		return finiteFloat64ToBool(float64(num))
	case float64:
		return finiteFloat64ToBool(v)
	case *float64:
		num, err := derefPointer(v, "float64")
		if err != nil {
			return false, err
		}
		return finiteFloat64ToBool(num)
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return false, fmt.Errorf("cannot convert json.Number %q to bool: %w", v.String(), err)
		}
		if err := finiteBoolInput(f); err != nil {
			return false, err
		}
		return f != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
}

// finiteFloat64ToBool is toBoolForEAV's float path: reject non-finite, then
// apply the existing threshold coercion unchanged.
func finiteFloat64ToBool(value float64) (bool, error) {
	if err := finiteBoolInput(value); err != nil {
		return false, err
	}
	return float64ToBool(value), nil
}

func derefPointer[T any](value *T, typeName string) (T, error) {
	if value == nil {
		var zero T
		return zero, fmt.Errorf("nil %s pointer", typeName)
	}
	return *value, nil
}

func boolFromPositive[T ~int | ~int16 | ~int32 | ~int64](value T) bool {
	return value > 0
}

func boolFromNonZero[T ~int32 | ~int64](value T) bool {
	return value != 0
}

func isTrueStringForEAV(value string) bool {
	return strings.ToLower(value) == "true" || value == "1"
}

func toTime(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		epoch, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return time.UnixMilli(epoch), nil
		}

		formats := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02",
			"2006-01",
		}
		for _, format := range formats {
			if parsed, err := time.Parse(format, v); err == nil {
				return parsed, nil
			}
		}
		return time.Time{}, fmt.Errorf("unsupported time format: %s", v)
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", value)
	}
}

func toBool(value any) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		return strconv.ParseBool(v)
	case int:
		return v != 0, nil
	case int64:
		return v != 0, nil
	case float64:
		if err := finiteBoolInput(v); err != nil {
			return false, err
		}
		return v != 0, nil
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return false, fmt.Errorf("cannot convert json.Number %q to bool: %w", v.String(), err)
		}
		if err := finiteBoolInput(f); err != nil {
			return false, err
		}
		return f != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
}

// ToFloat64Ok is an exported helper that behaves like the legacy optimizer helper:
// it returns (float64, bool) where bool indicates success.
func ToFloat64(v any) (float64, bool) {
	return numutil.ToFloat64(v)
}
