package transform

import (
	"fmt"
	"strconv"
	"time"

	"github.com/lychee-technology/forma/internal/numutil"
)

// Scalar coercion helpers shared by the EAV write funnels
// (AttributeConverter.ToEAVRecord, populateTypedValue in typed_value.go).
// derefPointer is also reused by numeric_conversion.go.
//
// The numeric coercion helpers (toFloat64ForEAV, parseTrimmedFloat64) live in
// numeric_conversion.go alongside the finiteForEAV guard they feed. The bool
// read-side rule (float64ToBool) lives in bool_numeric.go.

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

// boolFromAny is the single write-side bool funnel (#404), shared by
// ToEAVRecord and populateTypedValue so the two entry points cannot embody
// different truth tables again. The rule:
//
//   - bool: the value.
//   - string: strconv.ParseBool; anything else is rejected. "banana" → false
//     is silent corruption, and rejecting is the write-validation posture.
//   - any numeric width, pointer or not, and json.Number: finite (#322), then
//     exactly 0 or 1. The read side's float64ToBool threshold is a tolerance
//     for persisted images, not an acceptance rule — for 0.3 either answer is
//     a guess, so the funnel does not guess.
//   - anything else: rejected.
func boolFromAny(value any) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case *bool:
		return derefPointer(v, "bool")
	case string:
		return boolFromString(v)
	case *string:
		str, err := derefPointer(v, "string")
		if err != nil {
			return false, err
		}
		return boolFromString(str)
	default:
		return boolFromNumeric(value)
	}
}

func boolFromString(value string) (bool, error) {
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("cannot convert string %q to bool: %w", value, err)
	}
	return parsed, nil
}

// boolFromNumeric accepts only the exact images 0 and 1 (see boolFromAny).
func boolFromNumeric(value any) (bool, error) {
	scalar, err := derefNumericPointer(value)
	if err != nil {
		return false, err
	}
	image, err := numutil.Float64(scalar)
	if err != nil {
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
	if err := finiteBoolInput(image); err != nil {
		return false, err
	}
	switch image {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("value %v is not a boolean image; must be 0 or 1", image)
	}
}

// derefNumericPointer unwraps the numeric pointer shapes ToEAVRecord accepts
// so numutil.Float64 sees the scalar; non-pointers pass through unchanged.
func derefNumericPointer(value any) (any, error) {
	switch v := value.(type) {
	case *int:
		return derefPointer(v, "int")
	case *int16:
		return derefPointer(v, "int16")
	case *int32:
		return derefPointer(v, "int32")
	case *int64:
		return derefPointer(v, "int64")
	case *float32:
		return derefPointer(v, "float32")
	case *float64:
		return derefPointer(v, "float64")
	default:
		return value, nil
	}
}

func derefPointer[T any](value *T, typeName string) (T, error) {
	if value == nil {
		var zero T
		return zero, fmt.Errorf("nil %s pointer", typeName)
	}
	return *value, nil
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

// ToFloat64 is an exported helper that behaves like the legacy optimizer helper:
// it returns (float64, bool) where bool indicates success.
func ToFloat64(v any) (float64, bool) {
	return numutil.ToFloat64(v)
}
