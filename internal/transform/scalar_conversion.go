package transform

import (
	"encoding/json"
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
//   - every numeric width the write path knows — the signed and unsigned
//     integers of each width, float32, float64, pointers to each
//     (numutil.Float64's set and derefNumericPointer's shapes, the same ones
//     toFloat64ForEAV accepts for numeric attributes, #566) — finite (#322),
//     then exactly 0 or 1. The read side's float64ToBool threshold is a
//     tolerance for persisted images, not an acceptance rule — for 0.3 either
//     answer is a guess, so the funnel does not guess. uintptr is not a
//     number here and stays rejected, as on the numeric funnel.
//   - json.Number: decided on its decimal text (boolFromNumberLiteral), so a
//     literal that merely rounds to 0 or 1 in float64 is still rejected.
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
	if literal, ok := scalar.(json.Number); ok {
		return boolFromNumberLiteral(literal)
	}
	image, err := numutil.Float64(scalar)
	if err != nil {
		return false, fmt.Errorf("cannot convert %T value %v to bool: %w", value, scalar, err)
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

// boolFromNumberLiteral decides a json.Number on its decimal text, not on its
// float64 image: "1.0000000000000001" rounds to float64(1) yet is not the
// image 1, and the strict rule must not accept it (PR #564 review). This is
// the HTTP path — httpapi decodes with UseNumber, so every JSON number
// arrives here. strconv.ParseFloat is the acceptance authority, as in
// numutil.TryParseNumber; that helper then refines any literal denoting an
// integer exactly ("1.0", "1e0", "-0") to int64 and leaves genuine fractions,
// integers past int64, and the Inf/NaN spellings as float64.
func boolFromNumberLiteral(literal json.Number) (bool, error) {
	if _, err := strconv.ParseFloat(string(literal), 64); err != nil {
		return false, fmt.Errorf("cannot convert json.Number %q to bool: %w", literal, err)
	}
	switch parsed := numutil.TryParseNumber(string(literal)).(type) {
	case int64:
		switch parsed {
		case 0:
			return false, nil
		case 1:
			return true, nil
		}
	case float64:
		if err := finiteBoolInput(parsed); err != nil {
			return false, err
		}
	}
	return false, fmt.Errorf("value %s is not a boolean image; must be 0 or 1", literal)
}

// derefNumericPointer unwraps every numeric pointer shape the write path
// knows (numutil.NumericScalar's widths) so the scalar converters see the
// value; non-pointers pass through unchanged and a nil pointer is refused.
// It is the one list of pointer shapes behind every Go-typed funnel — numeric
// (toFloat64ForEAV), the exact bigint sidecar (toInt64ExactForEAV) and bool
// (boolFromNumeric) — so their type sets cannot drift apart again (#566).
func derefNumericPointer(value any) (any, error) {
	switch v := value.(type) {
	case *int:
		return derefNumeric(v)
	case *int8:
		return derefNumeric(v)
	case *int16:
		return derefNumeric(v)
	case *int32:
		return derefNumeric(v)
	case *int64:
		return derefNumeric(v)
	case *uint:
		return derefNumeric(v)
	case *uint8:
		return derefNumeric(v)
	case *uint16:
		return derefNumeric(v)
	case *uint32:
		return derefNumeric(v)
	case *uint64:
		return derefNumeric(v)
	case *float32:
		return derefNumeric(v)
	case *float64:
		return derefNumeric(v)
	default:
		return value, nil
	}
}

// derefNumeric dereferences one numeric pointer shape, naming the pointee
// type ("nil uint16 pointer") when it is nil.
func derefNumeric[T numutil.NumericScalar](p *T) (any, error) {
	if p == nil {
		var zero T
		return nil, fmt.Errorf("nil %T pointer", zero)
	}
	return *p, nil
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
