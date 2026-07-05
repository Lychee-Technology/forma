package numutil

import (
	"encoding/json"
	"fmt"
	"strconv"
)

func ToFloat64(value any) (float64, bool) {
	val, err := Float64(value)
	return val, err == nil
}

func Float64(value any) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case json.Number:
		return v.Float64()
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
}

// NumericScalar constrains the numeric scalar types shared by the pointer helpers.
type NumericScalar interface {
	~float64 | ~float32 | ~int | ~int16 | ~int32 | ~int64
}

// OptionalFloat64FromPointer converts an optional numeric pointer to float64;
// a nil pointer reports omitted=true.
func OptionalFloat64FromPointer[T NumericScalar](value *T) (parsed float64, omitted bool, err error) {
	if value == nil {
		return 0, true, nil
	}
	parsed, err = Float64(*value)
	if err != nil {
		return 0, false, err
	}
	return parsed, false, nil
}

// OptionalPointerValue dereferences an optional pointer; a nil pointer reports
// omitted=true with the zero value.
func OptionalPointerValue[T any](value *T) (T, bool) {
	if value == nil {
		var zero T
		return zero, true
	}
	return *value, false
}

// TryParseNumber parses s as int64, then float64, falling back to the raw string.
func TryParseNumber(s string) any {
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s
}
