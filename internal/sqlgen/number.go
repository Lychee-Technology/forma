package sqlgen

import (
	"strconv"

	"github.com/lychee-technology/forma/internal/numutil"
)

func tryParseNumber(s string) any {
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s
}

type numericScalar interface {
	~float64 | ~float32 | ~int | ~int16 | ~int32 | ~int64
}

func optionalFloat64FromPointer[T numericScalar](value *T) (float64, bool, error) {
	if value == nil {
		return 0, true, nil
	}
	parsed, err := numutil.Float64(*value)
	if err != nil {
		return 0, false, err
	}
	return parsed, false, nil
}

func optionalPointerValue[T any](value *T) (T, bool) {
	if value == nil {
		var zero T
		return zero, true
	}
	return *value, false
}

func toFloat64(value any) (float64, error) {
	return numutil.Float64(value)
}
