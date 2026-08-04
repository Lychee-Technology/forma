package numutil

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
)

// Int64Exact extracts an exact int64 from value without a float64 hop.
// It reports ok=false when the value is not integral, overflows int64, or
// has a type it cannot losslessly interpret; callers then fall back to the
// float64 path (which keeps today's semantics).
func Int64Exact(value any) (int64, bool) {
	switch v := value.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int16:
		return int64(v), true
	case json.Number:
		i, err := v.Int64()
		return i, err == nil
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		return i, err == nil
	case float64:
		// 只接受整值且在 int64 可表示区间内的 float64;此时 int64(v) 对
		// 该 float64 值本身是精确转换。2^63 及以上(含 float64(MaxInt64))
		// 越界,交回 float64 回退路径维持旧语义。
		if v != math.Trunc(v) || v < -9223372036854775808.0 || v >= 9223372036854775808.0 {
			return 0, false
		}
		return int64(v), true
	default:
		return 0, false
	}
}

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

// TryParseNumber parses s as int64, then float64, falling back to the raw
// string. Integral spellings in decimal or exponent form ("42.0",
// "9.007199254740993e15") refine to exact int64 when they fit (#357):
// ParseFloat gates acceptance so the accepted grammar is unchanged, and
// big.Rat supplies the exact value ParseFloat would have rounded above 2^53.
// Integral values beyond int64 range and genuinely fractional literals stay
// float64.
func TryParseNumber(s string) any {
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		if i, ok := integralInt64(s); ok {
			return i
		}
		return f
	}
	return s
}

// integralInt64 reports the exact int64 value of a literal that denotes an
// integer, in any spelling big.Rat understands. Callers must have already
// gated s through ParseFloat: big.Rat alone accepts forms the condition DSL
// must not (e.g. "1/3"), and rejects forms it must keep ("inf", "NaN"),
// which fall back to float64 unchanged.
func integralInt64(s string) (int64, bool) {
	r, ok := new(big.Rat).SetString(s)
	if !ok || !r.IsInt() {
		return 0, false
	}
	n := r.Num()
	if !n.IsInt64() {
		return 0, false
	}
	return n.Int64(), true
}
