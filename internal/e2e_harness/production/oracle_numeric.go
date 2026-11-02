package production

import (
	"cmp"
	"fmt"
	"math"
	"math/big"
	"strconv"
)

// The oracle's numeric forms. The integer family (smallint, integer,
// bigint) is int64, exact over the full range a column-bound bigint holds
// (#205): a float64 image cannot tell 2^53 from 2^53+1, so an oracle that
// folded integers to float64 would pass an engine that rounded them, and
// would itself mis-sort and mis-filter values past 2^53. numeric, bool (as
// 1/0) and date/datetime (as epoch millis) stay float64, the image their
// storage keeps. An integer operand that is not a whole int64 (a filter
// value such as "5.5") stays a *big.Rat, so every comparison stays exact.

// twoPow63 is the first float64 past MaxInt64; float64 values in
// [-2^63, 2^63) convert to int64 exactly when they are whole.
const twoPow63 = float64(1 << 63)

// toExactInteger normalizes an integer-family value to int64. A float64
// that is not a whole number in the int64 range is kept as float64 so the
// diff shows it rather than a silently truncated integer.
func toExactInteger(value any) (any, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case float64:
		return wholeFloatToInt64(v), nil
	case float32:
		return wholeFloatToInt64(float64(v)), nil
	case string:
		return parseExactInteger(v)
	default:
		return nil, fmt.Errorf("integer value has type %T", value)
	}
}

func wholeFloatToInt64(v float64) any {
	if v == math.Trunc(v) && v >= -twoPow63 && v < twoPow63 {
		return int64(v)
	}
	return v
}

// parseExactInteger parses the string form of an integer-family value or
// filter operand without a float64 hop: bare digits, a decimal spelling
// ("…993.0") and an exponent spelling ("9.007199254740993e15") of the same
// whole number all give the same int64, as the engine's exact bind does.
func parseExactInteger(raw string) (any, error) {
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return n, nil
	}
	r, ok := new(big.Rat).SetString(raw)
	if !ok {
		return nil, fmt.Errorf("integer value %q is not a number", raw)
	}
	if r.IsInt() && r.Num().IsInt64() {
		return r.Num().Int64(), nil
	}
	return r, nil
}

// compareNumbers compares two numeric oracle values exactly. ok is false
// when either side is not numeric.
func compareNumbers(a, b any) (result int, ok bool) {
	if af, isF := a.(float64); isF {
		if bf, isF := b.(float64); isF {
			return compareFloats(af, bf), true
		}
	}
	if ai, isI := a.(int64); isI {
		if bi, isI := b.(int64); isI {
			return cmp.Compare(ai, bi), true
		}
	}
	ar, aOK := toBigFloat(a)
	br, bOK := toBigFloat(b)
	if !aOK || !bOK {
		return 0, false
	}
	return ar.Cmp(br), true
}

// compareFloats keeps the comparator's historical float semantics: NaN is
// neither less nor greater than anything.
func compareFloats(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// toBigFloat widens a numeric oracle value to an exact big.Float. The
// precision covers an int64, a float64 and the quotient of a decimal
// filter operand closely enough to order it against any int64. NaN has no
// exact form and is not comparable this way.
func toBigFloat(v any) (*big.Float, bool) {
	const prec = 256
	switch x := v.(type) {
	case int64:
		return new(big.Float).SetPrec(prec).SetInt64(x), true
	case float64:
		if math.IsNaN(x) {
			return nil, false
		}
		return new(big.Float).SetPrec(prec).SetFloat64(x), true
	case *big.Rat:
		return new(big.Float).SetPrec(prec).SetRat(x), true
	default:
		return nil, false
	}
}

func toFloat64(value any) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("numeric value %q: %w", v, err)
		}
		return f, nil
	default:
		return 0, fmt.Errorf("numeric value has type %T", value)
	}
}
