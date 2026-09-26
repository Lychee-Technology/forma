package numutil

import (
	"encoding/json"
	"math"
	"strconv"
)

// Integrality is what a numeric input denotes for an integer destination:
// the verdict of ClassifyInt64.
type Integrality uint8

const (
	// IntegralityUnknown carries no verdict: the input is a type
	// ClassifyInt64 does not read, a textual spelling strconv.ParseFloat
	// rejects, or an Inf/NaN spelling.
	IntegralityUnknown Integrality = iota
	// IntegralityExact: the input denotes an integer within int64, and the
	// int64 returned beside it is that integer.
	IntegralityExact
	// IntegralityOutOfRange: the input denotes an integer outside int64.
	IntegralityOutOfRange
	// IntegralityFractional: the input denotes a non-integer.
	IntegralityFractional
)

// ClassifyInt64 reports what value denotes for an integer destination,
// judged on the input's own representation and never on its float64 image:
// a json.Number or string by its digits, a float by its value, an integer
// type by itself. The image cannot be the judge because past 2^52 it is
// always whole: "9007199254740991.5" is fractional although its image is
// 2^53, and "9.007199254740993e15" is the exact 9007199254740993 although
// that image is 2^53 too. A textual spelling is read by the #357 syntactic
// parser behind the ParseFloat gate, so every spelling ParseFloat accepts
// (decimal, exponent, hex-float, underscores) gets a verdict at a cost
// linear in its length; a spelling it rejects gets none, and the caller's
// float64 conversion is what refuses it.
func ClassifyInt64(value any) (int64, Integrality) {
	switch v := value.(type) {
	case int64:
		return v, IntegralityExact
	case int:
		return int64(v), IntegralityExact
	case int32:
		return int64(v), IntegralityExact
	case int16:
		return int64(v), IntegralityExact
	case json.Number:
		return classifyLiteral(string(v))
	case string:
		return classifyLiteral(v)
	case float64:
		return classifyFloat64(v)
	case float32:
		return classifyFloat64(float64(v))
	default:
		return 0, IntegralityUnknown
	}
}

// Int64Exact extracts an exact int64 from value without a float64 hop: the
// IntegralityExact verdict of ClassifyInt64. It reports ok=false for every
// other verdict; callers then fall back to the float64 path.
func Int64Exact(value any) (int64, bool) {
	v, verdict := ClassifyInt64(value)
	return v, verdict == IntegralityExact
}

// classifyLiteral judges a textual spelling. ParseFloat is the acceptance
// authority, as in TryParseNumber; parseIntegralInt64 relies on it for
// well-formedness.
func classifyLiteral(s string) (int64, Integrality) {
	if _, err := strconv.ParseFloat(s, 64); err != nil {
		return 0, IntegralityUnknown
	}
	return parseIntegralInt64(s)
}

// classifyFloat64 judges a float by its value. A whole float64 within
// [-2^63, 2^63) converts to int64 exactly; 2^63 (which float64(MaxInt64)
// rounds up to) is the first magnitude past int64.
func classifyFloat64(v float64) (int64, Integrality) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, IntegralityUnknown
	}
	if v != math.Trunc(v) {
		return 0, IntegralityFractional
	}
	if v < -9223372036854775808.0 || v >= 9223372036854775808.0 {
		return 0, IntegralityOutOfRange
	}
	return int64(v), IntegralityExact
}
