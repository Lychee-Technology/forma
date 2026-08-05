package numutil

import (
	"encoding/json"
	"fmt"
	"math"
	"math/bits"
	"strconv"
	"strings"
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

// maxInt64Digits is the decimal width of MaxInt64/MinInt64 (19 digits). A
// significant run plus its trailing zeros longer than this cannot be an
// in-range int64; a run of exactly this width still needs ParseInt to decide.
const maxInt64Digits = 19

// TryParseNumber parses s as int64, then float64, falling back to the raw
// string. Integral spellings in decimal or exponent form ("42.0",
// "9.007199254740993e15", "1.5e3"), and the hex-float and underscore forms
// ParseFloat also accepts ("0x1p4", "1_000"), refine to exact int64 when they
// fit (#357). ParseFloat is the sole acceptance authority — the refinement runs
// only on strings it already accepted, so the grammar neither widens (big.Rat's
// "1/3" stays a raw string) nor narrows. Integral values beyond int64 range and
// genuinely fractional literals — including nonzero literals that underflow
// float64 to zero — stay float64.
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

// integralInt64 reports the exact int64 value of a ParseFloat-accepted literal
// that denotes an integer, in any spelling and at any length. It decides this
// syntactically — no big.Rat, no big.Int — so its cost is strictly linear in
// len(s) with small constants and a caller-supplied literal cannot amplify it
// (#357 round-2). Returns false for genuine fractions (including nonzero
// literals that underflow float64 to zero), integers outside int64 range, and
// the Inf/NaN spellings ParseFloat accepts.
//
// s must already have passed strconv.ParseFloat: the parser relies on that for
// well-formedness (underscore placement, digit content, hex "p" exponent) and
// is not itself an acceptance decision — any malformed shape returns false,
// which only ever means "keep float64".
func integralInt64(s string) (int64, bool) {
	neg := false
	switch {
	case strings.HasPrefix(s, "-"):
		neg, s = true, s[1:]
	case strings.HasPrefix(s, "+"):
		s = s[1:]
	}
	if len(s) > 1 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X') {
		return hexIntegralInt64(neg, s[2:])
	}
	return decIntegralInt64(neg, s)
}

// decIntegralInt64 decides the decimal/exponent spellings. The value is
// digits × 10^trail where trail folds the exponent, the fractional width and
// the significant run's trailing zeros together: trail < 0 means the literal
// keeps a fractional part, trail >= 0 means it is an integer whose canonical
// digit string ParseInt can range-check.
func decIntegralInt64(neg bool, s string) (int64, bool) {
	mant, expLit := s, ""
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		mant, expLit = s[:i], s[i+1:]
	}
	digits, fracLen, ok := scanDigits(mant, isDecDigit)
	if !ok || len(digits) == 0 {
		return 0, false // "inf"/"Infinity"/"NaN" carry no digits.
	}
	first, last := significantRun(digits)
	if first < 0 {
		return 0, true // Every digit is zero: the literal denotes zero exactly.
	}
	exp, ok := parseExponent(expLit, int64(len(mant))+64)
	if !ok {
		return 0, false
	}
	trail := exp - int64(fracLen) + int64(len(digits)-1-last)
	if trail < 0 {
		return 0, false
	}
	if int64(last-first+1)+trail > maxInt64Digits {
		return 0, false
	}
	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
	b.Write(digits[first : last+1])
	for i := int64(0); i < trail; i++ {
		b.WriteByte('0')
	}
	v, err := strconv.ParseInt(b.String(), 10, 64)
	if err != nil {
		return 0, false // 19 digits that still overflow, e.g. "9223372036854775808".
	}
	return v, true
}

// hexIntegralInt64 decides the hex-float spellings, whose value is
// hexDigits × 2^binExp. ParseFloat requires the "p" exponent on hex floats, so
// a missing one means the caller skipped the gate.
func hexIntegralInt64(neg bool, s string) (int64, bool) {
	marker := strings.IndexAny(s, "pP")
	if marker < 0 {
		return 0, false
	}
	mant, expLit := s[:marker], s[marker+1:]
	digits, fracLen, ok := scanDigits(mant, isHexDigit)
	if !ok || len(digits) == 0 {
		return 0, false
	}
	first, last := significantRun(digits)
	if first < 0 {
		return 0, true
	}
	run := digits[first : last+1]
	if len(run) > 16 { // >64 bits of significand cannot land in int64.
		return 0, false
	}
	mag, err := strconv.ParseUint(string(run), 16, 64)
	if err != nil {
		return 0, false
	}
	exp, ok := parseExponent(expLit, 4*int64(len(mant))+128)
	if !ok {
		return 0, false
	}
	binExp := exp - 4*int64(fracLen) + 4*int64(len(digits)-1-last)
	mag, ok = shiftMagnitude(mag, binExp)
	if !ok {
		return 0, false
	}
	return signedFromMagnitude(neg, mag)
}

// shiftMagnitude applies 2^binExp to a nonzero magnitude, reporting false when
// the result would keep a fractional part (right shift past a set bit) or
// outgrow 64 bits.
func shiftMagnitude(mag uint64, binExp int64) (uint64, bool) {
	switch {
	case binExp < 0:
		if -binExp >= 64 {
			return 0, false // mag is nonzero, so it cannot survive the shift.
		}
		shift := uint(-binExp)
		if bits.TrailingZeros64(mag) < int(shift) {
			return 0, false
		}
		return mag >> shift, true
	case binExp > 0:
		if int64(bits.Len64(mag))+binExp > 64 {
			return 0, false
		}
		return mag << uint(binExp), true
	default:
		return mag, true
	}
}

// signedFromMagnitude applies the sign, admitting 2^63 only as MinInt64.
func signedFromMagnitude(neg bool, mag uint64) (int64, bool) {
	if neg {
		if mag == 1<<63 {
			return math.MinInt64, true
		}
		if mag > math.MaxInt64 {
			return 0, false
		}
		return -int64(mag), true
	}
	if mag > math.MaxInt64 {
		return 0, false
	}
	return int64(mag), true
}

// scanDigits collects the mantissa digits in order, skipping the underscores
// ParseFloat already validated, and counts how many fall after the point. It
// reports false on any other character, which is how the letter-only Inf/NaN
// spellings drop out.
func scanDigits(mant string, isDigit func(byte) bool) (digits []byte, fracLen int, ok bool) {
	digits = make([]byte, 0, len(mant))
	seenPoint := false
	for i := 0; i < len(mant); i++ {
		switch c := mant[i]; {
		case c == '_':
		case c == '.':
			seenPoint = true
		case isDigit(c):
			digits = append(digits, c)
			if seenPoint {
				fracLen++
			}
		default:
			return nil, 0, false
		}
	}
	return digits, fracLen, true
}

// significantRun returns the first and last index of a non-zero digit, or
// (-1, -1) when every digit is zero. Digits are ASCII in both bases, so '0' is
// the zero digit either way.
func significantRun(digits []byte) (first, last int) {
	first, last = -1, -1
	for i, c := range digits {
		if c != '0' {
			if first < 0 {
				first = i
			}
			last = i
		}
	}
	return first, last
}

// parseExponent reads an exponent literal, saturating at ±limit. The caller
// passes a limit that already dominates every other term of its exponent sum
// (the mantissa cannot contribute more digits than it has characters), so a
// saturated exponent yields the same verdict — out of int64 range, or
// fractional — that the true exponent would; a fixed clamp would not, because a
// long enough fraction can cancel an arbitrarily large exponent. ParseFloat
// accepts exponents far wider than int64 ("1e-99999999999999999999"), which is
// why saturation, not strconv, does the reading. It reports false on anything
// the ParseFloat gate should already have rejected, so such input keeps float64.
func parseExponent(lit string, limit int64) (int64, bool) {
	if lit == "" {
		return 0, true
	}
	neg := false
	switch lit[0] {
	case '-':
		neg, lit = true, lit[1:]
	case '+':
		lit = lit[1:]
	}
	var v int64
	digits := 0
	for i := 0; i < len(lit); i++ {
		c := lit[i]
		if c == '_' {
			continue
		}
		if !isDecDigit(c) {
			return 0, false
		}
		digits++
		// v < limit here, and limit is derived from len(mant), so the product
		// cannot overflow for any string that fits in memory.
		v = v*10 + int64(c-'0')
		if v >= limit {
			v = limit
			break
		}
	}
	if digits == 0 {
		return 0, false
	}
	if neg {
		return -v, true
	}
	return v, true
}

func isDecDigit(c byte) bool { return c >= '0' && c <= '9' }

func isHexDigit(c byte) bool {
	return isDecDigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}
