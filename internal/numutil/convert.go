package numutil

import (
	"encoding/json"
	"fmt"
	"math"
	"math/bits"
	"strconv"
	"strings"
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

// maxInt64Digits is the decimal width of MaxInt64/MinInt64 (19 digits). A
// significant run plus its trailing zeros longer than this cannot be an
// in-range int64; a run of exactly this width still needs ParseInt to decide.
const maxInt64Digits = 19

// TryParseNumber parses s as int64, then float64, falling back to the raw
// string. Integral spellings in decimal or exponent form ("42.0",
// "9.007199254740993e15", "1.5e3"), and the hex-float and underscore forms
// ParseFloat also accepts ("0x1p4", "1_000"), refine to exact int64 when they
// fit (#357). ParseFloat is the sole acceptance authority — the refinement runs
// only on strings it already accepted, so the grammar neither widens (rational
// syntax like "1/3" stays a raw string) nor narrows. Integral values beyond
// int64 range and genuinely fractional literals — including nonzero literals
// that underflow float64 to zero — stay float64.
func TryParseNumber(s string) any {
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		if i, verdict := parseIntegralInt64(s); verdict == IntegralityExact {
			return i
		}
		return f
	}
	return s
}

// parseIntegralInt64 reports the exact int64 value of a ParseFloat-accepted
// literal that denotes an integer, in any spelling and at any length, and
// otherwise which way it misses: IntegralityFractional for a genuine
// fraction (including a nonzero literal that underflows float64 to zero),
// IntegralityOutOfRange for an integer outside int64. It decides this
// syntactically — no big.Rat, no big.Int — so its cost is linear in len(s)
// with small constants and a caller-supplied literal cannot amplify it (#357
// round-2). The Inf/NaN spellings ParseFloat accepts get IntegralityUnknown.
//
// s must already have passed strconv.ParseFloat: the parser relies on that for
// well-formedness (underscore placement, digit content, hex "p" exponent) and
// is not itself an acceptance decision — any malformed shape returns
// IntegralityUnknown, which only ever means "keep float64".
func parseIntegralInt64(s string) (int64, Integrality) {
	neg := false
	switch {
	case strings.HasPrefix(s, "-"):
		neg, s = true, s[1:]
	case strings.HasPrefix(s, "+"):
		s = s[1:]
	}
	if len(s) > 1 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X') {
		return parseHexIntegralInt64(neg, s[2:])
	}
	return parseDecIntegralInt64(neg, s)
}

// parseDecIntegralInt64 decides the decimal/exponent spellings. The value is
// digits × 10^trail where trail folds the exponent, the fractional width and
// the significant run's trailing zeros together: trail < 0 means the literal
// keeps a fractional part, trail >= 0 means it is an integer whose canonical
// digit string ParseInt can range-check.
func parseDecIntegralInt64(neg bool, s string) (int64, Integrality) {
	mant, expLit := s, ""
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		mant, expLit = s[:i], s[i+1:]
	}
	digits, fracLen, ok := scanDigits(mant, isDecDigit)
	if !ok || len(digits) == 0 {
		return 0, IntegralityUnknown // "inf"/"Infinity"/"NaN" carry no digits.
	}
	first, last := findSignificantRun(digits)
	if first < 0 {
		return 0, IntegralityExact // Every digit is zero: the literal denotes zero exactly.
	}
	exp, ok := parseExponent(expLit, int64(len(mant))+64)
	if !ok {
		return 0, IntegralityUnknown
	}
	trail := exp - int64(fracLen) + int64(len(digits)-1-last)
	if trail < 0 {
		return 0, IntegralityFractional
	}
	if int64(last-first+1)+trail > maxInt64Digits {
		return 0, IntegralityOutOfRange
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
		return 0, IntegralityOutOfRange // 19 digits that still overflow, e.g. "9223372036854775808".
	}
	return v, IntegralityExact
}

// parseHexIntegralInt64 decides the hex-float spellings, whose value is
// hexDigits × 2^binExp. ParseFloat requires the "p" exponent on hex floats, so
// a missing one means the caller skipped the gate.
func parseHexIntegralInt64(neg bool, s string) (int64, Integrality) {
	marker := strings.IndexAny(s, "pP")
	if marker < 0 {
		return 0, IntegralityUnknown
	}
	mant, expLit := s[:marker], s[marker+1:]
	digits, fracLen, ok := scanDigits(mant, isHexDigit)
	if !ok || len(digits) == 0 {
		return 0, IntegralityUnknown
	}
	first, last := findSignificantRun(digits)
	if first < 0 {
		return 0, IntegralityExact
	}
	run := digits[first : last+1]
	exp, ok := parseExponent(expLit, 4*int64(len(mant))+128)
	if !ok {
		return 0, IntegralityUnknown
	}
	binExp := exp - 4*int64(fracLen) + 4*int64(len(digits)-1-last)
	// The run starts and ends with a nonzero hex digit, so its value needs
	// 4*len(run)-3 bits at minimum and carries at most 3 trailing zero *bits*
	// (the widest such last digit is "8" = 0b1000). Integrality therefore caps
	// any right shift at 3 bits, so an 18-digit run — at least 2^68 — still
	// leaves 2^65 after every shift it can survive, and is out of int64 range
	// for certain; a deeper shift moves a set bit past the point. 17 digits is
	// the widest that can ever land, and it needs the two-word path below
	// because 65..68 bits do not fit one uint64.
	if len(run) > 17 {
		if binExp < -int64(hexDigitTrailingZeroBits(run[len(run)-1])) {
			return 0, IntegralityFractional
		}
		return 0, IntegralityOutOfRange
	}
	if len(run) == 17 {
		return parseWideHexIntegralInt64(neg, run, binExp)
	}
	mag, err := strconv.ParseUint(string(run), 16, 64)
	if err != nil {
		return 0, IntegralityUnknown
	}
	mag, verdict := shiftMagnitude(mag, binExp)
	if verdict != IntegralityExact {
		return 0, verdict
	}
	return toSignedInt64(neg, mag)
}

// hexDigitTrailingZeroBits counts the trailing zero bits of a nonzero hex
// digit's value: 0 for an odd digit, 3 for "8".
func hexDigitTrailingZeroBits(digit byte) int {
	value, err := strconv.ParseUint(string(digit), 16, 8)
	if err != nil || value == 0 {
		return 0
	}
	return bits.TrailingZeros64(value)
}

// parseWideHexIntegralInt64 decides a 17-hex-digit significant run, whose value
// hi*2^64 + lo needs 65..68 bits and so cannot be accumulated in one uint64
// (hi is run[0], 1..15; lo is the remaining 16 digits). Rejecting on width
// alone would be wrong, not merely conservative: the run ends in a nonzero
// digit, which still leaves up to 3 trailing zero bits, so a right shift of
// 1..3 can bring the value back inside int64 — "0x10000000000000008p-3" is
// exactly 2^61+1.
//
// Only binExp < 0 can save such a run: at binExp >= 0 the value is already at
// least 2^64. Writing k = -binExp, the literal is integral iff lo's low k bits
// are all zero (a set bit shifted out means a genuine fraction), which also
// forces k <= 3 — the guard below only bounds the shift width so that the
// 64-bit expression stays defined. The shifted value is
// (hi << (64-k)) | (lo >> k), well defined in uint64 exactly when hi >> k == 0;
// hi >> k != 0 means the result is at least 2^64, i.e. out of range anyway.
// toSignedInt64 then applies the sign, admitting 2^63 only as MinInt64.
func parseWideHexIntegralInt64(neg bool, run []byte, binExp int64) (int64, Integrality) {
	if binExp >= 0 {
		return 0, IntegralityOutOfRange // at least 2^64 before any shift.
	}
	if binExp < -3 {
		return 0, IntegralityFractional // a set bit is shifted past the point.
	}
	hi, err := strconv.ParseUint(string(run[:1]), 16, 64)
	if err != nil {
		return 0, IntegralityUnknown
	}
	lo, err := strconv.ParseUint(string(run[1:]), 16, 64)
	if err != nil {
		return 0, IntegralityUnknown
	}
	k := uint(-binExp) // 1..3
	if lo&((1<<k)-1) != 0 {
		return 0, IntegralityFractional // a set bit would be shifted out.
	}
	if hi>>k != 0 {
		return 0, IntegralityOutOfRange // the shifted value still needs >= 65 bits.
	}
	return toSignedInt64(neg, hi<<(64-k)|lo>>k)
}

// shiftMagnitude applies 2^binExp to a nonzero magnitude, reporting
// IntegralityFractional when the result would keep a fractional part (right
// shift past a set bit) and IntegralityOutOfRange when it outgrows 64 bits.
func shiftMagnitude(mag uint64, binExp int64) (uint64, Integrality) {
	switch {
	case binExp < 0:
		if -binExp >= 64 {
			return 0, IntegralityFractional // mag is nonzero, so it cannot survive the shift.
		}
		shift := uint(-binExp)
		if bits.TrailingZeros64(mag) < int(shift) {
			return 0, IntegralityFractional
		}
		return mag >> shift, IntegralityExact
	case binExp > 0:
		if int64(bits.Len64(mag))+binExp > 64 {
			return 0, IntegralityOutOfRange
		}
		return mag << uint(binExp), IntegralityExact
	default:
		return mag, IntegralityExact
	}
}

// toSignedInt64 applies the sign, admitting 2^63 only as MinInt64.
func toSignedInt64(neg bool, mag uint64) (int64, Integrality) {
	if neg {
		if mag == 1<<63 {
			return math.MinInt64, IntegralityExact
		}
		if mag > math.MaxInt64 {
			return 0, IntegralityOutOfRange
		}
		return -int64(mag), IntegralityExact
	}
	if mag > math.MaxInt64 {
		return 0, IntegralityOutOfRange
	}
	return int64(mag), IntegralityExact
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

// findSignificantRun returns the first and last index of a non-zero digit, or
// (-1, -1) when every digit is zero. Digits are ASCII in both bases, so '0' is
// the zero digit either way.
func findSignificantRun(digits []byte) (first, last int) {
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
