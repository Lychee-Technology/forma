package numutil

import (
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIntegralInt64AgainstRat is the differential oracle for the syntactic
// parser that replaced big.Rat in the production path (#357 round-2). For every
// literal in a generated corpus of ParseFloat-accepted spellings it asserts the
// parser's verdict and value match exact rational arithmetic: a literal is
// integral-in-range exactly when big.Rat says its denominator is 1 and its
// numerator fits int64.
//
// big.Rat is imported here and nowhere else in the package: the point of the
// rewrite is that production never pays for it, while tests keep its
// known-correct arithmetic as the reference.
func TestIntegralInt64AgainstRat(t *testing.T) {
	corpus := integralCorpus()
	require.Greater(t, len(corpus), 3000, "corpus too small to be a meaningful oracle")

	integral, fractional := assertParserMatchesRat(t, corpus)
	t.Logf("differential corpus: %d literals (%d integral, %d not)", len(corpus), integral, fractional)

	// A corpus that drifted to all-fractional (or all-integral) would still
	// pass every assertion above while testing only half the decision.
	require.Greater(t, integral, 500, "corpus exercises too few integral literals")
	require.Greater(t, fractional, 500, "corpus exercises too few non-integral literals")
}

// TestWideHexIntegralInt64AgainstRat isolates the 17..18-hex-digit significand
// class, whose accepting cases are rare enough under the general generator that
// they would be invisible inside the main corpus's totals. A 17-digit run needs
// 65..68 bits yet can still land in int64 after a 1..3-bit right shift; that
// combination is exactly what the earlier 16-digit width cap got wrong, so this
// test asserts its own non-degeneracy instead of borrowing the main corpus's.
func TestWideHexIntegralInt64AgainstRat(t *testing.T) {
	rng := rand.New(rand.NewSource(20260805))
	corpus := make([]string, 0, 4000)
	for i := 0; i < 4000; i++ {
		corpus = append(corpus, randomWideHexLiteral(rng))
	}

	integral, fractional := assertParserMatchesRat(t, corpus)
	t.Logf("wide-hex corpus: %d literals (%d integral, %d not)", len(corpus), integral, fractional)
	require.Greater(t, integral, 200, "wide-hex generator no longer reaches the accepting path")
	require.Greater(t, fractional, 200, "wide-hex generator no longer reaches the rejecting path")
}

// assertParserMatchesRat compares the parser's verdict and value against exact
// rational arithmetic for every literal, returning the split so callers can
// assert their own coverage.
func assertParserMatchesRat(t *testing.T, corpus []string) (integral, fractional int) {
	t.Helper()
	for _, s := range corpus {
		// The parser is only ever reached behind the ParseFloat gate, so a
		// literal ParseFloat rejects would be testing a contract that does not
		// exist. A rejected literal means the generator is broken.
		_, err := strconv.ParseFloat(s, 64)
		require.NoError(t, err, "generated literal %q must be ParseFloat-accepted", s)

		r, ok := new(big.Rat).SetString(s)
		require.True(t, ok, "reference could not read %q", s)
		wantOK := r.IsInt() && r.Num().IsInt64()

		gotVal, gotOK := integralInt64(s)
		require.Equal(t, wantOK, gotOK, "integrality verdict for %q (exact value %s)", s, r.RatString())
		if wantOK {
			require.Equal(t, r.Num().Int64(), gotVal, "exact value for %q", s)
			integral++
			continue
		}
		fractional++
	}
	return integral, fractional
}

// integralCorpus builds the differential corpus: curated boundary spellings
// plus deterministically generated decimal and hex-float literals.
func integralCorpus() []string {
	rng := rand.New(rand.NewSource(20260804))
	corpus := curatedSpellings()
	for i := 0; i < 5000; i++ {
		corpus = append(corpus, randomDecimalLiteral(rng))
	}
	for i := 0; i < 2000; i++ {
		corpus = append(corpus, randomHexLiteral(rng))
	}
	for i := 0; i < 2000; i++ {
		corpus = append(corpus, randomWideHexLiteral(rng))
	}
	return corpus
}

// curatedSpellings covers the boundaries a uniform generator hits rarely or
// never: the int64 endpoints, the 2^53 float64 precision cliff, and spellings
// whose length or padding used to defeat the round-1 cost guards.
func curatedSpellings() []string {
	values := []int64{
		0, 1, -1, 9, -9, 10, 1000, math.MaxInt64, math.MinInt64,
		math.MaxInt64 - 1, math.MinInt64 + 1, 1 << 53, (1 << 53) + 1, -(1 << 53) - 1,
		1000000000000000000, -1000000000000000000,
	}
	out := make([]string, 0, len(values)*6+16)
	for _, v := range values {
		d := strconv.FormatInt(v, 10)
		sign, mag := "", d
		if strings.HasPrefix(d, "-") {
			sign, mag = "-", d[1:]
		}
		out = append(out,
			d,
			d+".0",
			sign+strings.Repeat("0", 50)+mag+".0",     // zero padding past the old 64-char cap
			sign+mag+"000e-3",                         // trailing zeros absorbed by the exponent
			sign+mag+"."+strings.Repeat("0", 40),      // long all-zero fraction
			sign+mag+"."+strings.Repeat("0", 40)+"e0", // ... with an exponent
		)
		if len(mag) > 1 { // shift the point into the mantissa and pay it back
			out = append(out, fmt.Sprintf("%s%s.%se%d", sign, mag[:1], mag[1:], len(mag)-1))
		}
	}
	return append(out,
		"0", "0.0", "-0.0", "+0.0", "0e5", "0e-5", "0.000000e10", "0.0e300",
		"5.", ".5", "5.e3", ".5e1", ".5e0", "1e19", "-1e19", "1e18", "9.9e18",
		"18446744073709551615", "18446744073709551616.0", "9223372036854775808.0",
		"-9223372036854775809.0", "1_000", "1_0.0_1", "1_000e1_0", "1e+5", "+1.5",
		"1000000000000000000000e-3", "123.456e3", "123.4560e2", "1e-300", "1e300",
		"0x1p4", "0x1.8p1", "0x1.8p0", "0x10p-4", "0x1p-1", "0x1p63", "-0x1p63",
		"0x1p64", "-0x1p64", "0x0p0", "0x0.0p0", "-0x0p0", "0x.8p1", "0x1.p1",
		"0x"+strings.Repeat("0", 50)+"1p4", "0xffffffffffffffffp0", "0x1_0p4",
		"0X1P-4", "0x1.fffffffffffffp62", "0x8000000000000000p0",
		// Significands wider than the fast-path caps (>19 decimal digits,
		// >16 hex digits) and shifts that span the whole 64-bit width.
		"12345678901234567890123.0", "1234567890123456789012e-3",
		"99999999999999999999e-1", "0.0000000000000000001e19",
		"0x1ffffffffffffffffp0", "0x1_0000_0000_0000_0000p0",
		"0x1234567890abcdef12p-8", "0x1p-64", "0x8000000000000000p-63",
		"0x1.0000000000000001p0",
		// 17-hex-digit runs: 65..68 bits of significand that a 1..3-bit right
		// shift can still land inside int64, plus their out-of-range and
		// fractional neighbours and the 18-digit width that never can.
		"0x10000000000000008p-3", "0x10000000000000004p-2", "0x18000000000000008p-3",
		"0x3FFFFFFFFFFFFFFF8p-3", "-0x3FFFFFFFFFFFFFFF8p-3",
		"0x1FFFFFFFFFFFFFFFCp-2", "-0x1FFFFFFFFFFFFFFFCp-2",
		"0x40000000000000008p-3", "-0x40000000000000008p-3",
		"0x40000000000000000p-3", "-0x40000000000000000p-3",
		"0x10000000000000008p-4", "0x10000000000000008p-2", "0x10000000000000008p0",
		"0x10000000000000002p-1", "-0x10000000000000002p-1", "0x8000000000000000Fp-3",
		"0x1.0000000000000008p60", "0x1000000000000000.8p-3",
		"0x100000000000000008p-3", "0x100000000000000008p-4",
		"0xfffffffffffffffffp-3", "0x20000000000000008p-3",
		// Exponents that only cancel because the mantissa is long: these are
		// the shapes a fixed exponent clamp would misjudge.
		"0."+strings.Repeat("0", 500)+"1e501", "1"+strings.Repeat("0", 500)+"e-500",
		"0."+strings.Repeat("0", 500)+"1e500",
	)
}

// randomDecimalLiteral emits a uniformly random decimal spelling: optional
// sign, an integer part, an optional fraction, an optional exponent, and
// underscores at positions Go's float syntax allows.
func randomDecimalLiteral(rng *rand.Rand) string {
	var b strings.Builder
	b.WriteString(randomSign(rng))
	intLen := rng.Intn(9)
	fracLen := rng.Intn(9)
	hasPoint := rng.Intn(3) > 0
	if !hasPoint && intLen == 0 {
		intLen = 1 + rng.Intn(8) // ParseFloat needs at least one digit
	}
	if hasPoint && intLen == 0 && fracLen == 0 {
		fracLen = 1 + rng.Intn(8)
	}
	b.WriteString(withUnderscores(rng, randomDigits(rng, intLen, 10)))
	if hasPoint {
		b.WriteByte('.')
		b.WriteString(withUnderscores(rng, randomDigits(rng, fracLen, 10)))
	}
	if rng.Intn(2) == 0 {
		b.WriteString(randomExponent(rng, "eE"))
	}
	return b.String()
}

// randomHexLiteral emits a random hex-float spelling. ParseFloat requires the
// binary exponent on hex floats, so it is never omitted.
func randomHexLiteral(rng *rand.Rand) string {
	var b strings.Builder
	b.WriteString(randomSign(rng))
	if rng.Intn(2) == 0 {
		b.WriteString("0x")
	} else {
		b.WriteString("0X")
	}
	intLen := rng.Intn(7)
	fracLen := rng.Intn(7)
	hasPoint := rng.Intn(3) > 0
	if !hasPoint && intLen == 0 {
		intLen = 1 + rng.Intn(6)
	}
	if hasPoint && intLen == 0 && fracLen == 0 {
		fracLen = 1 + rng.Intn(6)
	}
	b.WriteString(withUnderscores(rng, randomDigits(rng, intLen, 16)))
	if hasPoint {
		b.WriteByte('.')
		b.WriteString(withUnderscores(rng, randomDigits(rng, fracLen, 16)))
	}
	b.WriteString(randomExponent(rng, "pP"))
	return b.String()
}

// randomWideHexLiteral emits hex spellings whose significant run is 17 or 18
// digits — the width that needs more than 64 bits to accumulate and that a
// small negative binary exponent (p in [-6,6] here) can still shift back into
// int64 range.
//
// Half the literals are *aimed*: a shift k is drawn first, then the leading
// digit is kept small and the trailing digit picked from those divisible by
// 2^k, which is the only shape that can accept. Aiming matters — under a
// uniform generator the accepting side is ~1.5% of the corpus, so the class
// would look covered while resting almost entirely on rejections.
func randomWideHexLiteral(rng *rand.Rand) string {
	const anyLead = "123456789abcdefABCDEF"
	const anyTail = "123456789abcdefABCDEF"
	// Trailing digits by shift budget: 2^k | digit.
	divisibleBy := [4]string{"", "2468aceACE", "48cC", "8"}

	runLen := 17
	if rng.Intn(3) == 0 {
		runLen = 18 // >= 2^68: provably out of range whatever the exponent
	}
	k := 1 + rng.Intn(3)
	aimed := rng.Intn(2) == 0
	lead, tail := anyLead, anyTail
	if aimed {
		lead, tail = "123", divisibleBy[k]
	}
	digits := []byte(randomDigits(rng, runLen, 16))
	digits[0] = lead[rng.Intn(len(lead))]
	digits[runLen-1] = tail[rng.Intn(len(tail))]

	var b strings.Builder
	b.WriteString(randomSign(rng))
	b.WriteString("0x")
	// Split the run across the point at a random position: the parser folds the
	// fractional width into the binary exponent, so the same value must be read
	// identically however the point moves through the run.
	point := rng.Intn(runLen + 1)
	b.WriteString(withUnderscores(rng, string(digits[:point])))
	if point < runLen || rng.Intn(2) == 0 {
		b.WriteByte('.')
		b.WriteString(withUnderscores(rng, string(digits[point:])))
	}
	// The parser folds 4*fracLen back in, so the binary exponent that decides
	// integrality is binExp; the written exponent pays the point offset back.
	binExp := -k
	if !aimed {
		binExp = rng.Intn(13) - 6
	}
	b.WriteString(fmt.Sprintf("%c%d", "pP"[rng.Intn(2)], binExp+4*(runLen-point)))
	return b.String()
}

func randomSign(rng *rand.Rand) string {
	switch rng.Intn(4) {
	case 0:
		return "-"
	case 1:
		return "+"
	default:
		return ""
	}
}

// randomExponent builds an exponent literal whose magnitude stays small enough
// that the big.Rat reference remains cheap; the extreme exponents are pinned
// as table cases instead.
func randomExponent(rng *rand.Rand, markers string) string {
	marker := markers[rng.Intn(len(markers))]
	value := rng.Intn(61) - 30
	if value >= 0 && rng.Intn(2) == 0 {
		return fmt.Sprintf("%c+%d", marker, value)
	}
	return fmt.Sprintf("%c%d", marker, value)
}

// randomDigits returns n digits in the given base, biased toward '0' so that
// zero runs, all-zero mantissas and trailing-zero absorption occur often.
func randomDigits(rng *rand.Rand, n, base int) string {
	const alphabet = "0123456789abcdef"
	out := make([]byte, n)
	for i := range out {
		if rng.Intn(3) == 0 {
			out[i] = '0'
			continue
		}
		c := alphabet[rng.Intn(base)]
		if base == 16 && rng.Intn(2) == 0 {
			c = strings.ToUpper(string(c))[0]
		}
		out[i] = c
	}
	return string(out)
}

// withUnderscores sprinkles underscores between adjacent digits, the only
// placement Go's literal syntax (and therefore ParseFloat) accepts.
func withUnderscores(rng *rand.Rand, digits string) string {
	if len(digits) < 2 || rng.Intn(4) > 0 {
		return digits
	}
	var b strings.Builder
	for i := 0; i < len(digits); i++ {
		if i > 0 && rng.Intn(3) == 0 {
			b.WriteByte('_')
		}
		b.WriteByte(digits[i])
	}
	return b.String()
}
