package numutil

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTryParseNumber(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect any
	}{
		{name: "int64", input: "42", expect: int64(42)},
		{name: "negative int64", input: "-7", expect: int64(-7)},
		{name: "float64", input: "3.14", expect: float64(3.14)},
		// "1e3" denotes the integer 1000, so since #357 it refines to int64
		// like every other integral spelling; a fractional exponent literal
		// still lands on float64.
		{name: "scientific integral", input: "1e3", expect: int64(1000)},
		{name: "scientific float64", input: "1.5e-3", expect: float64(0.0015)},
		{name: "non-numeric", input: "abc", expect: "abc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TryParseNumber(tt.input)
			switch exp := tt.expect.(type) {
			case int64:
				val, ok := got.(int64)
				assert.True(t, ok, "expected int64")
				assert.Equal(t, exp, val)
			case float64:
				val, ok := got.(float64)
				assert.True(t, ok, "expected float64")
				assert.InDelta(t, exp, val, 1e-9)
			case string:
				val, ok := got.(string)
				assert.True(t, ok, "expected string")
				assert.Equal(t, exp, val)
			default:
				t.Fatalf("unsupported expected type %T", exp)
			}
		})
	}
}

// TestTryParseNumberIntegralSpellings pins the #357 contract: an accepted
// literal that denotes an integer in int64 range yields an exact int64 in
// EVERY spelling, not only the bare-digits one. The grammar itself is
// unchanged — ParseFloat still decides acceptance, so a string it rejects
// ("1/3", "abc") still falls through to the raw-string arm no matter what
// other number syntaxes would take it.
func TestTryParseNumberIntegralSpellings(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  any
	}{
		{"bare integer", "42", int64(42)},
		{"decimal spelling", "42.0", int64(42)},
		{"decimal spelling above 2^53", "9007199254740993.0", int64(9007199254740993)},
		{"exponent spelling above 2^53", "9.007199254740993e15", int64(9007199254740993)},
		{"exponent spelling at MaxInt64", "9.223372036854775807e18", int64(math.MaxInt64)},
		{"trailing-zero exponent", "1e0", int64(1)},
		// A fractional mantissa still denotes an integer once the exponent is
		// applied — it is the value that decides, not the spelling's shape.
		{"fractional mantissa, integral value", "1.5e3", int64(1500)},
		{"negative decimal spelling", "-9007199254740993.0", int64(-9007199254740993)},
		{"negative zero folds to int64 zero", "-0.0", int64(0)},
		{"fractional stays float64", "3.5", float64(3.5)},
		// Integral but outside int64 — int64 cannot carry it, so the float64
		// fallback (today's behavior) stands rather than a silent truncation.
		{"integral beyond int64 range", "1e300", float64(1e300)},
		// ParseFloat accepts these; they carry no digits, so the refinement
		// fails closed and the float64 they always produced survives.
		{"positive infinity keeps float64", "inf", math.Inf(1)},
		{"NaN keeps float64", "NaN", math.NaN()},
		// Grammar must not widen: rational syntax would take "1/3", but the
		// ParseFloat gate is the sole acceptance authority.
		{"rational syntax is not a number", "1/3", "1/3"},
		{"non-numeric", "abc", "abc"},

		// A nonzero literal that underflows float64 to zero denotes a genuine
		// fraction (10^-1000000 ≠ 0), so the fractional-literal contract keeps
		// it on float64 — the type it had before #357. Only a spelling that
		// *denotes* zero is integral zero.
		{"underflowing nonzero literal stays float64", "1e-1000000", float64(0)},
		{"zero in exponent spelling is integral zero", "0e5", int64(0)},
		// Spelling length is not a contract boundary: an integral value refines
		// exactly however long its spelling is. Zero padding past any cap must
		// not resurrect the rounded float64 (2^53+1 rounds to ...92 as float64).
		{
			"over-long integral spelling refines exactly",
			strings.Repeat("0", 50) + "9007199254740993.0",
			int64(9007199254740993),
		},
		// Beyond int64 range there is no exact int64 to bind, so the float64
		// fallback (today's behavior) stands.
		{"beyond int64 range stays float64", "1e19", float64(1e19)},

		// Decimal edges of the syntactic parser: the decision is
		// "digits × 10^trail with trail >= 0", where trail folds the exponent,
		// the fractional width and the significant run's trailing zeros.
		{"all-zero mantissa ignores the exponent", "0.000000e10", int64(0)},
		{"trailing zeros absorb a negative exponent", "1000000000000000000000e-3", int64(1e18)},
		{"exponent cancels the fraction exactly", "123.456e3", int64(123456)},
		{"exponent one short of the fraction stays float64", "123.4560e2", float64(12345.6)},
		{"underscores are transparent", "1_000", int64(1000)},
		{"exponent spelling at MinInt64", "-9.223372036854775808e18", int64(math.MinInt64)},
		{"decimal spelling at MinInt64", "-9223372036854775808.0", int64(math.MinInt64)},
		{"MaxInt64+1 has no int64 and stays float64", "9223372036854775808.0", float64(9223372036854775808)},
		{"negative underflow stays float64", "-1e-1000000", float64(0)},

		// Hex-float edges: the value is hexDigits × 2^binExp, integral only when
		// the significand survives the binary shift with no bit lost.
		{"hex integer", "0x1p4", int64(16)},
		{"hex fraction that lands on an integer", "0x1.8p1", int64(3)},
		{"hex fraction that stays fractional", "0x1.8p0", float64(1.5)},
		{"hex right shift onto an integer", "0x10p-4", int64(1)},
		{"hex right shift past a set bit", "0x1p-1", float64(0.5)},
		{"hex zero padding is transparent", "0x" + strings.Repeat("0", 50) + "1p4", int64(16)},

		// Cost is linear in len(s) with no amplification: these literals are
		// answered by string inspection alone, so an arbitrarily long spelling
		// is both exact (no length cap) and cheap (no exact-arithmetic
		// denominator to materialize).
		{"100k-char padding still refines exactly", strings.Repeat("0", 100000) + "42.0", int64(42)},
		{"huge exponent on a zero mantissa is integral zero", "0e" + strings.Repeat("9", 1000), int64(0)},
		{
			"huge exponent cancelled by a long fraction",
			"0." + strings.Repeat("0", 5000) + "1e5001",
			int64(1),
		},
		// The exponent reader saturates at a limit derived from the mantissa
		// length, not at a fixed constant: a fraction this long cancels an
		// exponent past any constant clamp, and clamping would misread the
		// literal as a fraction and drop it to float64.
		{
			"exponent beyond any fixed clamp still cancels",
			"0." + strings.Repeat("0", 100000) + "1e100001",
			int64(1),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := TryParseNumber(tc.input)
			switch want := tc.want.(type) {
			case int64:
				require.IsType(t, int64(0), got, "spelling %q must refine to int64", tc.input)
				require.Equal(t, want, got.(int64))
			case float64:
				require.IsType(t, float64(0), got, "spelling %q must stay float64", tc.input)
				if math.IsNaN(want) {
					require.True(t, math.IsNaN(got.(float64)), "want NaN, got %v", got)
					return
				}
				require.Equal(t, want, got.(float64))
			case string:
				require.IsType(t, "", got, "spelling %q must fall through to the raw string", tc.input)
				require.Equal(t, want, got.(string))
			default:
				t.Fatalf("unsupported expected type %T", want)
			}
		})
	}
}

func TestInt64Exact(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want int64
		ok   bool
	}{
		{"int64_max", int64(math.MaxInt64), math.MaxInt64, true},
		{"int64_neg_max", int64(-math.MaxInt64), -math.MaxInt64, true},
		{"int", int(7), 7, true},
		{"int32", int32(-5), -5, true},
		{"int16", int16(3), 3, true},
		{"json_number_int", json.Number("9223372036854775807"), math.MaxInt64, true},
		{"json_number_frac", json.Number("1.5"), 0, false},
		{"string_int", "-9223372036854775807", -math.MaxInt64, true},
		{"string_junk", "abc", 0, false},
		{"float64_integral", float64(1 << 62), 1 << 62, true},
		{"float64_frac", float64(1.5), 0, false},
		{"float64_2_63", math.Ldexp(1, 63), 0, false}, // 2^63 越界 int64
		{"nil", nil, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := Int64Exact(tc.in)
			require.Equal(t, tc.ok, ok)
			if tc.ok {
				require.Equal(t, tc.want, got)
			}
		})
	}
}
