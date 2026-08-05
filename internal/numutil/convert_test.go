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
// ("1/3", "abc") still falls through to the raw-string arm even when
// big.Rat could parse it.
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
		// ParseFloat accepts these; big.Rat does not. The refinement fails
		// closed and the float64 they always produced survives.
		{"positive infinity keeps float64", "inf", math.Inf(1)},
		{"NaN keeps float64", "NaN", math.NaN()},
		// Grammar must not widen: big.Rat alone would take "1/3".
		{"rat-only fraction is not a number", "1/3", "1/3"},
		{"non-numeric", "abc", "abc"},

		// Cost guards (see refineFloatLiteral). Each row is value-preserving;
		// they exist so the guards cannot be removed silently, and they are
		// cheap by construction — a hostile literal never reaches big.Rat.
		//
		// Underflow class: ParseFloat accepts "1e-1000000" and returns 0 with
		// no error, but big.Rat would materialize a 10^1000000 denominator
		// (~20ms per call, ×3 emitters per condition leaf). The zero fold
		// answers int64(0), which compares identically to the float64(0) this
		// literal produced before.
		{"underflow to zero folds to int64 zero", "1e-1000000", int64(0)},
		{"plain zero folds to int64 zero", "0e5", int64(0)},
		// Length cap: an integral value spelled longer than refinableLen keeps
		// the pre-#357 float64 binding rather than paying unbounded big.Rat
		// cost. 42 has spellings far shorter than the cap, so nothing an API
		// caller would write is affected.
		{"over-long integral spelling stays float64", "42." + strings.Repeat("0", 70), float64(42)},
		// Magnitude cap: beyond ±refinableMagnitude no literal can denote an
		// in-range int64, so refinement is pointless work.
		{"beyond magnitude cap stays float64", "1e19", float64(1e19)},
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
