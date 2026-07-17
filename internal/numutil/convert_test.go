package numutil

import (
	"encoding/json"
	"math"
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
		{name: "scientific float64", input: "1e3", expect: float64(1000)},
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
