package transform

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

// #729: an eav_data smallint or integer is judged on the NUMERIC token the
// read query emitted, not its float64. A fraction finer than float64 keeps
// rounds onto a whole number, which the float rule alone would admit.
func TestCheckIntegerImageJudgesTheToken(t *testing.T) {
	near := tokenRecord(t, "1001.0000000000000001")
	require.Equal(t, float64(1001), *near.ValueNumeric, "the float64 image hides the fraction")

	for _, tc := range []struct {
		token    string
		declared forma.ValueType
		fits     bool
	}{
		{"1001.0000000000000001", forma.ValueTypeInteger, false},
		{"2147483647.0000000001", forma.ValueTypeInteger, false},
		{"32767.00000000001", forma.ValueTypeSmallInt, false},
		{"4294967296.0", forma.ValueTypeInteger, false},
		{"1e400", forma.ValueTypeInteger, false},
		{"1001.0", forma.ValueTypeInteger, true},
		{"1e+3", forma.ValueTypeSmallInt, true},
		{"-2147483648", forma.ValueTypeInteger, true},
		{"-32768.000", forma.ValueTypeSmallInt, true},
	} {
		t.Run(string(tc.declared)+"/"+tc.token, func(t *testing.T) {
			err := checkIntegerImage(tokenRecord(t, tc.token), tc.declared)
			if tc.fits {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, "stored "+string(tc.declared)+" image "+tc.token+" in value_numeric does not fit declared type")
		})
	}
}

// Numeric and bigint are not integer widths this check holds: numeric has
// none, and bigint has its own image contract (#590).
func TestCheckIntegerImageIgnoresOtherTypes(t *testing.T) {
	for _, vt := range []forma.ValueType{forma.ValueTypeNumeric, forma.ValueTypeBigInt} {
		require.NoError(t, checkIntegerImage(tokenRecord(t, "1001.5"), vt))
	}
}
