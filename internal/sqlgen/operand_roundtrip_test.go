package sqlgen

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

// #384 P1b: a float64 numeric operand must render in a form CAST(... AS
// DOUBLE) recovers bit-identically, or DuckDB equality misses rows the PG
// NUMERIC comparison (which receives the true float64) matches. %.15g dropped
// the 16th-17th significant digits.
func TestDecimalStringRoundTripsFloat64(t *testing.T) {
	cases := []float64{
		0.1234567890123456,   // 16 sig digits — %.15g mangled this
		0.12345678901234567,  // 17 sig digits
		9007199254740993.5,   // beyond 2^53, parses to ...994
		1e-300, 1.5, 3, 1e28, // assorted magnitudes
	}
	for _, v := range cases {
		t.Run(strconv.FormatFloat(v, 'g', -1, 64), func(t *testing.T) {
			s := decimalString(v)
			back, err := strconv.ParseFloat(s, 64)
			require.NoError(t, err)
			require.Equal(t, v, back, "rendered operand %q must recover the identical float64", s)
		})
	}
}

// #384 P2b: both engines parse bool operands under one rule — ParseBool
// spellings plus any integer with the >0 truthiness the PG path always had.
// Before this, "2" bound only on PG and "true" bound only on DuckDB, so the
// same filter erred on exactly one route depending on spelling.
func TestBoolOperandParityAcrossEngines(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"flag": {AttributeID: 7, ValueType: forma.ValueTypeBool},
	}
	cases := []struct {
		literal string
		want    bool
	}{
		{"1", true}, {"0", false},
		{"true", true}, {"false", false},
		{"TRUE", true}, {"t", true},
		{"2", true}, {"-1", false},
	}
	for _, tc := range cases {
		t.Run(tc.literal, func(t *testing.T) {
			cond := &forma.KvCondition{Attr: "flag", Value: "equals:" + tc.literal}
			paramIndex := 1
			dual, err := ToDualClauses(cond, "eav_table", 1, cache, &paramIndex)
			require.NoError(t, err)
			require.Equal(t, []any{int16(7), tc.want}, dual.PgArgs, "pg bind")
			require.Equal(t, []any{tc.want}, dual.DuckArgs, "duck bind")
		})
	}

	// Garbage rejects identically on both routes, as invalid input.
	cond := &forma.KvCondition{Attr: "flag", Value: "equals:maybe"}
	paramIndex := 1
	_, err := ToDualClauses(cond, "eav_table", 1, cache, &paramIndex)
	require.Error(t, err)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
}

// #384 (fourth-review P1): integral operands for the DOUBLE-width classes
// narrow through the same float64 funnel the stored data took, so above 2^53
// the PG bind is the float64 image DuckDB's CAST(? AS DOUBLE) also lands on —
// one verdict on both engines instead of PG-exact-miss vs DuckDB-rounded-hit.
// bigint keeps the exact int64: its storage is BIGINT-backed on every tier.
func TestNarrowEAVNumericOperand(t *testing.T) {
	const above = int64(9007199254740993) // 2^53 + 1
	image := float64(9007199254740992)    // its float64 image
	require.Equal(t, image, NarrowEAVNumericOperand(forma.ValueTypeInteger, above))
	require.Equal(t, image, NarrowEAVNumericOperand(forma.ValueTypeSmallInt, above))
	require.Equal(t, image, NarrowEAVNumericOperand(forma.ValueTypeNumeric, above))
	require.Equal(t, above, NarrowEAVNumericOperand(forma.ValueTypeBigInt, above))
	require.Equal(t, float64(42), NarrowEAVNumericOperand(forma.ValueTypeInteger, 42))
}
