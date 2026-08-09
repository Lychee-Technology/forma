package sqlgen

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// TestNumericBinderTypeParity pins the invariant #355 establishes: for the
// whole numeric family, one literal yields one Go type on every binder. Before
// this, the integer/smallint arm of parseDuckDBRawParam kept float64 while
// ConvertPgMainValue went int64-first, so the two Postgres/DuckDB legs of the
// same predicate carried different values for the same input string.
//
// This changes no query result. Below 2^31, int64 and float64 denote the same
// number so comparisons are identical. Above it, both parameter types raise the
// same conversion error from CAST(? AS INTEGER), so queries fail identically
// either way. What is fixed here is the divergence itself.
func TestNumericBinderTypeParity(t *testing.T) {
	cases := []struct {
		name      string
		valueType forma.ValueType
		literal   string
		want      any
	}{
		{"bigint_bare_integral", forma.ValueTypeBigInt, "9007199254740993", int64(9007199254740993)},
		{"integer_bare_integral", forma.ValueTypeInteger, "42", int64(42)},
		{"integer_decimal_spelling", forma.ValueTypeInteger, "42.0", int64(42)},
		{"integer_exponent_spelling", forma.ValueTypeInteger, "4.2e1", int64(42)},
		{"smallint_bare_integral", forma.ValueTypeSmallInt, "7", int64(7)},
		{"smallint_negative_integral", forma.ValueTypeSmallInt, "-7", int64(-7)},
		{"numeric_fraction_stays_float", forma.ValueTypeNumeric, "2.5", 2.5},
		{"integer_fraction_stays_float", forma.ValueTypeInteger, "2.5", 2.5},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			meta := forma.AttributeMetadata{ValueType: tc.valueType}

			pgValue, err := ConvertPgMainValue(tc.literal, "probe", meta)
			require.NoError(t, err)
			require.Equal(t, tc.want, pgValue, "ConvertPgMainValue")

			duckRaw, err := parseDuckDBRawParam(tc.literal, "probe", tc.valueType)
			require.NoError(t, err)
			require.Equal(t, tc.want, duckRaw, "parseDuckDBRawParam")
		})
	}
}

// TestToDuckDBParamIntegerArmPassesInt64Through guards the hop that would
// otherwise make the parseDuckDBRawParam change a no-op: ToDuckDBParam's
// smallint/integer arm funnelled everything through toOptionalFloat64Param,
// so an exact int64 was converted straight back to float64 one call later
// (predicate_normalizer.go binds the result of ToDuckDBParam, not of
// parseDuckDBRawParam). The bigint/numeric arm never hit this because it exits
// through toDuckDBDecimalParam instead.
func TestToDuckDBParamIntegerArmPassesInt64Through(t *testing.T) {
	for _, vt := range []forma.ValueType{forma.ValueTypeInteger, forma.ValueTypeSmallInt} {
		got, err := ToDuckDBParam(int64(9007199254740993), vt)
		require.NoError(t, err)
		require.Equal(t, int64(9007199254740993), got, "value type %s", vt)
	}
}

// TestToDuckDBParamIntegerArmStillWidensOtherIntegerTypes pins that the
// passthrough is narrow: int/int16/int32 and float64 inputs keep the existing
// float64 conversion, which callers other than the predicate path rely on.
func TestToDuckDBParamIntegerArmStillWidensOtherIntegerTypes(t *testing.T) {
	got, err := ToDuckDBParam(int32(42), forma.ValueTypeInteger)
	require.NoError(t, err)
	require.Equal(t, float64(42), got)

	got, err = ToDuckDBParam(int16(7), forma.ValueTypeSmallInt)
	require.NoError(t, err)
	require.Equal(t, float64(7), got)
}
