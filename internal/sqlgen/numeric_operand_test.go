package sqlgen

import (
	"math"
	"math/big"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// Stored whole numbers spanning int64: both limits, the 2^31 and 2^53
// neighbourhoods, and the small values fractions fall between.
var wholeNumberSamples = []int64{
	math.MinInt64, math.MinInt64 + 1, -(1 << 53) - 1, -(1 << 53), math.MinInt32, -8, -7, -2, -1, 0, 1, 2, 7, 8,
	math.MaxInt32, math.MaxInt32 + 1, 1 << 53, 1<<53 + 1, math.MaxInt64 - 1, math.MaxInt64,
}

// Numeric operand literals: whole numbers in and beyond int64, fractions of
// both signs and magnitudes, and spellings whose float64 image is whole.
var numericOperandSamples = []string{
	"0", "7", "-7", "7.0", "7.5", "-7.5", "7.9", "-7.9", "0.5", "-0.5", "1e-300", "-1e-300",
	"2147483647", "2147483648", "-2147483649", "9007199254740992", "9007199254740993",
	"9007199254740993.5", "-9007199254740993.5", "9223372036854775807", "-9223372036854775808",
	"9223372036854775807.5", "9223372036854775808", "-9223372036854775809", "1e19", "-1e19",
	"1e30", "-1e30", "1.7976931348623157e308", "-1.7976931348623157e308",
}

var numericSQLOps = []string{"=", "!=", "<", "<=", ">", ">="}

// exactCmp compares a stored whole number with an operand value (an int64 or
// a float64) exactly: big.Float holds both without rounding.
func exactCmp(stored int64, operand any) int {
	a := new(big.Float).SetInt64(stored)
	b := new(big.Float)
	switch o := operand.(type) {
	case int64:
		b.SetInt64(o)
	case float64:
		b.SetFloat64(o)
	default:
		panic("operand must be int64 or float64")
	}
	return a.Cmp(b)
}

func opHolds(sqlOp string, cmp int) bool {
	switch sqlOp {
	case "=":
		return cmp == 0
	case "!=":
		return cmp != 0
	case "<":
		return cmp < 0
	case "<=":
		return cmp <= 0
	case ">":
		return cmp > 0
	case ">=":
		return cmp >= 0
	}
	panic("unknown operator " + sqlOp)
}

// TestWholeNumberComparison_MatchesArithmetic is the oracle for the rewrite:
// for every stored whole number, the rewritten int64 comparison answers what
// exact arithmetic answers for the operand as the binders parse it.
func TestWholeNumberComparison_MatchesArithmetic(t *testing.T) {
	for _, literal := range numericOperandSamples {
		operand, ok, err := numericOperand("probe", literal, forma.ValueTypeInteger)
		require.NoError(t, err, literal)
		require.True(t, ok, literal)
		for _, sqlOp := range numericSQLOps {
			op, n, err := wholeNumberComparison(sqlOp, operand)
			require.NoError(t, err, "%s %s", sqlOp, literal)
			for _, stored := range wholeNumberSamples {
				want := opHolds(sqlOp, exactCmp(stored, operand))
				got := opHolds(op, exactCmp(stored, n))
				require.Equal(t, want, got, "%d %s %s rewritten to %s %d", stored, sqlOp, literal, op, n)
			}
		}
	}
}

func TestWholeNumberComparison_RefusesNonNumbers(t *testing.T) {
	for _, operand := range []any{math.NaN(), math.Inf(1), math.Inf(-1), "7"} {
		_, _, err := wholeNumberComparison("<", operand)
		require.Error(t, err, "%v", operand)
	}
	_, _, err := wholeNumberComparison("LIKE", 7.5)
	require.Error(t, err)
}

// TestNumericOperand_RefusesNonFinite pins the non-finite refusal on every
// binder and every numeric value type: before it, lt:NaN matched every row
// of an EAV attribute (PostgreSQL orders NaN above every number) and failed
// to encode against an entity_main column after the statement started. A
// bigint operand keeps the #502 out-of-range message.
func TestNumericOperand_RefusesNonFinite(t *testing.T) {
	types := []forma.ValueType{forma.ValueTypeNumeric, forma.ValueTypeInteger, forma.ValueTypeSmallInt, forma.ValueTypeBigInt}
	for _, vt := range types {
		meta := forma.AttributeMetadata{ValueType: vt}
		for _, literal := range []string{"NaN", "Inf", "-Inf", "+Inf", "infinity"} {
			_, pgMainErr := ConvertPgMainValue(literal, "probe", meta)
			_, _, pgEavErr := parsePgEavValue("probe", meta, literal)
			_, duckErr := parseDuckDBRawParam(literal, "probe", vt)
			_, listErr := coerceListElement("probe", meta, literal)
			for name, err := range map[string]error{"pg main": pgMainErr, "pg eav": pgEavErr, "duck": duckErr, "list": listErr} {
				require.ErrorIs(t, err, forma.ErrInvalidInput, "%s %s %s", vt, literal, name)
				if vt == forma.ValueTypeBigInt {
					require.Contains(t, err.Error(), "out of range for bigint attribute 'probe'", name)
				}
			}
		}
	}
}

// TestHybridMainComparison_WholeNumberColumns pins the entity_main payload:
// a numeric comparison against an int2/int4/int8 column binds the rewritten
// int64 cast to bigint; a double column and a non-numeric value type keep
// the parsed value and the column-typed placeholder.
func TestHybridMainComparison_WholeNumberColumns(t *testing.T) {
	cases := []struct {
		col, sqlOp string
		vt         forma.ValueType
		value      any
		wantOp     string
		wantValue  any
		wantCast   string
	}{
		{"integer_01", "=", forma.ValueTypeInteger, 7.9, "<", int64(math.MinInt64), "bigint"},
		{"integer_01", "<", forma.ValueTypeInteger, int64(2147483648), "<", int64(2147483648), "bigint"},
		{"smallint_01", ">", forma.ValueTypeSmallInt, -1.5, ">=", int64(-1), "bigint"},
		{"bigint_01", "!=", forma.ValueTypeBigInt, 7.5, "<=", int64(math.MaxInt64), "bigint"},
		{"ltbase_created_at", "<=", forma.ValueTypeBigInt, 1.5, "<=", int64(1), "bigint"},
		{"double_01", "=", forma.ValueTypeNumeric, 7.9, "=", 7.9, ""},
		{"bigint_01", "=", forma.ValueTypeDateTime, int64(1700000000000), "=", int64(1700000000000), ""},
	}
	for _, tc := range cases {
		p := hybridMainComparison(tc.col, tc.vt, tc.sqlOp, tc.value)
		require.NoError(t, p.Err)
		require.Equal(t, tc.wantOp, p.MainSQLOp, "%+v", tc)
		require.Equal(t, tc.wantValue, p.MainValue, "%+v", tc)
		require.Equal(t, tc.wantCast, p.MainCast, "%+v", tc)
		sql, args := p.RenderMain("m.c", func() string { return "$1" })
		want := "m.c " + tc.wantOp + " $1"
		if tc.wantCast != "" {
			want += "::" + tc.wantCast
		}
		require.Equal(t, want, sql)
		require.Equal(t, []any{tc.wantValue}, args)
	}
}

// TestInt64Elems_KeepsWholeImages: a list element equals what an equals
// operand equals, so a float64 element whose image is a whole int64 is kept
// and every other float64 element is dropped.
func TestInt64Elems_KeepsWholeImages(t *testing.T) {
	got, err := int64Elems("probe", []any{int64(7), 7.5, 9007199254740994.0, -9223372036854775808.0, 1e30})
	require.NoError(t, err)
	require.Equal(t, []int64{7, 9007199254740994, math.MinInt64}, got)
}
