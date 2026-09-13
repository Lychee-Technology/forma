package sqlgen

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// TestBoolTruthiness pins the one read-side spelling every SQL reader derives
// a bool from (#404): the stored numeric image is the nearest of 0/1, so
// float noise around either end does not flip the answer. This is the SQL
// image of transform.float64ToBool's threshold; the two must stay in
// lockstep.
func TestBoolTruthiness(t *testing.T) {
	require.Equal(t, "(value_numeric > 0.5)", BoolTruthiness("value_numeric"))
	require.Equal(t, "(x.value_numeric > 0.5)", BoolTruthiness("x.value_numeric"))
}

// TestPgEavLeafPayload_ComparisonLHSUsesBoolTruthiness: the PG-EAV leg
// compares the same truthiness the DuckDB tiers derive, in the same spelling.
func TestPgEavLeafPayload_ComparisonLHSUsesBoolTruthiness(t *testing.T) {
	p := PgEavLeafPayload{ValueColumn: "value_numeric", Truthy: true}
	require.Equal(t, BoolTruthiness("x.value_numeric"), p.ComparisonLHS("x"))
}

// TestConvertPgMainValue_BoolAcceptsEveryOperandSpelling: the pg-main
// main-table route parses a bool operand under the same rule as the EAV and
// DuckDB routes (parseBoolOperand, #384 P2b). Before #404 it ran strconv.Atoi
// alone, so `equals:true` was a 400 on this route and a match on the others.
func TestConvertPgMainValue_BoolAcceptsEveryOperandSpelling(t *testing.T) {
	boolInt := forma.AttributeMetadata{
		ValueType:     forma.ValueTypeBool,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnSmallint01, Encoding: forma.MainColumnEncodingBoolInt},
	}
	boolText := forma.AttributeMetadata{
		ValueType:     forma.ValueTypeBool,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText02, Encoding: forma.MainColumnEncodingBoolText},
	}
	for _, tc := range []struct {
		operand  string
		wantInt  any
		wantText any
	}{
		{operand: "true", wantInt: int64(1), wantText: "1"},
		{operand: "TRUE", wantInt: int64(1), wantText: "1"},
		{operand: "t", wantInt: int64(1), wantText: "1"},
		{operand: "false", wantInt: int64(0), wantText: "0"},
		{operand: "1", wantInt: int64(1), wantText: "1"},
		{operand: "0", wantInt: int64(0), wantText: "0"},
		{operand: "2", wantInt: int64(1), wantText: "1"},
	} {
		t.Run(tc.operand, func(t *testing.T) {
			got, err := ConvertPgMainValue(tc.operand, "flag", boolInt)
			require.NoError(t, err)
			require.Equal(t, tc.wantInt, got)
			got, err = ConvertPgMainValue(tc.operand, "flag", boolText)
			require.NoError(t, err)
			require.Equal(t, tc.wantText, got)
		})
	}
	_, err := ConvertPgMainValue("banana", "flag", boolInt)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
}
