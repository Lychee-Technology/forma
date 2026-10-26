package widthaudit

import (
	"math"
	"math/big"
	"strconv"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/stretchr/testify/require"
)

// TestNumericTargetsSelectsStorageThatCanLeaveTheDomain: eav_data and a
// double_* column can hold a non-finite numeric; the integer columns a
// numeric can also bind to cannot, and other types are not numeric.
func TestNumericTargetsSelectsStorageThatCanLeaveTheDomain(t *testing.T) {
	mc := schemameta.NewMetadataCache()
	require.NoError(t, mc.RegisterSchema("prices", 300, forma.SchemaAttributeCache{
		"price":  {AttributeName: "price", AttributeID: 1, ValueType: forma.ValueTypeNumeric},
		"prices": {AttributeName: "prices", AttributeID: 2, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeNumeric},
		"cost": {AttributeName: "cost", AttributeID: 3, ValueType: forma.ValueTypeNumeric,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: "double_01"}},
		"whole": {AttributeName: "whole", AttributeID: 4, ValueType: forma.ValueTypeNumeric,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: "bigint_01"}},
		"qty":    {AttributeName: "qty", AttributeID: 5, ValueType: forma.ValueTypeInteger},
		"counts": {AttributeName: "counts", AttributeID: 6, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeInteger},
	}))
	want := []Target{
		{SchemaID: 300, SchemaName: "prices", AttrID: 1, AttrName: "price", Declared: forma.ValueTypeNumeric},
		{SchemaID: 300, SchemaName: "prices", AttrID: 2, AttrName: "prices", Declared: forma.ValueTypeNumeric},
		{SchemaID: 300, SchemaName: "prices", AttrID: 3, AttrName: "cost", Declared: forma.ValueTypeNumeric, Column: "double_01"},
	}
	require.Equal(t, want, NumericTargets(mc))
}

// TestFloat64OverflowIsWhereAParseOverflows pins the census bound to the
// read path's parse (model.parseValueNumeric): one below it reads as
// MaxFloat64, and it reads as an infinity.
func TestFloat64OverflowIsWhereAParseOverflows(t *testing.T) {
	below := new(big.Int).Sub(mustBigInt(t, float64Overflow), big.NewInt(1)).String()
	image, err := strconv.ParseFloat(below, 64)
	require.NoError(t, err)
	require.Equal(t, math.MaxFloat64, image)

	image, err = strconv.ParseFloat(float64Overflow, 64)
	require.ErrorIs(t, err, strconv.ErrRange)
	require.True(t, math.IsInf(image, 1))
}

func mustBigInt(t *testing.T, s string) *big.Int {
	t.Helper()
	n, ok := new(big.Int).SetString(s, 10)
	require.True(t, ok, s)
	return n
}
