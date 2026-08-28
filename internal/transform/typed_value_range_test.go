package transform

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// #384: the EAV write funnel must reject numeric-family values that do not
// fit the declared integer type. eav_data.value_numeric is an unconstrained
// NUMERIC, so this funnel is the only place the declared width can still be
// enforced; anything admitted here must answer identically on every tier.
func TestPopulateTypedValue_DeclaredIntegerFit(t *testing.T) {
	meta := func(vt forma.ValueType) forma.AttributeMetadata {
		return forma.AttributeMetadata{AttributeID: 7, ValueType: vt}
	}
	cases := []struct {
		name    string
		vt      forma.ValueType
		value   any
		wantErr bool
	}{
		{"integer max ok", forma.ValueTypeInteger, float64(math.MaxInt32), false},
		{"integer min ok", forma.ValueTypeInteger, float64(math.MinInt32), false},
		{"integer 2^31 rejected", forma.ValueTypeInteger, float64(math.MaxInt32) + 1, true},
		{"integer 2^32 rejected", forma.ValueTypeInteger, float64(4294967296), true},
		{"integer below min rejected", forma.ValueTypeInteger, float64(math.MinInt32) - 1, true},
		{"integer non-integral rejected", forma.ValueTypeInteger, 1.5, true},
		{"smallint max ok", forma.ValueTypeSmallInt, float64(math.MaxInt16), false},
		{"smallint min ok", forma.ValueTypeSmallInt, float64(math.MinInt16), false},
		{"smallint 40000 rejected", forma.ValueTypeSmallInt, float64(40000), true},
		{"bigint max exact string ok", forma.ValueTypeBigInt, "9223372036854775807", false},
		{"bigint min exact string ok", forma.ValueTypeBigInt, "-9223372036854775808", false},
		{"bigint 2^63 rejected", forma.ValueTypeBigInt, math.Ldexp(1, 63), true},
		{"bigint -2^63 float ok", forma.ValueTypeBigInt, math.Ldexp(-1, 63), false},
		{"bigint 1e18 ok", forma.ValueTypeBigInt, float64(1e18), false},
		{"bigint non-integral rejected", forma.ValueTypeBigInt, 1.5, true},
		{"numeric stays unconstrained", forma.ValueTypeNumeric, math.Ldexp(1, 80), false},
		{"numeric fractional ok", forma.ValueTypeNumeric, 1.5, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var rec model.EAVRecord
			set, err := populateTypedValue(&rec, "qty", tc.value, meta(tc.vt))
			if tc.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, forma.ErrInvalidInput,
					"declared-width rejection must be user-facing invalid input")
				return
			}
			require.NoError(t, err)
			require.True(t, set)
		})
	}
}

// List elements go through the same funnel with the items-typed elemMeta, so
// the declared items type bounds each element (#384).
func TestPopulateTypedValue_ListElementFit(t *testing.T) {
	meta := forma.AttributeMetadata{
		AttributeID: 7,
		ValueType:   forma.ValueTypeList,
		ItemsType:   forma.ValueTypeInteger,
	}
	var rec model.EAVRecord
	rec.ArrayIndices = "0"
	_, err := populateTypedValue(&rec, "levels", float64(4294967296), meta)
	require.Error(t, err)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
}
