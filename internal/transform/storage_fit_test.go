package transform

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

func boundMeta(vt forma.ValueType, col forma.MainColumn, enc forma.MainColumnEncoding) forma.AttributeMetadata {
	return forma.AttributeMetadata{
		AttributeID:   9,
		ValueType:     vt,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: col, Encoding: enc},
	}
}

// #459: the write funnel's fidelity rule is "fit the physical destination".
// A bound narrow column caps the value at the column's width no matter how
// wide the declared valueType is, so storeInMainColumn's int16()/int32()
// narrowing can never wrap.
func TestCheckStorageFit_BoundColumnWidth(t *testing.T) {
	cases := []struct {
		name    string
		meta    forma.AttributeMetadata
		value   any
		wantErr string // substring of the published message; "" means accepted
	}{
		{"integer declared, smallint column, 40000 rejected",
			boundMeta(forma.ValueTypeInteger, forma.MainColumnSmallint01, forma.MainColumnEncodingDefault), float64(40000),
			"bound column smallint_01 (smallint)"},
		{"numeric declared, integer column, 3e9 rejected",
			boundMeta(forma.ValueTypeNumeric, forma.MainColumnInteger01, forma.MainColumnEncodingDefault), float64(3e9),
			"bound column integer_01 (integer)"},
		{"numeric declared, integer column, 1.5 rejected",
			boundMeta(forma.ValueTypeNumeric, forma.MainColumnInteger01, forma.MainColumnEncodingDefault), 1.5,
			"whole number required"},
		{"bigint declared, integer column, 2^31 rejected",
			boundMeta(forma.ValueTypeBigInt, forma.MainColumnInteger02, forma.MainColumnEncodingDefault), float64(math.MaxInt32) + 1,
			"bound column integer_02"},
		{"numeric declared, smallint column, 4 ok",
			boundMeta(forma.ValueTypeNumeric, forma.MainColumnSmallint02, forma.MainColumnEncodingDefault), 4, ""},
		{"integer declared, integer column, max ok",
			boundMeta(forma.ValueTypeInteger, forma.MainColumnInteger01, forma.MainColumnEncodingDefault), float64(math.MaxInt32), ""},
		{"bigint declared, bigint column, max exact string ok",
			boundMeta(forma.ValueTypeBigInt, forma.MainColumnBigint01, forma.MainColumnEncodingDefault), "9223372036854775807", ""},
		{"numeric declared, double column, 1e300 ok",
			boundMeta(forma.ValueTypeNumeric, forma.MainColumnDouble01, forma.MainColumnEncodingDefault), 1e300, ""},
		{"smallint declared, integer column, still capped by declared type",
			boundMeta(forma.ValueTypeSmallInt, forma.MainColumnInteger01, forma.MainColumnEncodingDefault), float64(40000),
			"declared type smallint"},
		{"date to bigint unix_ms ok",
			boundMeta(forma.ValueTypeDate, forma.MainColumnBigint02, forma.MainColumnEncodingUnixMs), "2024-03-14T09:26:00Z", ""},
		{"bool to smallint bool_smallint ok",
			boundMeta(forma.ValueTypeBool, forma.MainColumnSmallint01, forma.MainColumnEncodingBoolInt), true, ""},
		{"system column binding is not a write destination",
			boundMeta(forma.ValueTypeDate, forma.MainColumnCreatedAt, forma.MainColumnEncodingUnixMs), "2024-03-14T09:26:00Z", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var rec model.EAVRecord
			set, err := populateTypedValue(&rec, "qty", tc.value, tc.meta)
			if tc.wantErr == "" {
				require.NoError(t, err)
				require.True(t, set)
				return
			}
			require.Error(t, err)
			require.ErrorIs(t, err, forma.ErrInvalidInput, "fit rejection must be user-facing invalid input")
			msg, ok := forma.ResolvePublicMessage(err)
			require.True(t, ok, "fit rejection must publish its message")
			require.Contains(t, msg, tc.wantErr)
			require.Contains(t, msg, "attribute 'qty'")
		})
	}
}
