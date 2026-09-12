package transform

import (
	"math"
	"testing"

	"github.com/google/uuid"
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

// #459 shapes 2 and 3: a valueType whose typed slot does not match the bound
// column's family used to 500 (text→uuid: uuid.Parse in storeInMainColumn)
// or silently drop (text→smallint: nil ValueNumeric, no else branch). Both
// are now refused in the funnel as published invalid input.
func TestCheckStorageFit_FamilyMismatch(t *testing.T) {
	cases := []struct {
		name    string
		meta    forma.AttributeMetadata
		value   any
		wantErr string
	}{
		{"text to uuid column, non-uuid rejected",
			boundMeta(forma.ValueTypeText, forma.MainColumnUUID02, forma.MainColumnEncodingDefault), "abc",
			`text value "abc" is not a UUID: bound column uuid_02 requires one`},
		{"text to uuid column, uuid accepted",
			boundMeta(forma.ValueTypeText, forma.MainColumnUUID02, forma.MainColumnEncodingDefault), "0190f3a4-2f1e-7c3b-9a2d-1b2c3d4e5f60", ""},
		{"text to smallint column rejected, not dropped",
			boundMeta(forma.ValueTypeText, forma.MainColumnSmallint01, forma.MainColumnEncodingDefault), "7",
			"text value cannot be stored in main column smallint_01, which stores a numeric value"},
		{"numeric to text column rejected",
			boundMeta(forma.ValueTypeNumeric, forma.MainColumnText01, forma.MainColumnEncodingDefault), 7,
			"numeric value cannot be stored in main column text_01, which stores a text value"},
		{"text to bigint unix_ms rejected",
			boundMeta(forma.ValueTypeText, forma.MainColumnBigint01, forma.MainColumnEncodingUnixMs), "2024-01-01",
			"text value cannot be stored in main column bigint_01, which stores a date, datetime or bool value"},
		{"uuid to uuid column accepted",
			boundMeta(forma.ValueTypeUUID, forma.MainColumnUUID01, forma.MainColumnEncodingDefault), "0190f3a4-2f1e-7c3b-9a2d-1b2c3d4e5f60", ""},
		{"numeric declared, bound to bigint_01 default encoding, value 1e19 rejected",
			boundMeta(forma.ValueTypeNumeric, forma.MainColumnBigint01, forma.MainColumnEncodingDefault), 1e19,
			"bound column bigint_01 (bigint)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var rec model.EAVRecord
			_, err := populateTypedValue(&rec, "leadId", tc.value, tc.meta)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			msg, ok := forma.ResolvePublicMessage(err)
			require.True(t, ok)
			require.Contains(t, msg, tc.wantErr)
		})
	}
}

// storeInMainColumn is the last line of defense: if a record ever reaches it
// with no value in the slot its column serializes, that is an error, not a
// silent drop (#459 shape 3).
func TestStoreInMainColumn_EmptySlotIsAnError(t *testing.T) {
	tr := &persistentRecordTransformer{}
	record := &model.PersistentRecord{
		TextItems: map[string]string{}, Int16Items: map[string]int16{}, Int32Items: map[string]int32{},
		Int64Items: map[string]int64{}, UUIDItems: map[string]uuid.UUID{}, Float64Items: map[string]float64{},
	}
	attr := model.EAVRecord{AttrID: 4} // no slot populated
	for _, binding := range []forma.MainColumnBinding{
		{ColumnName: forma.MainColumnSmallint01, Encoding: forma.MainColumnEncodingDefault},
		{ColumnName: forma.MainColumnText01, Encoding: forma.MainColumnEncodingDefault},
		{ColumnName: forma.MainColumnUUID01, Encoding: forma.MainColumnEncodingDefault},
		{ColumnName: forma.MainColumnBigint01, Encoding: forma.MainColumnEncodingUnixMs},
		{ColumnName: forma.MainColumnText02, Encoding: forma.MainColumnEncodingBoolText},
	} {
		b := binding
		err := tr.storeInMainColumn(record, attr, &b)
		require.Error(t, err, "binding %+v", b)
		require.Contains(t, err.Error(), string(b.ColumnName))
		require.Contains(t, err.Error(), "attr id 4")
	}
	require.Empty(t, record.TextItems)
	require.Empty(t, record.Int16Items)
}
