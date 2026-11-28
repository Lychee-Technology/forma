package transform

import (
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
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

// A system column alias is judged like any other image (#745). Admission
// refuses a smallint or integer alias of a system timestamp, so one only
// reaches the decode through metadata no admission saw; it is then refused
// with the redeclaration as its remedy, never read as a wrapped value. Both
// widths on ltbase_schema_id and a bigint alias of a timestamp read exactly.
func TestCheckIntegerImageJudgesSystemColumnAliases(t *testing.T) {
	millis := time.Now().UnixMilli()
	record := &model.PersistentRecord{
		SchemaID: 100, RowID: uuid.New(), CreatedAt: millis, UpdatedAt: millis, DeletedAt: &millis,
	}
	alias := func(vt forma.ValueType, column forma.MainColumn) forma.AttributeMetadata {
		return forma.AttributeMetadata{AttributeID: 9, ValueType: vt, ColumnBinding: &forma.MainColumnBinding{ColumnName: column}}
	}
	for _, vt := range []forma.ValueType{forma.ValueTypeInteger, forma.ValueTypeSmallInt} {
		for _, column := range []forma.MainColumn{forma.MainColumnCreatedAt, forma.MainColumnUpdatedAt, forma.MainColumnDeletedAt} {
			t.Run(string(vt)+"/"+string(column), func(t *testing.T) {
				_, err := StoredAttributeValue(record, alias(vt, column))
				require.ErrorContains(t, err, "system column "+string(column)+" value "+strconv.FormatInt(millis, 10)+
					" does not fit declared type "+string(vt))
				require.ErrorContains(t, err, "redeclare the attribute bigint, numeric, date or datetime (#745)")
				require.NotContains(t, err.Error(), "rewrite the attribute")
			})
		}
	}
	for _, tc := range []struct {
		vt   forma.ValueType
		want any
	}{
		{forma.ValueTypeSmallInt, int16(100)},
		{forma.ValueTypeInteger, int32(100)},
	} {
		value, err := StoredAttributeValue(record, alias(tc.vt, forma.MainColumnSchemaID))
		require.NoError(t, err)
		require.Equal(t, tc.want, value, "%s alias of ltbase_schema_id", tc.vt)
	}
	value, err := StoredAttributeValue(record, alias(forma.ValueTypeBigInt, forma.MainColumnCreatedAt))
	require.NoError(t, err)
	require.Equal(t, millis, value)
}
