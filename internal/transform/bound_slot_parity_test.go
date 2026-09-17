package transform

import (
	"fmt"
	"math"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// parityColumns is one column per main column type; ColumnType() is derived
// from the name, so one representative per family covers the dispatch.
var parityColumns = []forma.MainColumn{
	forma.MainColumnText01, forma.MainColumnSmallint01, forma.MainColumnInteger01,
	forma.MainColumnBigint01, forma.MainColumnDouble01, forma.MainColumnUUID01,
}

var parityEncodings = []forma.MainColumnEncoding{
	forma.MainColumnEncodingDefault, forma.MainColumnEncodingUnixMs, forma.MainColumnEncodingISO8601,
	forma.MainColumnEncodingBoolInt, forma.MainColumnEncodingBoolText,
}

// paritySamples is one typed value per EAVRecord slot family. Every numeric
// image fits a smallint so the width rule (covered by the other tests in
// this package) never decides a pair here; only slot placement does.
var paritySamples = []struct {
	vt    forma.ValueType
	value any
}{
	{forma.ValueTypeText, "abc"},
	{forma.ValueTypeUUID, "0190f3a4-2f1e-7c3b-9a2d-1b2c3d4e5f60"},
	{forma.ValueTypeNumeric, 1},
	{forma.ValueTypeDate, "1970-01-01T00:00:01Z"},
	{forma.ValueTypeBool, true},
}

func newEmptyPersistentRecord() *model.PersistentRecord {
	return &model.PersistentRecord{
		TextItems: map[string]string{}, Int16Items: map[string]int16{}, Int32Items: map[string]int32{},
		Int64Items: map[string]int64{}, UUIDItems: map[string]uuid.UUID{}, Float64Items: map[string]float64{},
	}
}

// slotsHolding lists the PersistentRecord maps that carry col; the main-table
// writer accepts a key only from the map of the column's own type.
func slotsHolding(record *model.PersistentRecord, col string) []forma.MainColumnType {
	var held []forma.MainColumnType
	if _, ok := record.TextItems[col]; ok {
		held = append(held, forma.MainColumnTypeText)
	}
	if _, ok := record.Int16Items[col]; ok {
		held = append(held, forma.MainColumnTypeSmallint)
	}
	if _, ok := record.Int32Items[col]; ok {
		held = append(held, forma.MainColumnTypeInteger)
	}
	if _, ok := record.Int64Items[col]; ok {
		held = append(held, forma.MainColumnTypeBigint)
	}
	if _, ok := record.Float64Items[col]; ok {
		held = append(held, forma.MainColumnTypeDouble)
	}
	if _, ok := record.UUIDItems[col]; ok {
		held = append(held, forma.MainColumnTypeUUID)
	}
	return held
}

// renderedNumeric is what an encoding's rendering keeps of the numeric slot
// when it is read back: the bool renderings collapse it to 0/1 and the
// RFC3339 rendering keeps whole seconds; the rest are exact.
func renderedNumeric(enc forma.MainColumnEncoding, v float64) float64 {
	switch enc {
	case forma.MainColumnEncodingBoolInt, forma.MainColumnEncodingBoolText:
		return boolToFloat64(float64ToBool(v))
	case forma.MainColumnEncodingISO8601:
		return math.Trunc(v/1000) * 1000
	}
	return v
}

// #559: checkBoundColumnFit and storeWithEncoding are one dispatch written
// twice, keyed on (encoding, ColumnType()). Over every encoding, every column
// type and every slot family: the check admits a value iff the store writes
// it, and what it writes lands in the map of the column it is bound to (the
// only key the main-table writer accepts) and reads back from there.
func TestBoundColumnSlotParity(t *testing.T) {
	tr := &persistentRecordTransformer{}
	for _, enc := range parityEncodings {
		for _, col := range parityColumns {
			for _, sample := range paritySamples {
				binding := &forma.MainColumnBinding{ColumnName: col, Encoding: enc}
				name := fmt.Sprintf("%s/%s/%s", enc, col, sample.vt)
				t.Run(name, func(t *testing.T) {
					var rec model.EAVRecord
					_, err := populateTypedValue(&rec, "v", sample.value, forma.AttributeMetadata{AttributeID: 9, ValueType: sample.vt})
					require.NoError(t, err)

					checkErr := checkBoundColumnFit(&rec, sample.vt, binding)
					record := newEmptyPersistentRecord()
					stored, storeErr := tr.storeWithEncoding(record, rec, binding)
					if checkErr != nil {
						require.False(t, stored, "check refused (%v) but the store wrote", checkErr)
						require.Empty(t, slotsHolding(record, string(col)))
						return
					}
					require.NoError(t, storeErr, "check admitted but the store refused")
					require.True(t, stored, "check admitted but the store found no value")
					require.Equal(t, []forma.MainColumnType{binding.ColumnType()}, slotsHolding(record, string(col)),
						"the value must land in the column's own map and nowhere else")

					meta := forma.AttributeMetadata{AttributeID: 9, ValueType: sample.vt, ColumnBinding: binding}
					got, err := tr.readFromMainColumn(record, meta, binding)
					require.NoError(t, err)
					require.NotNil(t, got, "an admitted value must read back from the column's slot")
					if rec.ValueText != nil {
						require.Equal(t, *rec.ValueText, *got.ValueText)
						return
					}
					require.NotNil(t, got.ValueNumeric)
					require.Equal(t, renderedNumeric(enc, *rec.ValueNumeric), *got.ValueNumeric)
				})
			}
		}
	}
}

// #559: an explicit encoding that reaches the store on a column it cannot
// render into is a funnel bypass; the error names the disagreement rather
// than claiming the slot was empty.
func TestStoreWithEncoding_ColumnMismatchIsAnError(t *testing.T) {
	tr := &persistentRecordTransformer{}
	cases := []struct {
		binding forma.MainColumnBinding
		value   any
		vt      forma.ValueType
		want    string
	}{
		{forma.MainColumnBinding{ColumnName: forma.MainColumnBigint01, Encoding: forma.MainColumnEncodingISO8601},
			"2024-01-01T00:00:00Z", forma.ValueTypeDate, "encoding iso8601 renders a text value"},
		{forma.MainColumnBinding{ColumnName: forma.MainColumnSmallint01, Encoding: forma.MainColumnEncodingBoolText},
			true, forma.ValueTypeBool, "encoding bool_text renders a text value"},
		{forma.MainColumnBinding{ColumnName: forma.MainColumnText01, Encoding: forma.MainColumnEncodingUnixMs},
			"2024-01-01T00:00:00Z", forma.ValueTypeDate, "encoding unix_ms renders a numeric value"},
		{forma.MainColumnBinding{ColumnName: forma.MainColumnUUID01, Encoding: forma.MainColumnEncodingBoolInt},
			true, forma.ValueTypeBool, "encoding bool_smallint renders a numeric value"},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s/%s", tc.binding.Encoding, tc.binding.ColumnName), func(t *testing.T) {
			var rec model.EAVRecord
			_, err := populateTypedValue(&rec, "v", tc.value, forma.AttributeMetadata{AttributeID: 4, ValueType: tc.vt})
			require.NoError(t, err)
			record := newEmptyPersistentRecord()
			b := tc.binding
			err = tr.storeInMainColumn(record, rec, &b)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
			require.Contains(t, err.Error(), string(b.ColumnName))
			require.Empty(t, slotsHolding(record, string(b.ColumnName)))
		})
	}
}
