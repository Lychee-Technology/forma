package transform

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/numutil"
	"github.com/stretchr/testify/require"
)

// widthInput is a Go numeric width in both shapes ToEAVRecord accepts.
type widthInput struct {
	name    string
	value   any
	pointer any
	nilPtr  any
}

func widthOf[T numutil.NumericScalar](v T) widthInput {
	return widthInput{name: fmt.Sprintf("%T(%v)", v, v), value: v, pointer: &v, nilPtr: (*T)(nil)}
}

// everyNumericWidth lists each Go numeric width the write path accepts (#566),
// at a value every destination type holds.
func everyNumericWidth() []widthInput {
	return []widthInput{
		widthOf(int(1)), widthOf(int8(1)), widthOf(int16(1)), widthOf(int32(1)), widthOf(int64(1)),
		widthOf(uint(1)), widthOf(uint8(1)), widthOf(uint16(1)), widthOf(uint32(1)), widthOf(uint64(1)),
		widthOf(float32(1)), widthOf(float64(1)),
	}
}

// TestEveryWriteFunnelAcceptsEveryNumericWidth is the #566 parity check: the
// numeric funnel, the exact bigint sidecar and the bool funnel read one type
// set, by value and through a pointer, so no width is storable as one logical
// type and refused as another. Each pointer shape refuses nil by name.
func TestEveryWriteFunnelAcceptsEveryNumericWidth(t *testing.T) {
	for _, w := range everyNumericWidth() {
		t.Run(w.name, func(t *testing.T) {
			for _, in := range []any{w.value, w.pointer} {
				image, err := toFloat64ForEAV(in)
				require.NoError(t, err, "toFloat64ForEAV(%T)", in)
				require.Equal(t, 1.0, image)

				exact, ok := toInt64ExactForEAV(in)
				require.True(t, ok, "toInt64ExactForEAV(%T)", in)
				require.Equal(t, int64(1), exact)

				truth, err := boolFromAny(in)
				require.NoError(t, err, "boolFromAny(%T)", in)
				require.True(t, truth)
			}
			for _, vt := range []forma.ValueType{
				forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeNumeric,
			} {
				var attr model.EAVRecord
				_, err := populateTypedValue(&attr, "n", w.value, forma.AttributeMetadata{AttributeID: 9, ValueType: vt})
				require.NoError(t, err, "populateTypedValue as %s", vt)
				require.Equal(t, 1.0, *attr.ValueNumeric)
			}

			pointee := fmt.Sprintf("%T", w.value)
			_, err := toFloat64ForEAV(w.nilPtr)
			require.EqualError(t, err, "nil "+pointee+" pointer")
			_, err = boolFromAny(w.nilPtr)
			require.EqualError(t, err, "nil "+pointee+" pointer")
			_, ok := toInt64ExactForEAV(w.nilPtr)
			require.False(t, ok)
		})
	}
}

// TestUnsignedWidthsAreJudgedOnTheirOwnValue: a declared integer judges an
// unsigned Go value by the integer itself, not by its float64 image. Past
// MaxInt64 it is an integer outside int64 and is refused in the caller's own
// digits (MaxUint64's image is 2^64, which is not what was sent); within
// int64 a bigint carries it exactly on the sidecar even where the image
// rounds.
func TestUnsignedWidthsAreJudgedOnTheirOwnValue(t *testing.T) {
	populate := func(value any, vt forma.ValueType) (model.EAVRecord, error) {
		var attr model.EAVRecord
		_, err := populateTypedValue(&attr, "n", value, forma.AttributeMetadata{AttributeID: 9, ValueType: vt})
		return attr, err
	}
	t.Run("uint64 max refused in its own digits", func(t *testing.T) {
		_, err := populate(uint64(math.MaxUint64), forma.ValueTypeBigInt)
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		require.Contains(t, err.Error(), "value 18446744073709551615 out of range for declared type bigint")
	})
	t.Run("uint just past int64 refused", func(t *testing.T) {
		_, err := populate(uint(math.MaxInt64)+1, forma.ValueTypeInteger)
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		require.Contains(t, err.Error(), "value 9223372036854775808 out of range")
	})
	t.Run("uint16 past smallint refused", func(t *testing.T) {
		_, err := populate(uint16(math.MaxUint16), forma.ValueTypeSmallInt)
		require.ErrorIs(t, err, forma.ErrInvalidInput)
	})
	t.Run("int8 and uint8 edges fit smallint", func(t *testing.T) {
		for _, v := range []any{int8(math.MinInt8), uint8(math.MaxUint8)} {
			_, err := populate(v, forma.ValueTypeSmallInt)
			require.NoError(t, err, "%T(%v)", v, v)
		}
	})
	// The sidecar carries the exact integer, so the eav_data image bound
	// (#590) refuses it naming what was sent, not its rounded image.
	t.Run("uint64 past 2^53 judged on its exact value", func(t *testing.T) {
		_, err := populate(uint64(numutil.MaxExactFloat64Integer+1), forma.ValueTypeBigInt)
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		require.Contains(t, err.Error(), "bigint value 9007199254740993 does not fit eav_data.value_numeric")
	})
}

// TestToEAVRecordUnsignedBigint pins ToEAVRecord's half of the same rule: an
// unsigned bigint within int64 reaches the exact sidecar by value and through
// a pointer; past MaxInt64 it keeps only the float64 image, as an int64-range
// miss always has.
func TestToEAVRecordUnsignedBigint(t *testing.T) {
	converter := NewAttributeConverter(nil)
	record := func(value any) model.EAVRecord {
		rec, err := converter.ToEAVRecord(model.EntityAttribute{
			SchemaID: 1, AttrID: 3, ValueType: forma.ValueTypeBigInt, Value: value,
		}, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, rec.ValueNumeric)
		return rec
	}
	above2p53 := uint64(numutil.MaxExactFloat64Integer + 1)
	for _, in := range []any{above2p53, &above2p53} {
		rec := record(in)
		require.NotNil(t, rec.ValueInt64, "%T", in)
		require.Equal(t, int64(above2p53), *rec.ValueInt64)
	}
	rec := record(uint64(math.MaxUint64))
	require.Nil(t, rec.ValueInt64)
	require.Equal(t, float64(math.MaxUint64), *rec.ValueNumeric)
}

// TestToPersistentRecordPreservesUnsignedBoundBigint: a column-bound bigint
// reads an unsigned Go value through the same exact path as int64.
func TestToPersistentRecordPreservesUnsignedBoundBigint(t *testing.T) {
	tr := NewPersistentRecordTransformer(newBigintPrecisionRegistry())
	for _, tc := range []struct {
		in   any
		want int64
	}{
		{uint64(math.MaxInt64), math.MaxInt64},
		{uint(numutil.MaxExactFloat64Integer + 1), numutil.MaxExactFloat64Integer + 1},
		{uint32(math.MaxUint32), math.MaxUint32},
		{int8(math.MinInt8), math.MinInt8},
	} {
		t.Run(fmt.Sprintf("%T(%v)", tc.in, tc.in), func(t *testing.T) {
			rec, err := tr.ToPersistentRecord(context.Background(), 202, uuid.New(),
				map[string]any{"amount": tc.in})
			require.NoError(t, err)
			require.Equal(t, tc.want, rec.Int64Items["bigint_01"])
		})
	}
}
