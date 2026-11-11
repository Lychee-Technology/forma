package numutil

import (
	"fmt"
	"math"
	"math/bits"
	"testing"

	"github.com/stretchr/testify/require"
)

// widthRow is one Go numeric width as the write path reads it: its float64
// image (Float64, by value and through OptionalFloat64FromPointer) and its
// verdict for an integer destination (ClassifyInt64, hence Int64Exact).
type widthRow struct {
	name    string
	value   any
	pointer func() (float64, bool, error)
	image   float64
	exact   int64
	verdict Integrality
}

func row[T NumericScalar](v T, image float64, exact int64, verdict Integrality) widthRow {
	return widthRow{
		name:    fmt.Sprintf("%T(%v)", v, v),
		value:   v,
		pointer: func() (float64, bool, error) { return OptionalFloat64FromPointer(&v) },
		image:   image,
		exact:   exact,
		verdict: verdict,
	}
}

// numericWidthTable is the one width table of #566: every Go numeric width
// the write path accepts, at its edges. Below 2^53 every integer converts
// exactly; past it the image narrows the same way for int64, uint and uint64,
// while ClassifyInt64 still reads the integer itself. A uint or uint64 above
// MaxInt64 is an integer outside int64, as its decimal spelling would be.
func numericWidthTable() []widthRow {
	const past2p53 = MaxExactFloat64Integer + 1
	rows := []widthRow{
		row(int(-7), -7, -7, IntegralityExact),
		row(int8(math.MinInt8), math.MinInt8, math.MinInt8, IntegralityExact),
		row(int8(math.MaxInt8), math.MaxInt8, math.MaxInt8, IntegralityExact),
		row(int16(math.MinInt16), math.MinInt16, math.MinInt16, IntegralityExact),
		row(int32(math.MaxInt32), math.MaxInt32, math.MaxInt32, IntegralityExact),
		row(int64(past2p53), MaxExactFloat64Integer, past2p53, IntegralityExact),
		row(int64(math.MinInt64), math.MinInt64, math.MinInt64, IntegralityExact),
		row(uint(42), 42, 42, IntegralityExact),
		row(uint8(math.MaxUint8), math.MaxUint8, math.MaxUint8, IntegralityExact),
		row(uint16(math.MaxUint16), math.MaxUint16, math.MaxUint16, IntegralityExact),
		row(uint32(math.MaxUint32), math.MaxUint32, math.MaxUint32, IntegralityExact),
		row(uint64(0), 0, 0, IntegralityExact),
		row(uint64(past2p53), MaxExactFloat64Integer, past2p53, IntegralityExact),
		row(uint64(math.MaxInt64), 1<<63, math.MaxInt64, IntegralityExact),
		row(uint64(math.MaxInt64)+1, 1<<63, 0, IntegralityOutOfRange),
		row(uint64(math.MaxUint64), 1<<64, 0, IntegralityOutOfRange),
		row(float32(-0.5), -0.5, 0, IntegralityFractional),
		row(float64(1<<62), 1<<62, 1<<62, IntegralityExact),
	}
	if bits.UintSize == 64 {
		rows = append(rows,
			row(uint(math.MaxInt64), 1<<63, math.MaxInt64, IntegralityExact),
			row(uint(math.MaxUint), 1<<64, 0, IntegralityOutOfRange))
	}
	return rows
}

func TestNumericWidthTable(t *testing.T) {
	for _, tc := range numericWidthTable() {
		t.Run(tc.name, func(t *testing.T) {
			image, err := Float64(tc.value)
			require.NoError(t, err, "Float64 by value")
			require.Equal(t, tc.image, image, "Float64 by value")

			image, omitted, err := tc.pointer()
			require.NoError(t, err, "Float64 through a pointer")
			require.False(t, omitted)
			require.Equal(t, tc.image, image, "Float64 through a pointer")

			exact, verdict := ClassifyInt64(tc.value)
			require.Equal(t, tc.verdict, verdict, "ClassifyInt64")
			require.Equal(t, tc.exact, exact, "ClassifyInt64")

			exact, ok := Int64Exact(tc.value)
			require.Equal(t, tc.verdict == IntegralityExact, ok, "Int64Exact")
			require.Equal(t, tc.exact, exact, "Int64Exact")
		})
	}
}

// TestNumericWidthTableBoundary pins what stays outside the set: uintptr is
// not a number in any caller's vocabulary, and a []byte is a slice, not a
// uint8.
func TestNumericWidthTableBoundary(t *testing.T) {
	for _, value := range []any{uintptr(1), []byte{1}, []uint64{1}} {
		t.Run(fmt.Sprintf("%T", value), func(t *testing.T) {
			_, err := Float64(value)
			require.EqualError(t, err, fmt.Sprintf("cannot convert %T to float64", value))
			_, verdict := ClassifyInt64(value)
			require.Equal(t, IntegralityUnknown, verdict)
		})
	}
}
