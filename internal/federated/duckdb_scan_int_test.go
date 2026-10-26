package federated

import (
	"math"
	"testing"
)

// TestNullDuckDBIntScansWholeDoubles: a numeric attribute bound to an
// integer column reads DOUBLE from DuckDB; sql.NullInt64 refused 1e6 and
// 2^53 ("1e+06" does not parse as an integer), failing the federated read.
func TestNullDuckDBIntScansWholeDoubles(t *testing.T) {
	for _, tc := range []struct {
		src  any
		want int64
	}{
		{float64(7), 7},
		{float64(1e6), 1_000_000},
		{float64(1 << 53), 1 << 53},
		{float64(-1 << 62), -1 << 62},
		{int64(math.MaxInt64), math.MaxInt64},
		{int32(-5), -5},
		{int16(12), 12},
	} {
		var n nullDuckDBInt
		if err := n.Scan(tc.src); err != nil || !n.Valid || n.Int64 != tc.want {
			t.Fatalf("Scan(%T %v) = %d, valid=%t, err=%v; want %d", tc.src, tc.src, n.Int64, n.Valid, err, tc.want)
		}
	}
	var null nullDuckDBInt
	if err := null.Scan(nil); err != nil || null.Valid {
		t.Fatalf("Scan(nil) = valid %t, err %v; want NULL", null.Valid, err)
	}
	for _, bad := range []float64{1001.5, math.NaN(), math.Inf(1), 0x1p63} {
		var n nullDuckDBInt
		if err := n.Scan(bad); err == nil {
			t.Fatalf("Scan(%v) = %d, want an error", bad, n.Int64)
		}
	}
}
