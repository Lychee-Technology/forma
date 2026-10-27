package federated

import (
	"database/sql"
	"fmt"
	"math"
)

// nullDuckDBInt scans a nullable smallint, integer or bigint main column
// from DuckDB. A numeric attribute bound to such a column reads DOUBLE on
// the DuckDB route (its scan type), and database/sql converts a float64
// through its %g text, which refuses every whole value from 1e6 up
// ("1e+06"). A whole float64 in the int64 range converts exactly, so the
// record holds what the OLTP route reads from the column, rounded the same
// way once the value passes ±2^53 (#733). Any other float64 is an error.
type nullDuckDBInt struct {
	Int64 int64
	Valid bool
}

// Scan implements sql.Scanner.
func (n *nullDuckDBInt) Scan(src any) error {
	n.Int64, n.Valid = 0, false
	f, ok := src.(float64)
	if !ok {
		var v sql.NullInt64
		if err := v.Scan(src); err != nil {
			return fmt.Errorf("scan duckdb integer column: %w", err)
		}
		n.Int64, n.Valid = v.Int64, v.Valid
		return nil
	}
	if f != math.Trunc(f) || f < math.MinInt64 || f >= math.MaxInt64 {
		return fmt.Errorf("scan duckdb integer column: %v is not a whole number in the int64 range", f)
	}
	n.Int64, n.Valid = int64(f), true
	return nil
}

// nullDuckDBBigint scans a nullable bigint main column. Under a numeric
// declaration the column reads DOUBLE, and every int64 from 2^63-512 up
// rounds to 2^63 there, as float64() rounds it on the OLTP route: a valid
// numeric value no int64 holds. A whole DOUBLE past the int64 range is
// therefore kept as the float64 Image rather than refused. The record
// carries it as the column's float64 image, which a numeric declaration
// reads as is and a bigint declaration refuses as naming no int64
// (transform.int64FromBigintImage), so it never wraps into an int64.
// Everything else scans as nullDuckDBInt.
type nullDuckDBBigint struct {
	nullDuckDBInt
	Image    float64
	HasImage bool
}

// Scan implements sql.Scanner.
func (n *nullDuckDBBigint) Scan(src any) error {
	n.Image, n.HasImage = 0, false
	if f, ok := src.(float64); ok && !math.IsInf(f, 0) && f == math.Trunc(f) && (f < math.MinInt64 || f >= math.MaxInt64) {
		n.nullDuckDBInt = nullDuckDBInt{}
		n.Image, n.HasImage = f, true
		return nil
	}
	return n.nullDuckDBInt.Scan(src)
}
