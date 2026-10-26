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
