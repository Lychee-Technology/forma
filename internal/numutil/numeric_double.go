package numutil

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/jackc/pgx/v5/pgtype"
)

// NumericToDouble decodes a PostgreSQL NUMERIC to the double the DuckDB
// Postgres scanner read it as before #785: the double nearest its decimal
// value, NaN and ±Infinity kept, and a value past the double range read as
// ±Infinity, which pgx's own NUMERIC-to-float64 conversion refuses. A NULL
// decodes to 0; the caller keeps the validity beside it.
//
// The CDC export stages eav_data.value_numeric through it (#785), so the
// parquet tiers hold its answer, and the hot leg's sort keys derive from it
// (sortkey, design.md §6.1) so that a hot key compares as its next export
// will.
func NumericToDouble(n pgtype.Numeric) (float64, error) {
	switch {
	case !n.Valid:
		return 0, nil
	case n.NaN:
		return math.NaN(), nil
	case n.InfinityModifier == pgtype.Infinity:
		return math.Inf(1), nil
	case n.InfinityModifier == pgtype.NegativeInfinity:
		return math.Inf(-1), nil
	}
	digits := "0"
	if n.Int != nil {
		digits = n.Int.String()
	}
	text := digits + "e" + strconv.FormatInt(int64(n.Exp), 10)
	f, err := strconv.ParseFloat(text, 64)
	if err != nil && !errors.Is(err, strconv.ErrRange) {
		return 0, fmt.Errorf("decode numeric %s as double: %w", text, err)
	}
	// On ErrRange ParseFloat returns the correctly signed infinity (or zero
	// for an underflow), which is the scanner's answer too.
	return f, nil
}
