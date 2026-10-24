package transform

import (
	"fmt"
	"math"
	"strconv"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// The image of a smallint or integer bound to a wider column (#708).
//
// A smallint or integer may be bound to a main column wider than its
// declared type: integer to bigint_* or double_*, smallint to integer_*,
// bigint_* or double_*. The write funnel judges the declared type first
// (checkStorageFit), so the column only ever receives a whole number inside
// that type's range. A column holding anything else was written before the
// funnel or around it, by direct SQL.
//
// The OLTP read used to convert such an image with Go's int32()/int16(): an
// out-of-range image wrapped and a fraction truncated. An update that did
// not name the attribute merged that converted value into the document it
// wrote, the funnel admitted it, and the column was overwritten with a value
// nobody sent. The read now refuses the image instead, as it refuses a
// bigint image that names no int64 (#590): a plain consistency error, so an
// unrelated update is blocked, while an update naming the attribute
// discards the stored image undecoded (resolveStoredValues) and repairs the
// row. The width census reports these rows (widthaudit, #708).
//
// The federated legs are unchanged: they read the column at its declared
// type through TRY_CAST, as the CDC export does (#627), so an out-of-range
// image reads as absent and a fraction rounds half to even. That difference
// from the OLTP route is transitional and documented; #621 owns a
// row-scoped representation both routes could share.
//
// An EAV-only smallint or integer keeps its #384 conversion: eav_data
// records carry no BoundColumn and never reach this check.

// checkBoundIntegerImage refuses the image a smallint or integer record read
// from a bound column holds when the declared type cannot hold it: a
// fraction, a magnitude past the type's range, NaN or an infinity. The
// comparison is exact: the type's bounds are float64 integers, and a
// bigint_* record judges its exact int64. The caller wraps the error with
// the record's schema, row and attribute (decodeEAVRecord).
func checkBoundIntegerImage(record model.EAVRecord, vt forma.ValueType) error {
	if record.BoundColumn == "" || record.ValueNumeric == nil || !isBoundIntegerType(vt) {
		return nil
	}
	lo, hi := integerRange(vt)
	image := *record.ValueNumeric
	text := boundImageText(image)
	fits := isWholeNumber(image) && image >= float64(lo) && image <= float64(hi)
	if record.ValueInt64 != nil {
		exact := *record.ValueInt64
		text = strconv.FormatInt(exact, 10)
		fits = exact >= lo && exact <= hi
	}
	if fits {
		return nil
	}
	return fmt.Errorf("stored %s image %s in bound column %s does not fit declared type %s (expected a whole number in [%d, %d]); "+
		"rewrite the attribute to repair the row (#708)", vt, text, record.BoundColumn, vt, lo, hi)
}

func isBoundIntegerType(vt forma.ValueType) bool {
	return vt == forma.ValueTypeSmallInt || vt == forma.ValueTypeInteger
}

// boundImageText names a stored double image the way the width census does:
// plain digits for a whole number, the shortest round-trip form for a
// fraction, exponent form from 1e21, and the PostgreSQL spelling of the
// non-finite values.
func boundImageText(image float64) string {
	switch {
	case math.IsNaN(image):
		return "NaN"
	case math.IsInf(image, 1):
		return "Infinity"
	case math.IsInf(image, -1):
		return "-Infinity"
	}
	return formatFitValue(image)
}
