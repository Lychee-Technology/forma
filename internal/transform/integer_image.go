package transform

import (
	"fmt"
	"math"
	"math/big"
	"strconv"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// The stored image of a smallint or integer (#708, #729).
//
// A smallint or integer is stored in eav_data.value_numeric, an
// unconstrained NUMERIC, or in a main column that may be wider than its
// declared type: integer in bigint_* or double_*, smallint in integer_*,
// bigint_* or double_*. The write funnel judges the declared type first
// (checkStorageFit), so storage only ever receives a whole number inside
// that type's range. Storage holding anything else was written before the
// funnel (#384) or around it, by direct SQL.
//
// The OLTP read used to convert such an image with Go's int32()/int16(): an
// out-of-range image wrapped and a fraction truncated. An update that did
// not name the attribute merged that converted value into the document it
// wrote, the funnel admitted it, and storage was overwritten with a value
// nobody sent. The read now refuses the image instead, as it refuses a
// bigint image that names no int64 (#590): a plain consistency error, so an
// unrelated update is blocked, while an update naming the attribute
// discards the stored image undecoded (resolveStoredValues) and repairs the
// row. The width census reports these rows (widthaudit).
//
// The check sits in the decode every route shares. A bound column reaches
// the federated legs through TRY_CAST(<column> AS <declared>), as the CDC
// export does (#627), so an out-of-range image there reads as absent and a
// fraction rounds half to even before Go sees it; that difference from the
// OLTP route is transitional and documented, and #621 owns a row-scoped
// representation both routes could share. An EAV-only attribute reaches the
// federated legs at storage width, DOUBLE (#384), so the federated route
// refuses its out-of-contract image as the OLTP route does.

// checkIntegerImage refuses the image a smallint or integer record holds
// when the declared type cannot hold it: a fraction, a magnitude past the
// type's range, NaN or an infinity. The comparison is exact: a bigint_*
// record judges its exact int64, an eav_data record the NUMERIC token the
// read query emitted (integerTokenFits), and a double_* or integer_* record
// its float64, which holds every whole number of either type exactly. The
// caller wraps the error with the record's schema, row and attribute
// (decodeEAVRecord).
func checkIntegerImage(record model.EAVRecord, vt forma.ValueType) error {
	if record.ValueNumeric == nil || !isDeclaredIntegerType(vt) {
		return nil
	}
	lo, hi := integerRange(vt)
	image := *record.ValueNumeric
	text := boundImageText(image)
	fits := isWholeNumber(image) && image >= float64(lo) && image <= float64(hi)
	switch {
	case record.ValueInt64 != nil:
		exact := *record.ValueInt64
		text = strconv.FormatInt(exact, 10)
		fits = exact >= lo && exact <= hi
	case record.ValueNumericRaw != "":
		text = record.ValueNumericRaw
		fits = integerTokenFits(record.ValueNumericRaw, lo, hi)
	}
	if fits {
		return nil
	}
	source, issue := "value_numeric", "#729"
	if record.BoundColumn != "" {
		source, issue = "bound column "+record.BoundColumn, "#708"
	}
	return fmt.Errorf("stored %s image %s in %s does not fit declared type %s (expected a whole number in [%d, %d]); "+
		"rewrite the attribute to repair the row (%s)", vt, text, source, vt, lo, hi, issue)
}

// integerTokenFits judges a NUMERIC token exactly. Its float64 can round a
// fraction onto a whole number (1001.0000000000000001 reads as 1001), so the
// token's own digits decide. A non-finite spelling names no number.
func integerTokenFits(token string, lo, hi int64) bool {
	if n, err := strconv.ParseInt(token, 10, 64); err == nil {
		return n >= lo && n <= hi
	}
	exact, ok := new(big.Rat).SetString(token)
	if !ok || !exact.IsInt() || !exact.Num().IsInt64() {
		return false
	}
	n := exact.Num().Int64()
	return n >= lo && n <= hi
}

func isDeclaredIntegerType(vt forma.ValueType) bool {
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
