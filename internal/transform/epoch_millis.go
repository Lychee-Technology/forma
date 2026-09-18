package transform

import (
	"fmt"
	"math"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// Epoch-millis helpers for date/datetime values (#582 redesign).
//
// The write funnels (populateTypedValue, ToEAVRecord) normalise every
// accepted input into one exact int64 of epoch millis (invariant A). That
// int64 is the logical value of the attribute (invariant B): the EAVRecord
// carries it in ValueInt64 and derives the float64 ValueNumeric image from
// it. Each physical destination then admits the subset of int64 values it
// persists and reads back unchanged (invariant C), through one function the
// fit check and the store share:
//
//   - eav_data.value_numeric and double_* columns keep a float64 image, exact
//     for |ms| <= 2^53 (checkFloat64ImageFit);
//   - bigint_* columns (default, unix_ms) keep the int64 itself;
//   - text_* columns with iso8601 keep an RFC3339 image at whole seconds
//     within the layout's four-digit year (iso8601Rendering).

// The instants an int64 of epoch millis names: the slot every date/datetime
// is normalised into (value_numeric and the exact value_int64 sidecar).
var (
	minEpochMillisTime = time.UnixMilli(math.MinInt64)
	maxEpochMillisTime = time.UnixMilli(math.MaxInt64)
)

// epochMillisOf is the write funnels' normalisation of a time.Time into the
// epoch-millis slot. time.Time.UnixMilli is undefined outside the int64
// millis range and wraps there (year 73069258127 came out as
// 1970-04-08T20:07:28Z), so an extreme but valid time.Time used to be stored
// as an unrelated in-range instant before any fit decision could see it
// (#587 review). The comparison is on the instant, so it is exact for every
// time.Time and zone; a value past either end is refused as the caller's
// value, never narrowed. Sub-millisecond precision is floored as before
// (#589 owns whether ingestion should refuse it).
func epochMillisOf(value time.Time) (int64, error) {
	if value.Before(minEpochMillisTime) || value.After(maxEpochMillisTime) {
		return 0, fmt.Errorf("time value %s cannot be stored as epoch milliseconds, which name instants from %s to %s",
			value.Format(time.RFC3339Nano),
			minEpochMillisTime.UTC().Format(time.RFC3339Nano),
			maxEpochMillisTime.UTC().Format(time.RFC3339Nano))
	}
	return value.UnixMilli(), nil
}

// setEpochMillis fills both slots of a date/datetime record from the exact
// millis: the sidecar is the logical value, the float64 the derived image.
func setEpochMillis(attr *model.EAVRecord, ms int64) {
	image := float64(ms)
	attr.ValueNumeric = &image
	attr.ValueInt64 = &ms
}

// exactEpochMillis is the one accessor for the logical value of a
// date/datetime record on the write path. Both funnels populate the sidecar,
// so it is the answer whenever present. A record carrying only the float64
// slot bypassed the funnels (a hand-built record, or one read back from a
// float64 image); its value is still exact when the slot is a whole number
// inside the int64 range, because every such float64 is an integer int64
// holds without rounding. Anything else (a fraction, NaN, an infinity, or a
// magnitude int64() would wrap) names no instant and is refused with the
// reason rather than silently converted.
func exactEpochMillis(attr *model.EAVRecord) (int64, error) {
	if attr.ValueInt64 != nil {
		return *attr.ValueInt64, nil
	}
	if attr.ValueNumeric == nil {
		return 0, fmt.Errorf("no epoch millisecond value in the record")
	}
	image := *attr.ValueNumeric
	if !isWholeMillis(image) || !inInt64Range(image) {
		return 0, fmt.Errorf("value %s names no epoch millisecond instant", describeEpochMillis(image))
	}
	return int64(image), nil
}

// inInt64Range reports whether a float64 converts to int64 without wrapping.
// math.MinInt64 converts to exactly -2^63 (a valid value); math.MaxInt64
// rounds up to exactly 2^63, so >= excludes the first float64 that no longer
// fits.
func inInt64Range(value float64) bool {
	return value >= math.MinInt64 && value < math.MaxInt64
}

// unixMillisToTimeUTC names the instant an int64 of epoch millis stands for.
func unixMillisToTimeUTC(ms int64) time.Time {
	return time.UnixMilli(ms).UTC()
}

// unixMillisFloat64ToTimeUTC is the read side's conversion of a persisted
// float64 image (eav_data.value_numeric, a double_* column). The image of an
// admitted value is a whole number within 2^53 (checkFloat64ImageFit), so it
// converts exactly; a legacy row outside that shape is a storage consistency
// error rather than the wrapped or rounded instant int64() would invent.
func unixMillisFloat64ToTimeUTC(value float64) (time.Time, error) {
	if !isWholeMillis(value) || !inInt64Range(value) {
		return time.Time{}, fmt.Errorf("stored value %s names no epoch millisecond instant", describeEpochMillis(value))
	}
	return unixMillisToTimeUTC(int64(value)), nil
}

// maxFloat64ImageMillis is the largest magnitude a float64 image of epoch
// millis keeps exactly across every read route: Postgres renders the float64
// as a decimal, JSON_AGG hands it back as a float64, and DuckDB casts it to
// BIGINT. Every integer up to 2^53 survives all of them unchanged; above it
// only sparse float64-representable integers do, with a decimal image that
// can differ from the value, so the contiguous exact set ends here (#582).
const maxFloat64ImageMillis = int64(1) << 53

// checkFloat64ImageFit refuses a value whose float64 image would not read
// back as the same instant from dest ("eav_data value_numeric" or a bound
// double column). The message names the millis, the instant and the way
// out, and is published by the funnel.
func checkFloat64ImageFit(ms int64, vt forma.ValueType, dest string) error {
	if ms >= -maxFloat64ImageMillis && ms <= maxFloat64ImageMillis {
		return nil
	}
	return fmt.Errorf("%s value %d (%s) cannot be stored in %s, which keeps epoch milliseconds exactly up to %d (2^53); bind the attribute to a bigint column for the full int64 range",
		vt, ms, unixMillisToTimeUTC(ms).Format(time.RFC3339Nano), dest, maxFloat64ImageMillis)
}

// eavValueNumericDest names the unbound destination in fit messages.
const eavValueNumericDest = "eav_data value_numeric"

// The RFC3339 layout has a four-digit year, so the image the iso8601
// encoding stores names an instant from 0000-01-01T00:00:00Z to
// 9999-12-31T23:59:59Z. time.Format writes a wider year ("10000-01-01…",
// "-0001-12-31…") that the read path's time.Parse(time.RFC3339) refuses, so
// such a value would be written and never read back (#587 review).
var (
	minISO8601Millis = time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	maxISO8601Millis = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC).UnixMilli()
)

// iso8601Rule is a property of the RFC3339 image, phrased as the clause the
// fit message ("… with encoding iso8601, which keeps …") and the store's
// bypass error ("encoding iso8601 keeps … and cannot hold …") attach to the
// encoding. The empty rule means the image holds the value.
type iso8601Rule string

const (
	iso8601KeepsWholeSeconds  iso8601Rule = "keeps whole seconds"
	iso8601KeepsFourDigitYear iso8601Rule = "keeps years 0000 to 9999 (the RFC3339 four-digit year)"
)

// iso8601Rendering is the image the iso8601 encoding stores for the exact
// epoch millis of a date/datetime: RFC3339, UTC, whole seconds (the layout
// has no fractional field, and the DuckDB outer select re-derives the same
// shape, #555), within the layout's four-digit year. It returns the image,
// or the rule the value breaks: millis off a whole second, or an instant
// outside years 0000–9999. checkBoundColumnFit refuses each as invalid input
// and storeWithEncoding refuses each rather than truncate, so no path narrows
// the value silently or writes an image the read path cannot parse (#582).
func iso8601Rendering(ms int64) (string, iso8601Rule) {
	if ms%1000 != 0 {
		return "", iso8601KeepsWholeSeconds
	}
	if ms < minISO8601Millis || ms > maxISO8601Millis {
		return "", iso8601KeepsFourDigitYear
	}
	return unixMillisToTimeUTC(ms).Format(time.RFC3339), ""
}

// isWholeMillis reports whether a float64 slot holds what a date/datetime
// image always holds after the funnel: a whole, finite number of epoch
// millis. NaN fails the equality; the infinities fail the finiteness test.
func isWholeMillis(value float64) bool {
	return !math.IsInf(value, 0) && value == math.Trunc(value)
}

// describeEpochMillis renders a float64 slot with the instant it names. A
// slot that is not a whole number of millis names no instant (int64() would
// round it to one that is not the caller's), and one beyond the int64 millis
// time.UnixMilli takes names none either (int64() wraps it, on amd64 to
// MinInt64), so each is described as such rather than as a made-up instant.
// Inside that range the instant is rendered even outside the RFC3339 year
// span, so a refused 253402300800000 shows its 10000-01-01T00:00:00Z.
func describeEpochMillis(value float64) string {
	if !isWholeMillis(value) {
		return formatFitValue(value) + " (not a whole number of epoch milliseconds)"
	}
	if !inInt64Range(value) {
		return formatFitValue(value) + " (beyond any epoch millisecond instant)"
	}
	return fmt.Sprintf("%s (%s)", formatFitValue(value), unixMillisToTimeUTC(int64(value)).Format(time.RFC3339Nano))
}
