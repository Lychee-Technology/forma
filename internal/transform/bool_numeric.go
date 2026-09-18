package transform

import (
	"fmt"
	"math"
	"time"
)

// numericBoolTrueThreshold is the read-side bool rule (#404): a persisted
// numeric image is read as the nearest of 0/1, so float noise around either
// end (0.0001, 0.9999) does not flip the answer. The SQL readers spell the
// same `> 0.5` (sqlgen.BoolTruthiness); keep them in lockstep. The
// write side does not use this tolerance — boolFromAny accepts only the exact
// images 0 and 1.
const numericBoolTrueThreshold = 0.5

func boolToFloat64(value bool) float64 {
	if value {
		return 1.0
	}
	return 0.0
}

func float64ToBool(value float64) bool {
	return value > numericBoolTrueThreshold
}

// boolFromBoolText reads a bool_text main-column value. The contract is
// "1"/"0" (forma.MainColumnEncodingBoolText); anything else is a storage
// consistency error, not a silent false — a stored "true" used to read back
// as false and be rewritten as "0" by the next partial update (#404).
func boolFromBoolText(value string) (bool, error) {
	switch value {
	case "1":
		return true, nil
	case "0":
		return false, nil
	default:
		return false, fmt.Errorf("bool_text value %q is not the \"1\"/\"0\" the encoding stores", value)
	}
}

// timeToUnixMillisFloat64 is the read side's conversion of an instant the
// storage already holds (an RFC3339 image has a four-digit year, so it is
// inside the int64 millis range by construction). The write funnels go
// through epochMillisOf, which refuses an instant outside that range.
func timeToUnixMillisFloat64(value time.Time) float64 {
	return float64(value.UnixMilli())
}

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

func unixMillisFloat64ToTimeUTC(value float64) time.Time {
	return time.UnixMilli(int64(value)).UTC()
}

// The RFC3339 layout has a four-digit year, so the image the iso8601
// encoding stores names an instant from 0000-01-01T00:00:00Z to
// 9999-12-31T23:59:59Z. time.Format writes a wider year ("10000-01-01…",
// "-0001-12-31…") that the read path's time.Parse(time.RFC3339) refuses, so
// such a value would be written and never read back (#587 review). The
// bounds are whole seconds far below 2^53, so the float comparison is exact,
// and a slot inside them converts to int64 without wrapping.
var (
	minISO8601Millis = float64(time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
	maxISO8601Millis = float64(time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC).UnixMilli())
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

// iso8601Rendering is the image the iso8601 encoding stores for the numeric
// slot of a date/datetime: RFC3339, UTC, whole seconds (the layout has no
// fractional field, and the DuckDB outer select re-derives the same shape,
// #555), within the layout's four-digit year. The slot holds a whole number
// of epoch millis — the precision the funnel keeps for every date/datetime,
// bound or not (populateTypedValue and ToEAVRecord both normalise a
// time.Time with UnixMilli) — so the image is lossless iff those millis sit
// on a whole second of a year the reader can parse back. It returns the
// image, or the rule the value breaks: a value off a whole second, a value
// outside years 0000–9999, and a slot that is not a whole, finite number of
// millis (a funnel bypass, which int64() would silently round before the
// whole-second test). checkBoundColumnFit refuses each as invalid input and
// storeWithEncoding refuses each rather than truncate, so no path narrows
// the value silently or writes an image the read path cannot parse (#582).
func iso8601Rendering(value float64) (string, iso8601Rule) {
	if !isWholeMillis(value) {
		return "", iso8601KeepsWholeSeconds
	}
	if value < minISO8601Millis || value > maxISO8601Millis {
		return "", iso8601KeepsFourDigitYear
	}
	t := unixMillisFloat64ToTimeUTC(value)
	if t.Nanosecond() != 0 {
		return "", iso8601KeepsWholeSeconds
	}
	return t.Format(time.RFC3339), ""
}

// isWholeMillis reports whether a numeric slot holds what a date/datetime
// slot always holds after the funnel: a whole, finite number of epoch
// millis. NaN fails the equality; the infinities fail the finiteness test.
func isWholeMillis(value float64) bool {
	return !math.IsInf(value, 0) && value == math.Trunc(value)
}
