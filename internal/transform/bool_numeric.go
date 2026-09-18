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

func timeToUnixMillisFloat64(value time.Time) float64 {
	return float64(value.UnixMilli())
}

func unixMillisFloat64ToTimeUTC(value float64) time.Time {
	return time.UnixMilli(int64(value)).UTC()
}

// iso8601Rendering is the image the iso8601 encoding stores for the numeric
// slot of a date/datetime: RFC3339, UTC, whole seconds (the layout has no
// fractional field, and the DuckDB outer select re-derives the same shape,
// #555). The slot holds a whole number of epoch millis — the precision the
// funnel keeps for every date/datetime, bound or not (populateTypedValue and
// ToEAVRecord both normalise a time.Time with UnixMilli) — so the image is
// lossless iff those millis sit on a whole second. It reports false for a
// value off a whole second, and for a slot that is not a whole, finite
// number of millis (a funnel bypass, which int64() would silently round
// before the whole-second test): checkBoundColumnFit refuses the former as
// invalid input and storeWithEncoding refuses both rather than truncate, so
// no path narrows the value silently (#582).
func iso8601Rendering(value float64) (string, bool) {
	if !isWholeMillis(value) {
		return "", false
	}
	t := unixMillisFloat64ToTimeUTC(value)
	if t.Nanosecond() != 0 {
		return "", false
	}
	return t.Format(time.RFC3339), true
}

// isWholeMillis reports whether a numeric slot holds what a date/datetime
// slot always holds after the funnel: a whole, finite number of epoch
// millis. NaN fails the equality; the infinities fail the finiteness test.
func isWholeMillis(value float64) bool {
	return !math.IsInf(value, 0) && value == math.Trunc(value)
}
