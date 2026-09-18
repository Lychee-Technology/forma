package transform

import (
	"fmt"
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

// iso8601Rendering is the image the iso8601 encoding stores for an epoch-ms
// value: RFC3339, UTC, whole seconds (the layout has no fractional field,
// and the DuckDB outer select re-derives the same shape, #555). It reports
// false for an instant off a whole second, which the rendering would
// truncate; checkBoundColumnFit refuses such a value as invalid input and
// storeWithEncoding refuses it as a funnel bypass, so no path narrows it
// silently (#582).
func iso8601Rendering(value float64) (string, bool) {
	t := unixMillisFloat64ToTimeUTC(value)
	if t.Nanosecond() != 0 {
		return "", false
	}
	return t.Format(time.RFC3339), true
}
