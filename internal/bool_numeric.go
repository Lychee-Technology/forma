package internal

import "time"

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

func timeToUnixMillisFloat64(value time.Time) float64 {
	return float64(value.UnixMilli())
}

func unixMillisFloat64ToTimeUTC(value float64) time.Time {
	return time.UnixMilli(int64(value)).UTC()
}
