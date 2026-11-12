package sortkey

import (
	"regexp"
	"strconv"
	"time"
)

// rfc3339Image matches the images an rfc3339-bound date holds: the
// canonical `YYYY-MM-DDTHH:MM:SS.sssZ` (docs/rfc3339-storage-contract.md),
// the `±24:60` offsets sqlgen writes for an instant outside years 0000 to
// 9999, and the other offset and fraction spellings RFC 3339 allows.
var rfc3339Image = regexp.MustCompile(`^(\d{4})-(\d{2})-(\d{2})[Tt ](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?([Zz]|([+-])(\d{2}):(\d{2}))?$`)

// RFC3339EpochMs is the Go side of sqlgen.RFC3339EpochMs, the read the
// export applies to an rfc3339-bound date (#756): the instant the image
// names, in epoch milliseconds, or NULL (nil) for an image that names none.
// It is exact on the images Forma writes, which are the query contract
// (#763); a non-canonical image is outside that contract, and this read
// does not try to reproduce every spelling DuckDB's TIMESTAMP cast admits.
func RFC3339EpochMs(image string) any {
	m := rfc3339Image.FindStringSubmatch(image)
	if m == nil {
		return nil
	}
	year, month, day := atoi(m[1]), atoi(m[2]), atoi(m[3])
	hour, minute, second := atoi(m[4]), atoi(m[5]), atoi(m[6])
	if month < 1 || month > 12 || hour > 23 || minute > 59 || second > 59 {
		return nil
	}
	t := time.Date(year, time.Month(month), day, hour, minute, second, 0, time.UTC)
	if t.Day() != day || int(t.Month()) != month {
		return nil // a day the month does not have
	}
	micros := t.Unix()*1_000_000 + fractionMicros(m[7])
	if m[9] != "" {
		offset := int64(atoi(m[10])*60+atoi(m[11])) * 60 * 1_000_000
		if m[9] == "+" {
			micros -= offset
		} else {
			micros += offset
		}
	}
	// epoch_ms divides the TIMESTAMP's microseconds, truncating toward zero.
	return micros / 1000
}

// fractionMicros reads a fraction's first six digits, the microseconds a
// DuckDB TIMESTAMP keeps.
func fractionMicros(frac string) int64 {
	for len(frac) < 6 {
		frac += "0"
	}
	v, _ := strconv.ParseInt(frac[:6], 10, 64)
	return v
}

func atoi(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}
