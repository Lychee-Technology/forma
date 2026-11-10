package sqlgen

import (
	"fmt"
	"time"
)

// rfc3339OffsetSuffix matches the numeric UTC offset an RFC 3339 image ends
// with (±hh:mm), the one zone spelling rfc3339.Parse applies besides `Z`.
const rfc3339OffsetSuffix = `[+-][0-9]{2}:[0-9]{2}$`

// RFC3339EpochMs renders the read every DuckDB surface applies to a
// date/datetime stored with the rfc3339 encoding (an RFC 3339 string in a
// text column): the hot leg (mainColRFC3339Expr) and the CDC export
// (cdc.castDateMainValue) share it, so the tiers agree on the value. It
// yields epoch-ms BIGINT (#200), and TRY_CAST makes a string that is not a
// timestamp read NULL on every tier alike instead of failing the read (#555).
// The cast keeps the fraction, so the canonical image reads to its
// millisecond (#842).
//
// An image ending in a numeric offset reads through TIMESTAMPTZ, which
// applies the offset as rfc3339.Parse does; the TIMESTAMP cast drops it and
// read 2024-01-02T11:04:06+08:00 as 11:04:06Z (#756). Any other string keeps
// the TIMESTAMP cast: the bundled ICU parses an offset-less string as
// TIMESTAMPTZ in the session TimeZone, which nothing sets, so it would read
// in the host's zone and the reader and the CDC host could disagree.
func RFC3339EpochMs(expr string) string {
	return fmt.Sprintf("CASE WHEN regexp_matches(%[1]s, '%[2]s') THEN epoch_ms(TRY_CAST(%[1]s AS TIMESTAMPTZ)) ELSE epoch_ms(TRY_CAST(%[1]s AS TIMESTAMP)) END",
		expr, rfc3339OffsetSuffix)
}

// The UTC instants the RFC 3339 four-digit year spells with `Z`:
// [0000-01-01T00:00:00.000Z, 10000-01-01T00:00:00.000Z).
var (
	rfc3339UTCMinMillis = time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	rfc3339UTCEndMillis = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
)

// rfc3339ImageFromEpochMs renders the outer-select image of an rfc3339
// read: the epoch-ms verdict spelled as the RFC 3339 string the reader's
// transform.readWithEncoding parses back (rfc3339.Parse), with exactly three
// fractional digits like the write path's image (#555, #842). An instant
// inside the four-digit UTC years is spelled in UTC, which is
// rfc3339.Image's canonical image. DuckDB's %g is the millisecond,
// zero-padded to three digits.
//
// Applying an offset can carry a four-digit-year image past either end of
// that range: 9999-12-31T23:30:00-01:00 is 10000-01-01T00:30:00Z, which
// strftime writes with a five-digit year that rfc3339.Parse refuses, failing
// the whole page while a Get of the row reads it. Such an instant is spelled
// at an offset of 25 hours instead, written -24:60 / +24:60: the widest
// offset rfc3339.Parse applies (time.Parse reads an offset hour up to 24 and
// a minute up to 60), so every instant it can name has a four-digit wall
// clock there. That spelling keeps the three fractional digits too.
func rfc3339ImageFromEpochMs(expr string) string {
	ts := fmt.Sprintf("epoch_ms(CAST(%s AS BIGINT))", expr)
	return fmt.Sprintf("CASE WHEN CAST(%[1]s AS BIGINT) >= %[3]d THEN strftime(%[2]s - INTERVAL 25 HOUR, '%%Y-%%m-%%dT%%H:%%M:%%S.%%g-24:60')"+
		" WHEN CAST(%[1]s AS BIGINT) < %[4]d THEN strftime(%[2]s + INTERVAL 25 HOUR, '%%Y-%%m-%%dT%%H:%%M:%%S.%%g+24:60')"+
		" ELSE strftime(%[2]s, '%%Y-%%m-%%dT%%H:%%M:%%S.%%gZ') END",
		expr, ts, rfc3339UTCEndMillis, rfc3339UTCMinMillis)
}
