package sqlgen

import "fmt"

// iso8601OffsetSuffix matches the numeric UTC offset an RFC3339 image ends
// with (±hh:mm), the one zone spelling iso8601.Parse applies besides `Z`.
const iso8601OffsetSuffix = `[+-][0-9]{2}:[0-9]{2}$`

// ISO8601EpochMs renders the read every DuckDB surface applies to a
// date/datetime stored with the iso8601 encoding (an RFC3339 string in a
// text column): the hot leg (mainColISO8601Expr) and the CDC export
// (cdc.castDateMainValue) share it, so the tiers agree on the value. It
// yields epoch-ms BIGINT (#200), and TRY_CAST makes a string that is not a
// timestamp read NULL on every tier alike instead of failing the read (#555).
//
// An image ending in a numeric offset reads through TIMESTAMPTZ, which
// applies the offset as iso8601.Parse does; the TIMESTAMP cast drops it and
// read 2024-01-02T11:04:06+08:00 as 11:04:06Z (#756). Any other string keeps
// the TIMESTAMP cast: the bundled ICU parses an offset-less string as
// TIMESTAMPTZ in the session TimeZone, which nothing sets, so it would read
// in the host's zone and the reader and the CDC host could disagree.
func ISO8601EpochMs(expr string) string {
	return fmt.Sprintf("CASE WHEN regexp_matches(%[1]s, '%[2]s') THEN epoch_ms(TRY_CAST(%[1]s AS TIMESTAMPTZ)) ELSE epoch_ms(TRY_CAST(%[1]s AS TIMESTAMP)) END",
		expr, iso8601OffsetSuffix)
}
