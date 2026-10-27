// Package iso8601 holds the one RFC3339 image the iso8601 main-column
// encoding stores for a date/datetime, and the rule that reads one back. The
// write funnel (internal/transform) renders the stored image through it and
// the query-filter binders (internal/sqlgen) render a filter literal through
// it, so the two cannot drift (#582, #588). The OLTP read and the
// baseline-schema census (internal/widthaudit) both judge a stored image by
// Parse, so the census refuses exactly what the read refuses (#733). It is a leaf package: sqlgen cannot import transform
// (transform → schemameta → sqlgen), so the rule lives below both.
package iso8601

import "time"

// The instants the RFC3339 layout can spell: a four-digit year. time.Format
// writes a wider year ("10000-01-01…", "-0001-12-31…") that the read path's
// time.Parse(time.RFC3339) refuses, so a value outside is refused before the
// store and no image is written that cannot be read back (#587 review).
var (
	minMillis = time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	maxMillis = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC).UnixMilli()
)

// Rule is a property of the RFC3339 image, phrased as the clause the write
// fit message ("… with encoding iso8601, which keeps …"), the store's bypass
// error ("encoding iso8601 keeps … and cannot hold …") and the filter
// refusal ("… with encoding iso8601, which keeps …") attach to the encoding.
// The empty rule means the image holds the value.
type Rule string

const (
	KeepsWholeSeconds  Rule = "keeps whole seconds"
	KeepsFourDigitYear Rule = "keeps years 0000 to 9999 (the RFC3339 four-digit year)"
)

// Image is the rendering the iso8601 encoding stores for the exact epoch
// millis of a date/datetime: RFC3339, UTC, whole seconds (the layout has no
// fractional field, and the DuckDB outer select re-derives the same shape,
// #555), within the layout's four-digit year. It returns the image, or the
// rule the value breaks: millis off a whole second, or an instant outside
// years 0000–9999. The write funnel refuses each as invalid input rather
// than truncate (#582); the filter binders refuse each rather than bind a
// literal the stored image can never equal or order against (#588). The
// image is UTC by construction, never the process zone: time.UnixMilli
// alone renders in time.Local.
func Image(ms int64) (string, Rule) {
	if ms%1000 != 0 {
		return "", KeepsWholeSeconds
	}
	if ms < minMillis || ms > maxMillis {
		return "", KeepsFourDigitYear
	}
	return time.UnixMilli(ms).UTC().Format(time.RFC3339), ""
}

// Parse is the read path's rule for a stored iso8601 image: time.Parse with
// the RFC3339 layout. It is wider than Image (a fraction or an offset reads
// too), since a read admits what it can name, not only what the write
// renders. An image it refuses fails every read of the row.
func Parse(image string) (time.Time, error) {
	return time.Parse(time.RFC3339, image)
}
