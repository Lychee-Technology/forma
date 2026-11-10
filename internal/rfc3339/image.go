// Package rfc3339 holds the one RFC 3339 image the rfc3339 main-column
// encoding stores for a date/datetime, and the rule that reads one back. The
// write funnel (internal/transform) renders the stored image through it and
// the query-filter binders (internal/sqlgen) render a filter literal through
// it, so the two cannot drift (#582, #588). The OLTP read and the
// baseline-schema census (internal/widthaudit) both judge a stored image by
// Parse, so the census refuses exactly what the read refuses (#733); the
// census and the migrate-rfc3339 listing also report an image the read
// parses that is not canonical (Judge). It is a
// leaf package: sqlgen cannot import transform (transform → schemameta →
// sqlgen), so the rule lives below both.
//
// The encoding was named iso8601 and kept whole seconds until #842; the
// migrate-rfc3339 tool rewrites the whole-second images of that encoding
// (LegacyImagePattern) into this package's shape.
package rfc3339

import "time"

// ImageLength is the length of every canonical image,
// YYYY-MM-DDThh:mm:ss.sssZ. One length with the separators at fixed
// positions is what makes text order instant order (#842).
const ImageLength = 24

// layout spells the canonical image: UTC, exactly three fractional digits.
const layout = "2006-01-02T15:04:05.000Z07:00"

// LegacyImagePattern matches the whole-second image the iso8601 encoding
// stored before #842 (YYYY-MM-DDThh:mm:ssZ): the only image the migration
// rewrites, and the one apply-schema refuses to leave behind. It is a POSIX
// regular expression PostgreSQL's ~ operator evaluates.
const LegacyImagePattern = `^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$`

// The instants the four-digit RFC 3339 year can spell in UTC:
// [0000-01-01T00:00:00.000Z, 9999-12-31T23:59:59.999Z].
var (
	minMillis = time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	maxMillis = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli() - 1
)

// Rule is a property of the RFC 3339 image, phrased as the clause the write
// fit message ("… with encoding rfc3339, which keeps …"), the store's bypass
// error ("encoding rfc3339 keeps … and cannot hold …") and the filter
// refusal ("… with encoding rfc3339, which keeps …") attach to the encoding.
// The empty rule means the image holds the value.
type Rule string

// KeepsFourDigitYear is the one rule: RFC 3339 spells no wider year.
const KeepsFourDigitYear Rule = "keeps years 0000 to 9999 (the RFC 3339 four-digit year)"

// Image is the rendering the rfc3339 encoding stores for the exact epoch
// millis of a date/datetime: RFC 3339 in UTC with exactly three fractional
// digits, YYYY-MM-DDThh:mm:ss.sssZ (#842), within the layout's four-digit
// year. It returns the image, or the rule the value breaks: an instant
// outside years 0000–9999. The write funnel refuses it as invalid input
// (#582); the filter binders refuse it rather than bind a literal the stored
// image can never equal or order against (#588). The image is UTC by
// construction, never the process zone: time.UnixMilli alone renders in
// time.Local.
func Image(ms int64) (string, Rule) {
	if ms < minMillis || ms > maxMillis {
		return "", KeepsFourDigitYear
	}
	return time.UnixMilli(ms).UTC().Format(layout), ""
}

// Parse is the read path's rule for a stored rfc3339 image: time.Parse with
// the RFC 3339 layout. It is wider than Image (a fraction of any length, an
// offset, or the pre-#842 whole-second image reads too), since a read admits
// what it can name, not only what the write renders. An image it refuses
// fails every read of the row.
func Parse(image string) (time.Time, error) {
	return time.Parse(time.RFC3339, image)
}
