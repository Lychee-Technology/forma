package transform

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/numutil"
)

// Epoch-millis helpers for date/datetime values (#582, #592).
//
// The write funnels (populateTypedValue, ToEAVRecord) normalise every
// accepted input into one exact int64 of epoch millis (invariant A). That
// int64 is the logical value of the attribute (invariant B): the EAVRecord
// carries it in ValueInt64 and derives the float64 ValueNumeric image from
// it. Each physical destination then admits the subset of int64 values it
// persists and reads back unchanged (invariant C), through one function the
// fit check and the store share, and the read side of each accepts exactly
// that subset, so a row that reads can always be rewritten:
//
//   - eav_data.value_numeric and double_* columns keep a float64 image, exact
//     for |ms| <= 2^53 (checkFloat64ImageFit);
//   - bigint_* columns (default, unix_ms) keep the int64 itself;
//   - text_* columns with iso8601 keep an RFC3339 image at whole seconds
//     within the layout's four-digit year (iso8601.Image, shared with the
//     query-filter binders, #588).
//
// The read side of a float64 destination judges the image the read query
// emitted, never a narrowed one (#592): the exact token where the route
// carries it (EAVRecord.ValueNumericRaw, from the Postgres JSON_AGG and the
// DuckDB attributes_json), the float64 otherwise (a double_* column). The
// DuckDB scan of the hot tier and the CDC export still read an off-contract
// NUMERIC narrowed before Forma sees it (#592 ruling, #621);
// validate-schema-consistency names those rows.

// The instants an int64 of epoch millis names: the slot every date/datetime
// is normalised into (value_numeric and the exact value_int64 sidecar).
// UnixMilli floors an instant to its millisecond, so the instants that
// normalise into the range run from the first millisecond's instant to the
// last nanosecond of the last millisecond, not to that millisecond's instant
// (#587 review).
var (
	minEpochMillisTime  = time.UnixMilli(math.MinInt64)
	maxEpochMillisTime  = time.UnixMilli(math.MaxInt64)
	lastEpochMillisTime = maxEpochMillisTime.Add(time.Millisecond - time.Nanosecond)
)

// epochMillisOf is the write funnels' normalisation of a time.Time into the
// epoch-millis slot. time.Time.UnixMilli is undefined outside the int64
// millis range and wraps there (year 73069258127 came out as
// 1970-04-08T20:07:28Z), so an extreme but valid time.Time used to be stored
// as an unrelated in-range instant before any fit decision could see it
// (#587 review). The comparison is on the instant, so it is exact for every
// time.Time and zone; a value past either end is refused as the caller's
// value, never narrowed.
//
// Precision finer than a millisecond is floored to the millisecond (#589
// ruling). Epoch millis are the precision of the type itself — every tier
// and the read path carry them — so the floor is the type's contract for
// every destination, bound or not, and not a narrowing any one destination
// could refuse the way #459 and #582 refuse a value its physical destination
// cannot hold. The floor is toward the past (UnixMilli), which in the
// RFC3339 spelling is "drop the digits after the third fractional digit" for
// pre-epoch instants too; the bound is on the floored millis, so an instant
// inside the last millisecond is admitted as MaxInt64, the same value its
// epoch-ms string normalises to.
func epochMillisOf(value time.Time) (int64, error) {
	if value.Before(minEpochMillisTime) || value.After(lastEpochMillisTime) {
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

// hasEpochMillis reports whether a date/datetime record carries a value in
// either epoch-millis slot. It is the one notion of "populated" every
// destination's check and store share, so a sidecar-only record (a bypass
// shape; the funnels fill both slots) is judged and written by the same
// rule on both sides rather than refused by one and rendered by the other.
func hasEpochMillis(attr *model.EAVRecord) bool {
	return attr.ValueInt64 != nil || attr.ValueNumeric != nil
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

// instantOfStoredImage is the read side's inverse of float64ImageOf: the
// instant a persisted float64 image (eav_data.value_numeric, a double_*
// column) names. It admits exactly the images float64ImageOf writes, a whole
// number with |ms| <= 2^53, so what a float64 destination reads back is what
// it admits, and a row that reads can always be rewritten: an update
// reconstructs the whole document and re-enters the write funnel, so an image
// the read accepted and the write refused would fail an update that never
// mentioned the attribute, as the caller's fault (#587 review). An image
// outside that set (a row written before #592, or by hand) is a storage
// consistency error naming the rule it breaks, never the wrapped, rounded or
// tier-dependent instant int64() would invent; validate-schema-consistency
// finds such rows before the upgrade.
//
// The image judged is the token the read query emitted when the record
// carries one (ValueNumericRaw): its float64 has already rounded 2^53+1 to
// 2^53 and a fraction just below 2^53 to a whole number, so a rule applied
// to it could not tell those images from valid ones (#592).
func instantOfStoredImage(record *model.EAVRecord) (time.Time, error) {
	if record.ValueNumericRaw == "" {
		return unixMillisFloat64ToTimeUTC(*record.ValueNumeric)
	}
	ms, err := wholeMillisOfToken(record.ValueNumericRaw)
	if err != nil {
		return time.Time{}, err
	}
	return instantOfImageMillis(ms, record.ValueNumericRaw)
}

// unixMillisFloat64ToTimeUTC applies instantOfStoredImage's rule to a float64
// image without a token: a double_* column, which keeps no more than its
// float64, or a record built in memory.
func unixMillisFloat64ToTimeUTC(value float64) (time.Time, error) {
	if !isWholeMillis(value) || !inInt64Range(value) {
		return time.Time{}, fmt.Errorf("stored value %s names no epoch millisecond instant", describeEpochMillis(value))
	}
	return instantOfImageMillis(int64(value), formatFitValue(value))
}

// instantOfImageMillis bounds a whole stored image by the range the float64
// destinations admit; stored is the image as the message quotes it.
func instantOfImageMillis(ms int64, stored string) (time.Time, error) {
	if !fitsFloat64Image(ms) {
		return time.Time{}, fmt.Errorf("stored value %s (%s) is outside the epoch milliseconds a float64 image keeps exactly (up to %d, 2^53); rewrite it with an update that names the attribute, or bind the attribute to a bigint column (docs/schema-consistency-migration.md)",
			stored, unixMillisToTimeUTC(ms).Format(time.RFC3339Nano), maxFloat64ImageMillis)
	}
	return unixMillisToTimeUTC(ms), nil
}

// wholeMillisOfToken reads a value_numeric token exactly. Postgres renders a
// NUMERIC as its decimal digits ("9007199254740993", "1704067200123.000"), a
// DOUBLE PRECISION in its shortest form ("9.007199254740992e+15"), and the
// non-finite values as "NaN"/"Infinity"/"-Infinity"; DuckDB renders the
// unified BIGINT as digits. A plain integer is the common case. Anything else
// is judged on its exact rational value: a fraction, NaN, an infinity or a
// magnitude past int64 names no instant, whatever its float64 rounds to. The
// float64 pre-check bounds the magnitude before big.Rat expands the token,
// and refuses only a token its float64 proves past int64: a scaled MaxInt64
// ("9223372036854775807.000") rounds to 2^63 as the values past it do, so
// the exact value tells them apart (#622 review).
func wholeMillisOfToken(token string) (int64, error) {
	if ms, err := strconv.ParseInt(token, 10, 64); err == nil {
		return ms, nil
	}
	image, parseErr := strconv.ParseFloat(token, 64)
	if !mayRoundFromInt64(image) {
		return 0, fmt.Errorf("stored value %s (%s) names no epoch millisecond instant", token, notAnInstantReason(image, parseErr))
	}
	exact, ok := new(big.Rat).SetString(token)
	switch {
	case !ok:
		return 0, fmt.Errorf("stored value %q is not a number", token)
	case !exact.IsInt():
		return 0, fmt.Errorf("stored value %s (not a whole number of epoch milliseconds) names no epoch millisecond instant", token)
	case !exact.Num().IsInt64():
		return 0, fmt.Errorf("stored value %s (beyond any epoch millisecond instant) names no epoch millisecond instant", token)
	}
	return exact.Num().Int64(), nil
}

// mayRoundFromInt64 reports whether a float64 can be the rounded image of an
// int64. Rounding is monotonic and MaxInt64 rounds up to 2^63, so every int64
// has an image within ±2^63, and a value whose image lies past it (or is
// NaN) is beyond int64 whatever its digits. inInt64Range is the test for a
// float64 that is itself the value; it excludes 2^63, which as an image is
// shared by MaxInt64 and the values just past it.
func mayRoundFromInt64(image float64) bool {
	return math.Abs(image) <= 1<<63
}

// notAnInstantReason is describeEpochMillis's reason for a token whose
// float64 proves it past int64: NaN and the infinity spellings are no whole
// number, while a finite token is beyond any instant even when its float64
// overflowed to an infinity (a NUMERIC 1e400, strconv.ErrRange).
func notAnInstantReason(image float64, parseErr error) string {
	if math.IsNaN(image) || (math.IsInf(image, 0) && parseErr == nil) {
		return "not a whole number of epoch milliseconds"
	}
	return "beyond any epoch millisecond instant"
}

// maxFloat64ImageMillis is the largest magnitude of epoch millis a float64
// image keeps exactly: every integer up to 2^53 has an exact float64 and
// past it some do not, so the contiguous exact set ends here. It is the
// bound of a bigint's image too (#590).
const maxFloat64ImageMillis = int64(numutil.MaxExactFloat64Integer)

// StoredDateImageRefusedSQL is instantOfStoredImage's rule as a Postgres
// predicate over a value_numeric column: true for exactly the images the
// read refuses, a fraction or a magnitude past 2^53, on NUMERIC and DOUBLE
// PRECISION alike, since Postgres orders NaN above every number and
// abs(-Infinity) is Infinity. validate-schema-consistency censuses legacy
// date rows with it (#592), so the census and the read state one rule.
func StoredDateImageRefusedSQL(column string) string {
	return fmt.Sprintf("(%[1]s <> trunc(%[1]s) OR abs(%[1]s) > %[2]d)", column, maxFloat64ImageMillis)
}

// fitsFloat64Image is the one statement of that range: the write side
// (checkFloat64ImageFit) admits a value by it and the read side
// (instantOfStoredImage) accepts a persisted image by it, so the two cannot
// drift apart.
func fitsFloat64Image(ms int64) bool {
	return ms >= -maxFloat64ImageMillis && ms <= maxFloat64ImageMillis
}

// checkFloat64ImageFit refuses a value whose float64 image would not read
// back as the same instant from dest (eav_data.value_numeric or a bound
// double column). The message names the millis, the instant and the way
// out, and is published by the funnel.
func checkFloat64ImageFit(ms int64, vt forma.ValueType, dest string) error {
	if fitsFloat64Image(ms) {
		return nil
	}
	return fmt.Errorf("%s value %d (%s) cannot be stored in %s, which keeps epoch milliseconds exactly up to %d (2^53); bind the attribute to a bigint column for the full int64 range",
		vt, ms, unixMillisToTimeUTC(ms).Format(time.RFC3339Nano), dest, maxFloat64ImageMillis)
}

// float64ImageOf is the image a float64 destination (eav_data.value_numeric,
// a double_* column) stores for a date/datetime record: float64 of the exact
// millis, which checkFloat64ImageFit guarantees reads back as the same
// instant. The fit check and the store both call it, so a record is admitted
// by the one iff the other writes it, and what is written is derived from
// the logical value, never copied from a float slot that may have been
// rounded on its way in (#559 parity, #592).
func float64ImageOf(attr *model.EAVRecord, vt forma.ValueType, dest string) (float64, error) {
	ms, err := exactEpochMillis(attr)
	if err != nil {
		return 0, fmt.Errorf("%s value cannot be stored in %s: %w", vt, dest, err)
	}
	if err := checkFloat64ImageFit(ms, vt, dest); err != nil {
		return 0, err
	}
	return float64(ms), nil
}

// eavValueNumericDest names the unbound destination in fit messages.
const eavValueNumericDest = "eav_data.value_numeric"

// isWholeMillis reports whether a float64 slot holds what a date/datetime
// image always holds after the funnel: a whole, finite number of epoch
// millis. NaN fails the equality; the infinities fail the finiteness test.
func isWholeMillis(value float64) bool {
	return isWholeNumber(value)
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
