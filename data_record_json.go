package forma

import (
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"time"
)

// DataRecord JSON rendering of date/datetime attributes (#591).
//
// The read path returns every date/datetime attribute as a time.Time (UTC, at
// epoch-millisecond precision). time.Time.MarshalJSON writes RFC3339Nano but
// refuses any year outside 0000 to 9999, while a date bound to a bigint
// column keeps the full int64 epoch-ms range (years -292275055 to 292278994,
// #582) and an unbound one keeps every year its float64 image holds. A record
// carrying such a value could not be encoded at all, and over HTTP the
// failure came after the 2xx status had been written.
//
// Each time.Time in Attributes is rendered by dateValueJSON instead: through
// time.Time.MarshalJSON, byte for byte, whenever that encodes it, and
// otherwise as a JSON string of the exact epoch milliseconds, e.g.
// "9223372036854775807". The write path reads an integer date string as
// epoch milliseconds (transform.toTime), so a caller can send back exactly
// what it read. A JSON number was not chosen: it rounds past 2^53 in common
// clients and is not an accepted date input.

// The instants an int64 of epoch milliseconds names, the same bounds the
// write path's normalisation admits (transform.epochMillisOf). The upper
// bound is the last nanosecond of the last millisecond, which floors to
// MaxInt64.
var (
	minRenderableMillisTime  = time.UnixMilli(math.MinInt64)
	lastRenderableMillisTime = time.UnixMilli(math.MaxInt64).Add(time.Millisecond - time.Nanosecond)
)

// MarshalJSON encodes the record with each date/datetime attribute rendered
// by dateValueJSON. Attributes is walked copy-on-write over the containers
// the read path builds (map[string]any, []any): the caller's maps and slices
// are never modified, and a record without a time.Time is encoded from them
// as is.
func (r DataRecord) MarshalJSON() ([]byte, error) {
	// plainDataRecord has DataRecord's fields and tags but not this method,
	// so encoding it does not recurse.
	type plainDataRecord DataRecord
	r.Attributes, _ = renderDateMap(r.Attributes)
	body, err := json.Marshal(plainDataRecord(r))
	if err != nil {
		return nil, fmt.Errorf("encode record %s of schema %q: %w", r.RowID, r.SchemaName, err)
	}
	return body, nil
}

// dateValueJSON renders one date/datetime attribute value.
type dateValueJSON time.Time

// MarshalJSON renders the value through time.Time.MarshalJSON when that
// encodes it (every instant whose year, in its own location, is 0000 to 9999)
// and otherwise as a JSON string of its epoch milliseconds, floored as the
// write path floors them (#589). A time.Time no int64 of epoch milliseconds
// names cannot come from the read path; it is refused with the range rather
// than rendered as a wrapped value.
func (d dateValueJSON) MarshalJSON() ([]byte, error) {
	value := time.Time(d)
	if rendered, err := value.MarshalJSON(); err == nil {
		return rendered, nil
	}
	if value.Before(minRenderableMillisTime) || value.After(lastRenderableMillisTime) {
		return nil, fmt.Errorf("date value %s cannot be rendered as epoch milliseconds, which name instants from %s to %s",
			value.Format(time.RFC3339Nano),
			minRenderableMillisTime.UTC().Format(time.RFC3339Nano),
			lastRenderableMillisTime.UTC().Format(time.RFC3339Nano))
	}
	return strconv.AppendQuote(nil, strconv.FormatInt(value.UnixMilli(), 10)), nil
}

// renderDateValue returns value with every time.Time in it replaced by its
// dateValueJSON, and whether anything was replaced.
func renderDateValue(value any) (any, bool) {
	switch v := value.(type) {
	case time.Time:
		return dateValueJSON(v), true
	case map[string]any:
		return renderDateMap(v)
	case []any:
		return renderDateSlice(v)
	default:
		return value, false
	}
}

// renderDateMap is renderDateValue for a map. The map is cloned on the first
// replacement, so an unchanged map is returned as is.
func renderDateMap(values map[string]any) (map[string]any, bool) {
	var rendered map[string]any
	for key, value := range values {
		replacement, changed := renderDateValue(value)
		if !changed {
			continue
		}
		if rendered == nil {
			rendered = maps.Clone(values)
		}
		rendered[key] = replacement
	}
	if rendered == nil {
		return values, false
	}
	return rendered, true
}

// renderDateSlice is renderDateValue for a list, cloned on the first
// replacement like renderDateMap.
func renderDateSlice(values []any) ([]any, bool) {
	var rendered []any
	for i, value := range values {
		replacement, changed := renderDateValue(value)
		if !changed {
			continue
		}
		if rendered == nil {
			rendered = slices.Clone(values)
		}
		rendered[i] = replacement
	}
	if rendered == nil {
		return values, false
	}
	return rendered, true
}
