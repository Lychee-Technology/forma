package forma

import (
	"encoding/json"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// marshalDateAttribute encodes a record whose one attribute is value and
// returns the attribute as it was rendered.
func marshalDateAttribute(t *testing.T, value time.Time) json.RawMessage {
	t.Helper()
	body, err := json.Marshal(DataRecord{SchemaName: "s", Attributes: map[string]any{"at": value}})
	require.NoError(t, err)
	var decoded struct {
		Attributes map[string]json.RawMessage `json:"attributes"`
	}
	require.NoError(t, json.Unmarshal(body, &decoded), "record body %s is not well-formed JSON", body)
	return decoded.Attributes["at"]
}

// TestDataRecordRendersDateOutsideRFC3339YearsAsEpochMillis is #591's
// regression: a bigint-bound date keeps the full int64 epoch-ms range, and
// time.Time.MarshalJSON refused every year outside 0000 to 9999, so encoding
// the record failed. It is rendered as the exact epoch milliseconds, as a
// string the write path reads back as the same value.
func TestDataRecordRendersDateOutsideRFC3339YearsAsEpochMillis(t *testing.T) {
	for _, ms := range []int64{
		math.MaxInt64,     // year 292278994
		math.MinInt64,     // year -292275055
		253402300800000,   // 10000-01-01T00:00:00Z, the first instant past 9999
		-62167219200001,   // the last millisecond of year -1
		math.MaxInt64 - 1, // a neighbour of each end
		math.MinInt64 + 1,
	} {
		t.Run(strconv.FormatInt(ms, 10), func(t *testing.T) {
			rendered := marshalDateAttribute(t, time.UnixMilli(ms).UTC())

			var text string
			require.NoError(t, json.Unmarshal(rendered, &text), "rendered %s is not a JSON string", rendered)
			assert.Equal(t, strconv.FormatInt(ms, 10), text)
		})
	}
}

// TestDataRecordRendersRFC3339DatesUnchanged pins that every value
// time.Time.MarshalJSON encodes is rendered byte for byte as before #591,
// including a zone the read path never produces and sub-millisecond digits a
// Go caller can hold.
func TestDataRecordRendersRFC3339DatesUnchanged(t *testing.T) {
	plus2 := time.FixedZone("plus2", 2*60*60)
	for name, value := range map[string]time.Time{
		"epoch":             time.UnixMilli(0).UTC(),
		"millis":            time.UnixMilli(1704164645123).UTC(),
		"last ms of 9999":   time.UnixMilli(253402300799999).UTC(),
		"first ms of 0000":  time.UnixMilli(-62167219200000).UTC(),
		"pre-epoch":         time.UnixMilli(-1).UTC(),
		"offset zone":       time.Date(2024, 1, 2, 3, 4, 5, 123e6, plus2),
		"nanoseconds":       time.Date(2024, 1, 2, 3, 4, 5, 123456789, time.UTC),
		"zero value":        {},
		"local year 0000":   time.Date(-1, 12, 31, 23, 0, 0, 0, time.UTC).In(plus2),
		"local 9999 in UTC": time.Date(9999, 12, 31, 23, 0, 0, 0, time.UTC),
	} {
		t.Run(name, func(t *testing.T) {
			want, err := value.MarshalJSON()
			require.NoError(t, err)
			assert.Equal(t, string(want), string(marshalDateAttribute(t, value)))
		})
	}
}

// TestDataRecordRendersZoneLocalYearPast9999AsEpochMillis: time.Time.MarshalJSON
// judges the year in the time's own location, so an instant inside 9999 in
// UTC is refused once its zone puts it in 10000. The epoch milliseconds name
// the instant whatever the zone.
func TestDataRecordRendersZoneLocalYearPast9999AsEpochMillis(t *testing.T) {
	value := time.Date(9999, 12, 31, 23, 30, 0, 0, time.UTC).In(time.FixedZone("plus2", 2*60*60))
	_, err := value.MarshalJSON()
	require.Error(t, err, "precondition: the zone-local year is past 9999")

	assert.Equal(t, strconv.Quote(strconv.FormatInt(value.UnixMilli(), 10)), string(marshalDateAttribute(t, value)))
}

// TestDataRecordRendersTheEpochMillisBoundsExactly: the instants that floor
// into the int64 range render as its ends, and the first instants past them
// are refused naming the range, never rendered as a wrapped value. Only a
// hand-built record can carry such an instant: the read path builds every
// date from an int64 of epoch milliseconds.
func TestDataRecordRendersTheEpochMillisBoundsExactly(t *testing.T) {
	assert.Equal(t, `"9223372036854775807"`, string(marshalDateAttribute(t, lastRenderableMillisTime)))
	assert.Equal(t, `"-9223372036854775808"`, string(marshalDateAttribute(t, minRenderableMillisTime)))

	for name, value := range map[string]time.Time{
		"past MaxInt64":   lastRenderableMillisTime.Add(time.Nanosecond),
		"before MinInt64": minRenderableMillisTime.Add(-time.Nanosecond),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := json.Marshal(DataRecord{Attributes: map[string]any{"at": value}})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "cannot be rendered as epoch milliseconds")
		})
	}
}

// TestDataRecordRendersNestedDatesWithoutMutatingAttributes covers the
// containers the read path builds for nested objects and lists, and pins that
// the caller's record is left as it was.
func TestDataRecordRendersNestedDatesWithoutMutatingAttributes(t *testing.T) {
	far := time.UnixMilli(math.MaxInt64).UTC()
	near := time.UnixMilli(1704164645000).UTC()
	inner := map[string]any{"at": far, "label": "x"}
	list := []any{far, "y", map[string]any{"at": near}}
	untouched := map[string]any{"n": int64(5)}
	record := &DataRecord{
		SchemaName: "s",
		RowID:      uuid.MustParse("01890000-0000-7000-8000-000000000001"),
		Attributes: map[string]any{"inner": inner, "list": list, "untouched": untouched, "top": far},
	}

	body, err := json.Marshal(record)
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"schema_name": "s",
		"row_id": "01890000-0000-7000-8000-000000000001",
		"attributes": {
			"inner": {"at": "9223372036854775807", "label": "x"},
			"list": ["9223372036854775807", "y", {"at": "2024-01-02T03:04:05Z"}],
			"untouched": {"n": 5},
			"top": "9223372036854775807"
		}
	}`, string(body))

	assert.Equal(t, far, record.Attributes["top"])
	assert.Equal(t, far, inner["at"])
	assert.Equal(t, far, list[0])
	assert.Equal(t, near, list[2].(map[string]any)["at"])
}

// TestDataRecordWithoutDatesEncodesAsBefore pins the shape MarshalJSON keeps
// for records it has nothing to render in, nil attributes included.
func TestDataRecordWithoutDatesEncodesAsBefore(t *testing.T) {
	rowID := uuid.MustParse("01890000-0000-7000-8000-000000000001")
	body, err := json.Marshal(DataRecord{SchemaName: "s", RowID: rowID})
	require.NoError(t, err)
	assert.Equal(t, `{"schema_name":"s","row_id":"01890000-0000-7000-8000-000000000001","attributes":null}`, string(body))

	body, err = json.Marshal(DataRecord{SchemaName: "s", RowID: rowID, Attributes: map[string]any{"b": "<x>", "a": 1}})
	require.NoError(t, err)
	assert.Equal(t, "{\"schema_name\":\"s\",\"row_id\":\"01890000-0000-7000-8000-000000000001\",\"attributes\":{\"a\":1,\"b\":\"\\u003cx\\u003e\"}}", string(body))
}

// TestResultsRenderTheirRecordsDates: the response types carry records by
// pointer, which reaches the value-receiver MarshalJSON as well.
func TestResultsRenderTheirRecordsDates(t *testing.T) {
	record := &DataRecord{SchemaName: "s", Attributes: map[string]any{"at": time.UnixMilli(math.MinInt64).UTC()}}
	for name, result := range map[string]any{
		"query": &QueryResult{Data: []*DataRecord{record}},
		"batch": &BatchResult{Successful: []*DataRecord{record}},
	} {
		t.Run(name, func(t *testing.T) {
			body, err := json.Marshal(result)
			require.NoError(t, err)
			assert.Contains(t, string(body), `"attributes":{"at":"-9223372036854775808"}`)
		})
	}
}
