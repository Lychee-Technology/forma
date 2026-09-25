package transform

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// #587 review: time.Time.UnixMilli is undefined outside the int64 millis
// range and wraps there, so an extreme but valid time.Time used to be
// normalised to an unrelated in-range instant before any fit decision and
// stored as such (year 73069258127 landed as 1970-04-08T20:07:28Z). The
// funnel refuses such a value at normalisation, for every date/datetime
// destination, and admits exactly the instants an int64 of millis names.
func TestDateTime_ExtremeTimeIsRefusedBeforeNormalisation(t *testing.T) {
	extreme := time.Date(73069258127, 1, 1, 0, 0, 0, 0, time.UTC)
	metas := map[string]forma.AttributeMetadata{
		"unbound":      {AttributeID: 9, ValueType: forma.ValueTypeDateTime},
		"unbound date": {AttributeID: 9, ValueType: forma.ValueTypeDate},
		"iso8601":      boundMeta(forma.ValueTypeDateTime, forma.MainColumnText02, forma.MainColumnEncodingISO8601),
		"unix_ms":      boundMeta(forma.ValueTypeDateTime, forma.MainColumnBigint01, forma.MainColumnEncodingUnixMs),
		"default":      boundMeta(forma.ValueTypeDateTime, forma.MainColumnBigint01, forma.MainColumnEncodingDefault),
	}
	for name, meta := range metas {
		t.Run(name, func(t *testing.T) {
			var rec model.EAVRecord
			set, err := populateTypedValue(&rec, "seenAt", extreme, meta)
			require.False(t, set)
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			msg, ok := forma.ResolvePublicMessage(err)
			require.True(t, ok)
			require.Contains(t, msg, "attribute 'seenAt'")
			require.Contains(t, msg, "73069258127-01-01T00:00:00Z")
			require.Contains(t, msg, "epoch milliseconds")
			require.Nil(t, rec.ValueNumeric)
			require.Nil(t, rec.ValueInt64)
		})
	}

	// The alternate funnel refuses the same value.
	c := NewAttributeConverter(nil)
	for _, value := range []any{extreme, &extreme} {
		_, err := c.ToEAVRecord(model.EntityAttribute{
			SchemaID: 1, AttrID: 9, ValueType: forma.ValueTypeDateTime, Value: value,
		}, uuid.New())
		require.Error(t, err)
		require.Contains(t, err.Error(), "73069258127-01-01T00:00:00Z")
	}

	// The reviewer's probe end to end: no text image is written.
	tr := &persistentRecordTransformer{}
	var rec model.EAVRecord
	_, err := populateTypedValue(&rec, "seenAt", extreme, metas["iso8601"])
	require.Error(t, err)
	record := &model.PersistentRecord{TextItems: map[string]string{}}
	require.Error(t, tr.storeInMainColumn(record, rec, metas["iso8601"].ColumnBinding))
	require.Empty(t, record.TextItems)
}

// #582: normalisation admits exactly the int64 millis range. Both funnels
// turn either end into its exact millis in the sidecar with the float image
// derived from it, whatever the time.Time's zone, and refuse one
// millisecond past either end. Whether a destination then holds the value
// is that destination's rule: a bigint column keeps the whole range (pinned
// here through the store and read); the float64 image of eav_data is #592.
func TestDateTime_EpochMillisRangeIsExact(t *testing.T) {
	c := NewAttributeConverter(nil)
	tr := &persistentRecordTransformer{}
	meta := forma.AttributeMetadata{AttributeID: 9, ValueType: forma.ValueTypeDateTime}
	bigint := []forma.AttributeMetadata{
		boundMeta(forma.ValueTypeDateTime, forma.MainColumnBigint01, forma.MainColumnEncodingUnixMs),
		boundMeta(forma.ValueTypeDateTime, forma.MainColumnBigint01, forma.MainColumnEncodingDefault),
	}
	for _, ms := range []int64{math.MinInt64, math.MaxInt64, 0, -1, 1704067200123} {
		for _, loc := range []*time.Location{time.UTC, time.FixedZone("plus14", 14*3600)} {
			t.Run(fmt.Sprintf("%d in %s", ms, loc), func(t *testing.T) {
				value := time.UnixMilli(ms).In(loc)
				for _, bm := range bigint {
					var rec model.EAVRecord
					set, err := populateTypedValue(&rec, "seenAt", value, bm)
					require.NoError(t, err)
					require.True(t, set)
					require.Equal(t, ms, *rec.ValueInt64)
					require.Equal(t, float64(ms), *rec.ValueNumeric)

					record := newEmptyPersistentRecord()
					require.NoError(t, tr.storeInMainColumn(record, rec, bm.ColumnBinding))
					require.Equal(t, ms, record.Int64Items["bigint_01"])
					got, err := tr.readFromMainColumn(record, bm, bm.ColumnBinding)
					require.NoError(t, err)
					require.Equal(t, ms, *got.ValueInt64)
					read, err := extractValueFromEAVRecord(*got, forma.ValueTypeDateTime)
					require.NoError(t, err)
					require.Equal(t, time.UnixMilli(ms).UTC(), read)
				}

				eav, err := c.ToEAVRecord(model.EntityAttribute{
					SchemaID: 1, AttrID: 9, ValueType: forma.ValueTypeDateTime, Value: value,
				}, uuid.New())
				require.NoError(t, err)
				require.Equal(t, ms, *eav.ValueInt64)
				require.Equal(t, float64(ms), *eav.ValueNumeric)
			})
		}
	}
	// The bound is on the floored millis, not the instant: the last
	// nanosecond before the next millisecond still names MaxInt64, so it is
	// admitted like the exact millisecond and the epoch-ms string are
	// (#587 review); the first nanosecond past it names MaxInt64+1.
	for name, tc := range map[string]struct {
		value time.Time
		want  int64
	}{
		"last ns inside the ceiling": {time.UnixMilli(math.MaxInt64).Add(time.Millisecond - time.Nanosecond), math.MaxInt64},
		"first ns inside the floor":  {time.UnixMilli(math.MinInt64), math.MinInt64},
		"sub-ms above the floor":     {time.UnixMilli(math.MinInt64).Add(time.Millisecond - time.Nanosecond), math.MinInt64},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.value.UnixMilli(), "test premise: UnixMilli floors into the range")
			for _, bm := range bigint {
				var rec model.EAVRecord
				set, err := populateTypedValue(&rec, "seenAt", tc.value, bm)
				require.NoError(t, err)
				require.True(t, set)
				require.Equal(t, tc.want, *rec.ValueInt64)
			}
			eav, err := c.ToEAVRecord(model.EntityAttribute{
				SchemaID: 1, AttrID: 9, ValueType: forma.ValueTypeDateTime, Value: tc.value,
			}, uuid.New())
			require.NoError(t, err)
			require.Equal(t, tc.want, *eav.ValueInt64)
		})
	}
	for name, value := range map[string]time.Time{
		"one ms past the ceiling": time.UnixMilli(math.MaxInt64).Add(time.Millisecond),
		"one ms below the floor":  time.UnixMilli(math.MinInt64).Add(-time.Millisecond),
		"one ns below the floor":  time.UnixMilli(math.MinInt64).Add(-time.Nanosecond),
	} {
		t.Run(name, func(t *testing.T) {
			for _, m := range append(bigint, meta) {
				var rec model.EAVRecord
				set, err := populateTypedValue(&rec, "seenAt", value, m)
				require.False(t, set)
				require.ErrorIs(t, err, forma.ErrInvalidInput)
			}
			_, err := c.ToEAVRecord(model.EntityAttribute{
				SchemaID: 1, AttrID: 9, ValueType: forma.ValueTypeDateTime, Value: value,
			}, uuid.New())
			require.Error(t, err)
		})
	}
}

// #589 ruling: epoch milliseconds are the precision of the date/datetime
// type itself — every tier and the read path carry them — so both funnels
// floor input finer than a millisecond at normalisation, for every
// destination alike, and the floored millis are what every destination rule
// then judges. That is the type's precision contract, not a narrowing one
// destination applies (#459, #582), so it is documented and pinned here
// rather than refused. The floor is toward the past for pre-epoch instants
// too: in the RFC3339 spelling it is always "drop the digits after the
// third fractional digit".
func TestDateTime_SubMillisecondInputIsFlooredToMillis(t *testing.T) {
	c := NewAttributeConverter(nil)
	whole := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) // 1704067200000
	metas := map[string]forma.AttributeMetadata{
		"unbound datetime": {AttributeID: 9, ValueType: forma.ValueTypeDateTime},
		"unbound date":     {AttributeID: 9, ValueType: forma.ValueTypeDate},
		"unix_ms":          boundMeta(forma.ValueTypeDateTime, forma.MainColumnBigint01, forma.MainColumnEncodingUnixMs),
		"default bigint":   boundMeta(forma.ValueTypeDateTime, forma.MainColumnBigint01, forma.MainColumnEncodingDefault),
	}
	cases := []struct {
		name  string
		value any
		want  int64
	}{
		{"time.Time one microsecond past the second", whole.Add(time.Microsecond), 1704067200000},
		{"time.Time last nanosecond of a millisecond", whole.Add(124*time.Millisecond - time.Nanosecond), 1704067200123},
		{"time.Time in an offset zone", whole.Add(time.Microsecond).In(time.FixedZone("plus2", 2*3600)), 1704067200000},
		{"time.Time last nanosecond before the epoch", time.UnixMilli(-1).Add(time.Millisecond - time.Nanosecond), -1},
		{"RFC3339 six fractional digits", "2024-01-01T00:00:00.000001Z", 1704067200000},
		{"RFC3339 nine fractional digits", "2024-01-01T00:00:00.123999999Z", 1704067200123},
		{"RFC3339 fraction in an offset zone", "2024-01-01T02:00:00.000999+02:00", 1704067200000},
		{"RFC3339 pre-epoch fraction floors toward the past", "1969-12-31T23:59:59.9995Z", -1},
	}
	for _, tc := range cases {
		for dest, meta := range metas {
			t.Run(tc.name+"/"+dest, func(t *testing.T) {
				var rec model.EAVRecord
				set, err := populateTypedValue(&rec, "seenAt", tc.value, meta)
				require.NoError(t, err)
				require.True(t, set)
				require.Equal(t, tc.want, *rec.ValueInt64)
				require.Equal(t, float64(tc.want), *rec.ValueNumeric)
				read, err := extractValueFromEAVRecord(rec, meta.ValueType)
				require.NoError(t, err)
				require.Equal(t, time.UnixMilli(tc.want).UTC(), read)
			})
		}
		// The alternate funnel takes a time.Time or a *time.Time, no strings,
		// and floors both the same way.
		value, ok := tc.value.(time.Time)
		if !ok {
			continue
		}
		for shape, input := range map[string]any{"time.Time": value, "*time.Time": &value} {
			t.Run(tc.name+"/ToEAVRecord/"+shape, func(t *testing.T) {
				eav, err := c.ToEAVRecord(model.EntityAttribute{
					SchemaID: 1, AttrID: 9, ValueType: forma.ValueTypeDateTime, Value: input,
				}, uuid.New())
				require.NoError(t, err)
				require.Equal(t, tc.want, *eav.ValueInt64)
				require.Equal(t, float64(tc.want), *eav.ValueNumeric)
			})
		}
	}

	// The issue's example, by its spelling: the microsecond is gone on the read.
	var rec model.EAVRecord
	_, err := populateTypedValue(&rec, "seenAt", "2024-01-01T00:00:00.000001Z", metas["unbound datetime"])
	require.NoError(t, err)
	read, err := extractValueFromEAVRecord(rec, forma.ValueTypeDateTime)
	require.NoError(t, err)
	require.Equal(t, "2024-01-01T00:00:00Z", read.(time.Time).Format(time.RFC3339Nano))
}
