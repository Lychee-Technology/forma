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

// The admitted set is exactly the int64 millis range: both ends convert to
// their exact millis in both funnels, and one millisecond past either end is
// refused, whatever the time.Time's zone.
func TestDateTime_EpochMillisRangeIsExact(t *testing.T) {
	c := NewAttributeConverter(nil)
	meta := forma.AttributeMetadata{AttributeID: 9, ValueType: forma.ValueTypeDateTime}
	for _, ms := range []int64{math.MinInt64, math.MaxInt64, 0, -1, 1704067200123} {
		for _, loc := range []*time.Location{time.UTC, time.FixedZone("plus14", 14*3600)} {
			t.Run(fmt.Sprintf("%d in %s", ms, loc), func(t *testing.T) {
				value := time.UnixMilli(ms).In(loc)
				var rec model.EAVRecord
				set, err := populateTypedValue(&rec, "seenAt", value, meta)
				require.NoError(t, err)
				require.True(t, set)
				require.Equal(t, ms, *rec.ValueInt64)
				require.Equal(t, float64(ms), *rec.ValueNumeric)

				eav, err := c.ToEAVRecord(model.EntityAttribute{
					SchemaID: 1, AttrID: 9, ValueType: forma.ValueTypeDateTime, Value: value,
				}, uuid.New())
				require.NoError(t, err)
				require.Equal(t, ms, *eav.ValueInt64)
				require.Equal(t, float64(ms), *eav.ValueNumeric)
			})
		}
	}
	for name, value := range map[string]time.Time{
		"one ms past the ceiling": time.UnixMilli(math.MaxInt64).Add(time.Millisecond),
		"one ms below the floor":  time.UnixMilli(math.MinInt64).Add(-time.Millisecond),
	} {
		t.Run(name, func(t *testing.T) {
			var rec model.EAVRecord
			set, err := populateTypedValue(&rec, "seenAt", value, meta)
			require.False(t, set)
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			_, err = c.ToEAVRecord(model.EntityAttribute{
				SchemaID: 1, AttrID: 9, ValueType: forma.ValueTypeDateTime, Value: value,
			}, uuid.New())
			require.Error(t, err)
		})
	}
}
