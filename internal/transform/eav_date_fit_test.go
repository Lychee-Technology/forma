package transform

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// eavDateRegistry has one unbound datetime and one unbound date attribute,
// so ToPersistentRecord routes both into eav_data.
func eavDateRegistry() forma.SchemaRegistry {
	return &stubSchemaRegistry{
		schemaID: 300, schemaName: "eav_dates",
		cache: forma.SchemaAttributeCache{
			"seenAt": {AttributeID: 9, ValueType: forma.ValueTypeDateTime},
			"bornOn": {AttributeID: 10, ValueType: forma.ValueTypeDate},
		},
	}
}

// eavExactMillis are the representative values inside the float64-exact
// range the unbound destination keeps; eavRoundedMillis are the first values
// past it and the int64 ends. 2^53+2 is a float64-representable integer, but
// its neighbours collapse onto it and its Postgres decimal image differs
// from the value on other magnitudes, so the admitted set is contiguous and
// ends at 2^53 (measured on real Postgres, see the #587 design decision).
var (
	eavExactMillis   = []int64{1704067200123, 0, -1, 9007199254740991, -9007199254740991, 9007199254740992, -9007199254740992}
	eavRoundedMillis = []int64{9007199254740993, 9007199254740994, -9007199254740993, -9007199254740994, math.MaxInt64, math.MinInt64}
)

// Invariant C for the unbound destination (#582): populateTypedValue admits
// a date/datetime into eav_data iff |ms| <= 2^53, ToPersistentRecord
// persists exactly that value as the float64 image with the sidecar cleared,
// FromPersistentRecord reads it back as the same instant, and a value past
// the range is refused as published invalid input, by the time.Time and the
// epoch-ms string inputs alike, in every zone. The same values bound to a
// bigint column are admitted and stored exactly, so the range is the
// destination's, not the type's.
func TestDateTime_EAVFitIsTheFloat64ExactRange(t *testing.T) {
	ctx := context.Background()
	tr := NewPersistentRecordTransformer(eavDateRegistry())
	unbound := forma.AttributeMetadata{AttributeID: 9, ValueType: forma.ValueTypeDateTime}
	unixMs := boundMeta(forma.ValueTypeDateTime, forma.MainColumnBigint01, forma.MainColumnEncodingUnixMs)
	inputs := func(ms int64) map[string]any {
		return map[string]any{
			"time.Time UTC":   time.UnixMilli(ms).UTC(),
			"time.Time +14":   time.UnixMilli(ms).In(time.FixedZone("plus14", 14*3600)),
			"epoch-ms string": strconv.FormatInt(ms, 10),
		}
	}
	for _, ms := range eavExactMillis {
		for name, in := range inputs(ms) {
			t.Run(fmt.Sprintf("exact %d %s", ms, name), func(t *testing.T) {
				var rec model.EAVRecord
				set, err := populateTypedValue(&rec, "seenAt", in, unbound)
				require.NoError(t, err)
				require.True(t, set)

				rowID := uuid.New()
				record, err := tr.ToPersistentRecord(ctx, 300, rowID, map[string]any{"seenAt": in, "bornOn": in})
				require.NoError(t, err)
				require.Len(t, record.OtherAttributes, 2)
				for _, eav := range record.OtherAttributes {
					require.Nil(t, eav.ValueInt64, "eav_data persists the float64 image only")
					require.Equal(t, float64(ms), *eav.ValueNumeric)
					require.Equal(t, ms, int64(*eav.ValueNumeric), "the persisted image is the logical millis")
				}
				got, err := tr.FromPersistentRecord(ctx, record)
				require.NoError(t, err)
				require.Equal(t, time.UnixMilli(ms).UTC(), got["seenAt"])
				require.Equal(t, time.UnixMilli(ms).UTC(), got["bornOn"])
			})
		}
	}
	for _, ms := range eavRoundedMillis {
		for name, in := range inputs(ms) {
			t.Run(fmt.Sprintf("refused %d %s", ms, name), func(t *testing.T) {
				var rec model.EAVRecord
				set, err := populateTypedValue(&rec, "seenAt", in, unbound)
				require.False(t, set)
				require.ErrorIs(t, err, forma.ErrInvalidInput)
				msg, ok := forma.ResolvePublicMessage(err)
				require.True(t, ok)
				require.Contains(t, msg, "attribute 'seenAt'")
				require.Contains(t, msg, fmt.Sprintf("datetime value %d (", ms))
				require.Contains(t, msg, "cannot be stored in eav_data value_numeric, which keeps epoch milliseconds exactly up to 9007199254740992 (2^53)")

				_, err = tr.ToPersistentRecord(ctx, 300, uuid.New(), map[string]any{"seenAt": in})
				require.ErrorIs(t, err, forma.ErrInvalidInput)

				// The bigint destination keeps the same value exactly.
				var bound model.EAVRecord
				set, err = populateTypedValue(&bound, "seenAt", in, unixMs)
				require.NoError(t, err)
				require.True(t, set)
				require.Equal(t, ms, *bound.ValueInt64)
			})
		}
	}
}

// #559 parity for the unbound destination: checkStorageFit admits a record
// iff storeInEAV writes it, over records carrying the sidecar, the float
// image only, or both. A record ToEAVRecord built past the range (the
// alternate funnel runs no fit check of its own) is refused by the store as
// a bypass, never rounded on its way into eav_data.
func TestDateTime_EAVCheckStoreParity(t *testing.T) {
	unbound := forma.AttributeMetadata{AttributeID: 9, ValueType: forma.ValueTypeDateTime}
	c := NewAttributeConverter(nil)
	for _, ms := range append(append([]int64{}, eavExactMillis...), eavRoundedMillis...) {
		image := float64(ms)
		exact := ms
		shapes := map[string]model.EAVRecord{
			"both slots":   {AttrID: 9, ValueNumeric: &image, ValueInt64: &exact},
			"sidecar only": {AttrID: 9, ValueInt64: &exact},
		}
		// A float-only record can only carry ms when float64 keeps it; past
		// that the constructed image is a different (rounded) value, which
		// the sidecar shapes above cover. The image itself is tested below.
		if inInt64Range(image) && int64(image) == ms {
			shapes["float only"] = model.EAVRecord{AttrID: 9, ValueNumeric: &image}
		}
		eav, err := c.ToEAVRecord(model.EntityAttribute{SchemaID: 300, AttrID: 9, ValueType: forma.ValueTypeDateTime, Value: time.UnixMilli(ms).UTC()}, uuid.New())
		require.NoError(t, err)
		shapes["ToEAVRecord"] = eav
		for name, rec := range shapes {
			t.Run(fmt.Sprintf("%d %s", ms, name), func(t *testing.T) {
				checkErr := checkStorageFit(&rec, unbound)
				record := newEmptyPersistentRecord()
				storeErr := storeInEAV(record, rec, forma.ValueTypeDateTime)
				if checkErr != nil {
					require.Error(t, storeErr, "check refused (%v) but the store wrote", checkErr)
					require.Empty(t, record.OtherAttributes)
					return
				}
				require.NoError(t, storeErr, "check admitted but the store refused")
				require.Len(t, record.OtherAttributes, 1)
				require.Nil(t, record.OtherAttributes[0].ValueInt64)
				require.Equal(t, float64(ms), *record.OtherAttributes[0].ValueNumeric)
				require.Equal(t, ms, int64(*record.OtherAttributes[0].ValueNumeric))
			})
		}
	}

	// A float-only slot that names no instant, or one past the exact range,
	// is refused on both sides; the float64(2^53+1) image is 2^53 itself
	// and so is admitted as that value.
	for _, v := range []float64{1000.5, math.NaN(), math.Inf(1), 1e300, math.MaxInt64, 9007199254740994, -9007199254740994} {
		t.Run(formatFitValue(v), func(t *testing.T) {
			image := v
			rec := model.EAVRecord{AttrID: 9, ValueNumeric: &image}
			require.Error(t, checkStorageFit(&rec, unbound))
			record := newEmptyPersistentRecord()
			require.Error(t, storeInEAV(record, rec, forma.ValueTypeDateTime))
			require.Empty(t, record.OtherAttributes)
		})
	}

	// Every other type is untouched by the date rule: a bigint keeps its
	// sidecar cleared and its image as before (#205 owns its ceiling).
	big := float64(1 << 60)
	bigExact := int64(1 << 60)
	record := newEmptyPersistentRecord()
	require.NoError(t, storeInEAV(record, model.EAVRecord{AttrID: 9, ValueNumeric: &big, ValueInt64: &bigExact}, forma.ValueTypeBigInt))
	require.Nil(t, record.OtherAttributes[0].ValueInt64)
	require.Equal(t, big, *record.OtherAttributes[0].ValueNumeric)
}

// A date/datetime bound to a double column takes the same float64-image
// rule: the registration matrix refuses the pair, but the funnel must not
// depend on registration (#459), and storeNumericRendering writes the
// float64 image there.
func TestDateTime_DoubleColumnFitIsTheFloat64ExactRange(t *testing.T) {
	tr := &persistentRecordTransformer{}
	meta := boundMeta(forma.ValueTypeDateTime, forma.MainColumnDouble01, forma.MainColumnEncodingDefault)
	for _, ms := range eavExactMillis {
		var rec model.EAVRecord
		set, err := populateTypedValue(&rec, "seenAt", time.UnixMilli(ms).UTC(), meta)
		require.NoError(t, err, "%d", ms)
		require.True(t, set)
		record := newEmptyPersistentRecord()
		require.NoError(t, tr.storeInMainColumn(record, rec, meta.ValueType, meta.ColumnBinding))
		require.Equal(t, float64(ms), record.Float64Items["double_01"])
	}
	for _, ms := range eavRoundedMillis {
		var rec model.EAVRecord
		_, err := populateTypedValue(&rec, "seenAt", time.UnixMilli(ms).UTC(), meta)
		require.ErrorIs(t, err, forma.ErrInvalidInput, "%d", ms)
		msg, _ := forma.ResolvePublicMessage(err)
		require.Contains(t, msg, "cannot be stored in main column double_01 (double), which keeps epoch milliseconds exactly up to 9007199254740992 (2^53)")
	}
}

// The read side admits exactly the images the write side admits (#587
// review): a float64 image that is not a whole number inside the int64
// range names no instant, and one past 2^53 (a legacy row written before
// #582, or a hand-edited one) is a plain consistency error naming the rule,
// not the wrapped MinInt64 int64() used to return for 2^63 or a
// tier-dependent instant. Every image that reads is then rewritable: an
// update reconstructs the whole document through ToPersistentRecord, so an
// image the read accepted and checkEAVFit refused would fail an update that
// never mentioned the attribute.
func TestDateTime_ReadAdmitsExactlyTheImagesTheWriteAdmits(t *testing.T) {
	unbound := forma.AttributeMetadata{AttributeID: 9, ValueType: forma.ValueTypeDateTime}
	refused := map[float64]string{
		math.MaxInt64: "names no epoch millisecond instant", -1e300: "names no epoch millisecond instant", 1e300: "names no epoch millisecond instant",
		1000.5: "names no epoch millisecond instant", math.NaN(): "names no epoch millisecond instant", math.Inf(1): "names no epoch millisecond instant",
		9007199254740994:  "outside the epoch milliseconds a float64 image keeps exactly (up to 9007199254740992, 2^53)",
		-9007199254740994: "outside the epoch milliseconds a float64 image keeps exactly (up to 9007199254740992, 2^53)",
		1 << 60:           "outside the epoch milliseconds a float64 image keeps exactly (up to 9007199254740992, 2^53)",
		math.MinInt64:     "outside the epoch milliseconds a float64 image keeps exactly (up to 9007199254740992, 2^53)",
	}
	for v, rule := range refused {
		t.Run("refused "+formatFitValue(v), func(t *testing.T) {
			image := v
			got, err := extractValueFromEAVRecord(model.EAVRecord{AttrID: 9, ValueNumeric: &image}, forma.ValueTypeDateTime)
			require.Error(t, err)
			require.Nil(t, got)
			require.Contains(t, err.Error(), rule)
			require.NotErrorIs(t, err, forma.ErrInvalidInput, "a stored image is the operator's, not the caller's")
			// The write side refuses the same image, so the two sets agree.
			require.Error(t, checkStorageFit(&model.EAVRecord{AttrID: 9, ValueNumeric: &image}, unbound))
		})
	}
	for _, ms := range eavExactMillis {
		t.Run("rewritable "+strconv.FormatInt(ms, 10), func(t *testing.T) {
			image := float64(ms)
			got, err := extractValueFromEAVRecord(model.EAVRecord{AttrID: 9, ValueNumeric: &image}, forma.ValueTypeDate)
			require.NoError(t, err)
			require.Equal(t, time.UnixMilli(ms).UTC(), got)
			// Feeding the instant back through the write funnel, as an update
			// does, is admitted and lands as the identical image.
			var rec model.EAVRecord
			set, err := populateTypedValue(&rec, "seenAt", got, unbound)
			require.NoError(t, err)
			require.True(t, set)
			record := newEmptyPersistentRecord()
			require.NoError(t, storeInEAV(record, rec, forma.ValueTypeDateTime))
			require.Equal(t, image, *record.OtherAttributes[0].ValueNumeric)
		})
	}
}
