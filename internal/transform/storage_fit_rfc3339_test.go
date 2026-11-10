package transform

// The rfc3339 encoding's fit, store and read (#582, #842), split out of
// storage_fit_test.go at the 500-line limit.

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/rfc3339"
)

// #842: the rfc3339 rendering is an RFC 3339 string with exactly three
// fractional digits, so it holds every millisecond the type carries. Before
// #842 the encoding (then iso8601) kept whole seconds and refused an instant
// off one (#582); the same instant is now accepted on every destination
// alike. The four-digit year is the one rule left.
//
// The rule judges the normalised epoch millis, not the spelling the caller
// sent: epoch millis are the type's precision (#589), so a finer fraction is
// floored to millis before this check runs.
func TestCheckStorageFit_RFC3339Millis(t *testing.T) {
	iso := boundMeta(forma.ValueTypeDateTime, forma.MainColumnText02, forma.MainColumnEncodingRFC3339)
	isoDate := boundMeta(forma.ValueTypeDate, forma.MainColumnText03, forma.MainColumnEncodingRFC3339)
	cases := []struct {
		name    string
		meta    forma.AttributeMetadata
		value   any
		wantErr string // substring of the published message; "" means accepted
	}{
		{"datetime with millis accepted", iso, "2024-01-01T00:00:00.123Z", ""},
		{"datetime whole second accepted", iso, "2024-01-01T00:00:00Z", ""},
		{"datetime zero fraction accepted", iso, "2024-01-01T00:00:00.000Z", ""},
		{"datetime epoch-ms string with millis accepted", iso, "1704067200123", ""},
		{"datetime time.Time with millis accepted", iso, time.UnixMilli(1704067200999).UTC(), ""},
		{"datetime pre-epoch sub-second accepted", iso, "-500", ""},
		{"date with millis accepted", isoDate, "2024-01-01T00:00:00.001Z", ""},
		{"date day accepted", isoDate, "2024-01-01", ""},
		// The RFC 3339 layout has a four-digit year: the reader
		// (time.Parse(time.RFC3339)) refuses the "10000-…" and "-0001-…"
		// images time.Format writes past either end, so the check refuses
		// the value instead of letting the store write a row the read path
		// cannot parse (#587 review).
		{"datetime year 0000 floor accepted", iso, "0000-01-01T00:00:00Z", ""},
		{"datetime year 9999 ceiling accepted", iso, "9999-12-31T23:59:59.999Z", ""},
		{"datetime epoch-ms string at the ceiling accepted", iso, "253402300799999", ""},
		{"datetime year 10000 rejected", iso, "253402300800000",
			"datetime value 253402300800000 (10000-01-01T00:00:00Z) cannot be stored in main column text_02 with encoding rfc3339, which keeps years 0000 to 9999 (the RFC 3339 four-digit year)"},
		{"datetime year -0001 rejected", iso, "-62167219200001",
			"datetime value -62167219200001 (-0001-12-31T23:59:59.999Z) cannot be stored in main column text_02 with encoding rfc3339, which keeps years 0000 to 9999"},
		{"date year 10000 rejected", isoDate, "253402300800000", "keeps years 0000 to 9999"},
		// Epoch millis are the type's precision (#589 ruling): the funnel
		// floors a finer fraction at normalisation, for every destination
		// alike, and the store renders the floored millis.
		{"datetime sub-millisecond fraction is floored to millis before the check", iso, "2024-01-01T00:00:00.000001Z", ""},
		{"datetime sub-millisecond fraction floored to .999 accepted", iso, "2024-01-01T00:00:00.9999999Z", ""},
		{"unbound datetime keeps millis",
			forma.AttributeMetadata{AttributeID: 9, ValueType: forma.ValueTypeDateTime}, "2024-01-01T00:00:00.123Z", ""},
		{"unix_ms bigint keeps millis",
			boundMeta(forma.ValueTypeDateTime, forma.MainColumnBigint01, forma.MainColumnEncodingUnixMs), "2024-01-01T00:00:00.123Z", ""},
		{"default bigint keeps millis",
			boundMeta(forma.ValueTypeDateTime, forma.MainColumnBigint01, forma.MainColumnEncodingDefault), "2024-01-01T00:00:00.123Z", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var rec model.EAVRecord
			set, err := populateTypedValue(&rec, "seenAt", tc.value, tc.meta)
			if tc.wantErr == "" {
				require.NoError(t, err)
				require.True(t, set)
				return
			}
			require.ErrorIs(t, err, forma.ErrInvalidInput, "fit rejection must be user-facing invalid input")
			msg, ok := forma.ResolvePublicMessage(err)
			require.True(t, ok, "fit rejection must publish its message")
			require.Contains(t, msg, tc.wantErr)
			require.Contains(t, msg, "attribute 'seenAt'")
		})
	}
}

// #582, #842: whatever the funnel admits into an rfc3339 column is stored
// as its canonical millisecond image and reads back with exactly the millis
// it was written with.
func TestRFC3339_StoreReadRoundTripIsExact(t *testing.T) {
	tr := &persistentRecordTransformer{}
	meta := boundMeta(forma.ValueTypeDateTime, forma.MainColumnText02, forma.MainColumnEncodingRFC3339)
	for value, image := range map[string]string{
		"2024-01-01T00:00:00Z":          "2024-01-01T00:00:00.000Z",
		"2024-01-01T00:00:00.123Z":      "2024-01-01T00:00:00.123Z",
		"2024-01-02T03:04:05.25Z":       "2024-01-02T03:04:05.250Z",
		"2024-01-02T11:04:05.250+08:00": "2024-01-02T03:04:05.250Z",
		"1970-01-01T00:00:00Z":          "1970-01-01T00:00:00.000Z",
		"-1000":                         "1969-12-31T23:59:59.000Z",
		"-1":                            "1969-12-31T23:59:59.999Z",
		"2099-12-31T23:59:59+02:00":     "2099-12-31T21:59:59.000Z",
		"2024-01-01":                    "2024-01-01T00:00:00.000Z",
		// Both ends of the RFC 3339 four-digit year: the reader must parse
		// the image of every admitted value (#587 review).
		"0000-01-01T00:00:00Z":     "0000-01-01T00:00:00.000Z",
		"9999-12-31T23:59:59.999Z": "9999-12-31T23:59:59.999Z",
		"-62167219200000":          "0000-01-01T00:00:00.000Z",
		"253402300799999":          "9999-12-31T23:59:59.999Z",
	} {
		t.Run(value, func(t *testing.T) {
			var rec model.EAVRecord
			_, err := populateTypedValue(&rec, "seenAt", value, meta)
			require.NoError(t, err)
			record := &model.PersistentRecord{TextItems: map[string]string{}}
			require.NoError(t, tr.storeInMainColumn(record, rec, meta.ValueType, meta.ColumnBinding))
			require.Equal(t, image, record.TextItems["text_02"])
			got, err := tr.readFromMainColumn(record, meta, meta.ColumnBinding)
			require.NoError(t, err)
			require.NotNil(t, got)
			require.Equal(t, *rec.ValueNumeric, *got.ValueNumeric)
		})
	}
}

// #842: the read keeps accepting the whole-second image the iso8601
// encoding wrote, so a row the migration has not reached yet still reads
// as the instant it names.
func TestRFC3339_ReadsLegacyWholeSecondImage(t *testing.T) {
	tr := &persistentRecordTransformer{}
	meta := boundMeta(forma.ValueTypeDateTime, forma.MainColumnText02, forma.MainColumnEncodingRFC3339)
	record := &model.PersistentRecord{TextItems: map[string]string{"text_02": "2024-01-01T00:00:01Z"}}
	got, err := tr.readFromMainColumn(record, meta, meta.ColumnBinding)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, float64(1704067201000), *got.ValueNumeric)
}

// #587 review / #582 redesign: the check and the store judge the exact
// millis (exactEpochMillis), never a rounded or wrapped int64() of the float
// slot. A hand-built record with only a float slot is a funnel bypass: a
// whole number inside the int64 range is still exact and is judged by the
// rfc3339 rule (year 10000 and year -0001 refused with the year rule); a
// fraction, NaN, an infinity or a magnitude past int64 names no instant and
// is refused as such by both. Nothing is written in either case, so 1000.5
// is never truncated to 1970-01-01T00:00:01Z and 10000-01-01T00:00:00Z is
// never stored.
func TestRFC3339_BypassSlotIsRefused(t *testing.T) {
	tr := &persistentRecordTransformer{}
	binding := &forma.MainColumnBinding{ColumnName: forma.MainColumnText02, Encoding: forma.MainColumnEncodingRFC3339}
	noInstant := func(describe string) (string, string) {
		return "datetime value %s " + describe + " names no epoch millisecond instant and cannot be stored in main column text_02 with encoding rfc3339",
			"encoding rfc3339 cannot hold a slot in main column text_02: value %s " + describe + " names no epoch millisecond instant"
	}
	yearRule := func(instant string) (string, string) {
		return "datetime value %s (" + instant + ") cannot be stored in main column text_02 with encoding rfc3339, which " + string(rfc3339.KeepsFourDigitYear),
			"encoding rfc3339 " + string(rfc3339.KeepsFourDigitYear) + " and cannot hold value %s in main column text_02"
	}
	notWhole, beyond := "(not a whole number of epoch milliseconds)", "(beyond any epoch millisecond instant)"
	cases := []struct {
		numVal float64
		check  string // the instant a whole in-range slot names; "" for a slot that names none
	}{
		{1000.5, ""}, {1704067200000.25, ""}, {-0.5, ""}, {math.NaN(), ""}, {math.Inf(1), ""}, {math.Inf(-1), ""},
		{math.MaxInt64, ""}, {1e300, ""}, {-1e300, ""},
		{253402300800000, "10000-01-01T00:00:00Z"}, {-62167219200001, "-0001-12-31T23:59:59.999Z"},
	}
	for _, tc := range cases {
		t.Run(formatFitValue(tc.numVal), func(t *testing.T) {
			var check, store string
			switch {
			case tc.check != "":
				check, store = yearRule(tc.check)
			case !isWholeMillis(tc.numVal):
				check, store = noInstant(notWhole)
			default:
				check, store = noInstant(beyond)
			}
			v := tc.numVal
			rec := model.EAVRecord{ValueNumeric: &v}

			err := checkBoundColumnFit(&rec, forma.ValueTypeDateTime, binding)
			require.Error(t, err)
			require.Equal(t, fmt.Sprintf(check, formatFitValue(tc.numVal)), err.Error())

			record := newEmptyPersistentRecord()
			stored, err := tr.storeWithEncoding(record, rec, forma.ValueTypeDateTime, binding)
			require.Error(t, err)
			require.False(t, stored)
			require.Equal(t, fmt.Sprintf(store, formatFitValue(tc.numVal)), err.Error())
			require.Empty(t, record.TextItems)
		})
	}
}
