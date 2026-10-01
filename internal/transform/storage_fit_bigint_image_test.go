package transform

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// bigintImageDestinations are the destinations that keep only the float64
// image of a declared bigint: storeInEAV clears the exact sidecar, and
// storeNumericRendering writes a double column from the image.
var bigintImageDestinations = []struct {
	name    string
	meta    forma.AttributeMetadata
	indices string
}{
	{"eav", forma.AttributeMetadata{AttributeID: 9, ValueType: forma.ValueTypeBigInt}, ""},
	{"eav list item", forma.AttributeMetadata{AttributeID: 9, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeBigInt}, "0"},
	{"double column", boundMeta(forma.ValueTypeBigInt, forma.MainColumnDouble01, forma.MainColumnEncodingDefault), ""},
}

// censusAdmitsBigint is the #501 integer-width census predicate for a
// declared bigint (widthaudit.BuildCensusQuery): the stored value is integral
// and inside [-2^53, 2^53], compared exactly as NUMERIC compares it (#590).
func censusAdmitsBigint(image float64) bool {
	exact, ok := new(big.Float).SetFloat64(image).Int(nil)
	if ok != big.Exact {
		return false
	}
	return exact.Cmp(big.NewInt(-maxBigintImage)) >= 0 && exact.Cmp(big.NewInt(maxBigintImage)) <= 0
}

// storedBigintImage runs an admitted record through the store for its
// destination and returns the float64 that is persisted.
func storedBigintImage(t *testing.T, rec model.EAVRecord, meta forma.AttributeMetadata) float64 {
	t.Helper()
	record := &model.PersistentRecord{Float64Items: map[string]float64{}}
	if meta.ColumnBinding == nil {
		storeInEAV(record, rec)
		require.Len(t, record.OtherAttributes, 1)
		require.Nil(t, record.OtherAttributes[0].ValueInt64, "eav_data must not carry the sidecar")
		require.NotNil(t, record.OtherAttributes[0].ValueNumeric)
		return *record.OtherAttributes[0].ValueNumeric
	}
	tr := &persistentRecordTransformer{}
	require.NoError(t, tr.storeInMainColumn(record, rec, meta.ColumnBinding))
	stored, ok := record.Float64Items[string(meta.ColumnBinding.ColumnName)]
	require.True(t, ok)
	return stored
}

// #590: eav_data and double_* persist the float64 image of a declared bigint,
// which is exact only within |v| <= 2^53. The funnel used to admit the whole
// int64 range on the strength of the exact sidecar (#205 stored the rounded
// image; #612 refused only the slice whose image is past int64). Admission,
// the stored image and the #501 census must agree in both directions: a
// value is admitted iff it is stored as itself iff the census is clean.
func TestCheckStorageFit_BigintImageDestinationParity(t *testing.T) {
	values := []any{
		int64(math.MaxInt64), "9223372036854775807", json.Number("9223372036854775807"),
		int64(9223372036854775295), json.Number("9223372036854775296"),
		int64(math.MinInt64), "-9223372036854775808", int64(-math.MaxInt64),
		int64(1) << 62, float64(1e18),
		int64(maxBigintImage) + 1, json.Number("9007199254740993"), "9007199254740993",
		int64(maxBigintImage), json.Number("9007199254740992"), float64(maxBigintImage),
		int64(maxBigintImage) - 1, int64(-maxBigintImage), int64(-maxBigintImage) - 1,
		float64(-maxBigintImage), math.Ldexp(-1, 63), math.Ldexp(1, 63),
		0, -1, int64(42), "42",
		// Spellings whose float64 image is whole and inside the range while
		// the literal is not the image (#590 review).
		json.Number("9007199254740991.5"), json.Number("9.007199254740993e15"),
		json.Number("9.007199254740992e15"), "9007199254740993.000",
	}
	for _, dest := range bigintImageDestinations {
		for _, value := range values {
			t.Run(fmt.Sprintf("%s/%v(%T)", dest.name, value, value), func(t *testing.T) {
				rec := model.EAVRecord{ArrayIndices: dest.indices}
				_, err := populateTypedValue(&rec, "n", value, dest.meta)
				require.NotNil(t, rec.ValueNumeric)
				if err != nil {
					require.ErrorIs(t, err, forma.ErrInvalidInput)
					// A refusal is right when the image is not the caller's
					// value (2^53+1 has the image 2^53, which the census
					// rightly admits) or when the census would flag it.
					image := *rec.ValueNumeric
					kept := rec.ValueInt64 != nil && censusAdmitsBigint(image) && int64(image) == *rec.ValueInt64
					require.False(t, kept, "refused a value the store keeps exactly as %v and the census admits", image)
					return
				}
				image := storedBigintImage(t, rec, dest.meta)
				require.True(t, censusAdmitsBigint(image), "admitted a value stored as %v, past 2^53", image)
				exact, err := int64FromBigintImage(image)
				require.NoError(t, err)
				require.Equal(t, *rec.ValueInt64, exact, "the stored image must read back as the caller's value")
			})
		}
	}
}

// The boundary is exact: 2^53 is the largest magnitude admitted, and the
// message names the caller's value, the destination, the image it became and
// the allowed range.
func TestCheckStorageFit_BigintImageBoundaryMessage(t *testing.T) {
	for _, dest := range bigintImageDestinations {
		t.Run(dest.name, func(t *testing.T) {
			for _, ok := range []any{int64(maxBigintImage), int64(-maxBigintImage), float64(maxBigintImage)} {
				rec := model.EAVRecord{ArrayIndices: dest.indices}
				_, err := populateTypedValue(&rec, "n", ok, dest.meta)
				require.NoError(t, err, "%v", ok)
			}

			refused := model.EAVRecord{ArrayIndices: dest.indices}
			_, err := populateTypedValue(&refused, "n", json.Number("9007199254740993"), dest.meta)
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			msg, published := forma.ResolvePublicMessage(err)
			require.True(t, published, "the refusal describes the caller's value, so it is published")
			require.Contains(t, msg, "attribute 'n'")
			require.Contains(t, msg, "bigint value 9007199254740993 does not fit")
			require.Contains(t, msg, "float64 image 9007199254740992")
			require.Contains(t, msg, "allowed [-9007199254740992, 9007199254740992]")

			// MaxInt64: the message names the caller's value, not the 2^63
			// its image became (#612 wording kept).
			refused = model.EAVRecord{ArrayIndices: dest.indices}
			_, err = populateTypedValue(&refused, "n", json.Number("9223372036854775807"), dest.meta)
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			msg, _ = forma.ResolvePublicMessage(err)
			require.Contains(t, msg, "bigint value 9223372036854775807 does not fit")
			require.Contains(t, msg, "float64 image 9223372036854775808")

			// A whole float64 input is judged as the integer it names, like
			// any other spelling: the message names that value.
			refused = model.EAVRecord{ArrayIndices: dest.indices}
			_, err = populateTypedValue(&refused, "n", float64(1e18), dest.meta)
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			msg, _ = forma.ResolvePublicMessage(err)
			require.Contains(t, msg, "bigint value 1000000000000000000 does not fit")
		})
	}
}

// The funnel judges a declared integer on the literal, not on its float64
// image (#590 review). Past 2^52 the image is always whole, so a fractional
// literal there (9007199254740991.5 has the image 2^53) used to pass the
// slot check and be stored as the image; and an exponent spelling of an
// integer past 2^53 (9.007199254740993e15) lost its exact sidecar and was
// admitted as 2^53 on the image destinations, and stored rounded on a
// bigint column. Every destination and every declared integer type judges
// the same way.
func TestPopulateTypedValue_IntegerJudgedOnTheLiteral(t *testing.T) {
	bigintColumn := boundMeta(forma.ValueTypeBigInt, forma.MainColumnBigint01, forma.MainColumnEncodingDefault)
	destinations := append(bigintImageDestinations, struct {
		name    string
		meta    forma.AttributeMetadata
		indices string
	}{"bigint column", bigintColumn, ""})

	for _, dest := range destinations {
		t.Run(dest.name, func(t *testing.T) {
			for _, lit := range []any{json.Number("9007199254740991.5"), "9007199254740991.5", json.Number("-9007199254740991.5"), json.Number("9.0071992547409915e15")} {
				rec := model.EAVRecord{ArrayIndices: dest.indices}
				_, err := populateTypedValue(&rec, "n", lit, dest.meta)
				require.ErrorIs(t, err, forma.ErrInvalidInput, "%v", lit)
				msg, published := forma.ResolvePublicMessage(err)
				require.True(t, published)
				require.Contains(t, msg, fmt.Sprintf("non-integral value %v does not fit declared type bigint (whole number required)", lit))
				require.Nil(t, rec.ValueInt64)
			}

			exact := model.EAVRecord{ArrayIndices: dest.indices}
			_, err := populateTypedValue(&exact, "n", json.Number("9.007199254740992e15"), dest.meta)
			require.NoError(t, err)
			require.NotNil(t, exact.ValueInt64)
			require.Equal(t, int64(maxBigintImage), *exact.ValueInt64)

			past := model.EAVRecord{ArrayIndices: dest.indices}
			_, err = populateTypedValue(&past, "n", json.Number("9.007199254740993e15"), dest.meta)
			if dest.meta.ColumnBinding != nil && dest.meta.ColumnBinding.ColumnName == forma.MainColumnBigint01 {
				require.NoError(t, err, "a bigint column stores the exact sidecar")
				require.NotNil(t, past.ValueInt64)
				require.Equal(t, int64(maxBigintImage)+1, *past.ValueInt64)
				return
			}
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			msg, _ := forma.ResolvePublicMessage(err)
			require.Contains(t, msg, "bigint value 9007199254740993 does not fit")
			require.Contains(t, msg, "float64 image 9007199254740992")
		})
	}

	// The narrower declared types judge the literal the same way; their
	// float64 image would have failed on range instead, so the refusal
	// names the fraction the caller sent rather than a rounded image.
	for _, vt := range []forma.ValueType{forma.ValueTypeSmallInt, forma.ValueTypeInteger} {
		var rec model.EAVRecord
		_, err := populateTypedValue(&rec, "n", json.Number("9007199254740991.5"), forma.AttributeMetadata{AttributeID: 9, ValueType: vt})
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		msg, _ := forma.ResolvePublicMessage(err)
		require.Contains(t, msg, "non-integral value 9007199254740991.5 does not fit declared type "+string(vt))
	}
	// numeric keeps float64 semantics: the literal's value is its image.
	var num model.EAVRecord
	_, err := populateTypedValue(&num, "n", json.Number("9007199254740991.5"), forma.AttributeMetadata{AttributeID: 9, ValueType: forma.ValueTypeNumeric})
	require.NoError(t, err)
	require.Equal(t, float64(maxBigintImage), *num.ValueNumeric)
}

// A bigint column persists the exact sidecar, so the image rule does not
// narrow it: the whole int64 range stays admitted there (#205).
func TestCheckStorageFit_BigintColumnKeepsFullRange(t *testing.T) {
	for _, v := range []any{json.Number("9223372036854775807"), json.Number("-9223372036854775808"), int64(maxBigintImage) + 1} {
		var rec model.EAVRecord
		_, err := populateTypedValue(&rec, "n", v,
			boundMeta(forma.ValueTypeBigInt, forma.MainColumnBigint01, forma.MainColumnEncodingDefault))
		require.NoError(t, err, "%v", v)
	}
}

// The read side of the image (#590): a whole image inside int64 converts to
// itself, past 2^53 included (a legacy row the census names); a fraction or a
// magnitude int64() would wrap is a consistency error that names the image.
func TestInt64FromBigintImage(t *testing.T) {
	for _, v := range []float64{0, -1, 42, maxBigintImage, -maxBigintImage, maxBigintImage + 2, 1e18, math.Ldexp(-1, 63), 9223372036854774784} {
		got, err := int64FromBigintImage(v)
		require.NoError(t, err, "%v", v)
		require.Equal(t, int64(v), got)
		require.Equal(t, v, float64(got), "%v", v)
	}
	for _, tc := range []struct {
		image float64
		want  string
	}{
		{1000.5, "stored bigint image 1000.5 is not a whole number"},
		{math.NaN(), "is not a whole number"},
		{math.Inf(1), "is not a whole number"},
		{math.Ldexp(1, 63), "stored bigint image 9223372036854775808 is outside the bigint range"},
		{math.Ldexp(1, 64), "stored bigint image 18446744073709551616 is outside the bigint range"},
		{math.Ldexp(-1, 64), "outside the bigint range"},
	} {
		_, err := int64FromBigintImage(tc.image)
		require.Error(t, err, "%v", tc.image)
		require.Contains(t, err.Error(), tc.want)
		require.NotErrorIs(t, err, forma.ErrInvalidInput, "a read-path consistency error is not a 4xx")
	}
}
