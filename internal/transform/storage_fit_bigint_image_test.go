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
// and inside [MinInt64, MaxInt64], compared exactly as NUMERIC compares it.
func censusAdmitsBigint(image float64) bool {
	exact, ok := new(big.Float).SetFloat64(image).Int(nil)
	if ok != big.Exact {
		return false
	}
	return exact.Cmp(big.NewInt(math.MinInt64)) >= 0 && exact.Cmp(big.NewInt(math.MaxInt64)) <= 0
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

// #612 review: the declared-type check admits the whole int64 range through
// the exact sidecar, but eav_data and double_* persist the float64 image, and
// every int64 from 2^63-512 up has the image 2^63. The funnel used to admit
// MaxInt64 into eav_data, where the census then reported it as a bigint out
// of contract and the OLTP read wrapped it through int64(). Admission and the
// census must agree on the stored image in both directions.
func TestCheckStorageFit_BigintImageDestinationParity(t *testing.T) {
	values := []any{
		int64(math.MaxInt64), "9223372036854775807", json.Number("9223372036854775807"),
		int64(maxBigintImageFit), json.Number("9223372036854775295"),
		int64(maxBigintImageFit + 1), json.Number("9223372036854775296"),
		int64(math.MinInt64), "-9223372036854775808", int64(-math.MaxInt64),
		int64(1) << 62, int64(1)<<53 + 1, 0, -1,
	}
	for _, dest := range bigintImageDestinations {
		for _, value := range values {
			t.Run(fmt.Sprintf("%s/%v(%T)", dest.name, value, value), func(t *testing.T) {
				rec := model.EAVRecord{ArrayIndices: dest.indices}
				_, err := populateTypedValue(&rec, "n", value, dest.meta)
				require.NotNil(t, rec.ValueNumeric)
				if err != nil {
					require.ErrorIs(t, err, forma.ErrInvalidInput)
					require.False(t, censusAdmitsBigint(*rec.ValueNumeric),
						"refused a value whose image %v the census admits", *rec.ValueNumeric)
					return
				}
				image := storedBigintImage(t, rec, dest.meta)
				require.True(t, censusAdmitsBigint(image), "admitted a value stored as %v, past int64", image)
			})
		}
	}
}

// The boundary is exact: 2^63-513 is the largest int64 admitted, and the
// message names it along with the image the caller's value became.
func TestCheckStorageFit_BigintImageBoundaryMessage(t *testing.T) {
	for _, dest := range bigintImageDestinations {
		t.Run(dest.name, func(t *testing.T) {
			ok := model.EAVRecord{ArrayIndices: dest.indices}
			_, err := populateTypedValue(&ok, "n", int64(maxBigintImageFit), dest.meta)
			require.NoError(t, err)

			refused := model.EAVRecord{ArrayIndices: dest.indices}
			_, err = populateTypedValue(&refused, "n", json.Number("9223372036854775296"), dest.meta)
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			msg, published := forma.ResolvePublicMessage(err)
			require.True(t, published, "the refusal describes the caller's value, so it is published")
			require.Contains(t, msg, "attribute 'n'")
			require.Contains(t, msg, "bigint value 9223372036854775296 does not fit")
			require.Contains(t, msg, "float64 image 9223372036854775808")
			require.Contains(t, msg, "allowed [-9223372036854775808, 9223372036854775295]")
		})
	}
}

// A bigint column persists the exact sidecar, so the image rule does not
// narrow it: MaxInt64 stays admitted there (#205).
func TestCheckStorageFit_BigintColumnKeepsFullRange(t *testing.T) {
	var rec model.EAVRecord
	_, err := populateTypedValue(&rec, "n", json.Number("9223372036854775807"),
		boundMeta(forma.ValueTypeBigInt, forma.MainColumnBigint01, forma.MainColumnEncodingDefault))
	require.NoError(t, err)
}
