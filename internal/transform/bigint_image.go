package transform

import (
	"fmt"
	"math"
	"strconv"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// The float64 image of a bigint (#590).
//
// A declared bigint is normalised by the write funnels into two slots: the
// float64 ValueNumeric image and, when the input is an exact integer, the
// int64 ValueInt64 sidecar. Two destinations keep only the image:
// eav_data.value_numeric (storeInEAV clears the sidecar) and a double_*
// column (storeNumericRendering writes the image). Every read route converts
// that image back: int64() on the OLTP route and TRY_CAST(... AS BIGINT) on
// the DuckDB routes, whose Postgres scanner types NUMERIC as DOUBLE. A
// bigint_* column persists the sidecar and keeps the full int64 range.
//
// A float64 is exact for every integer of magnitude at most 2^53 and rounds
// past it (2^53+1 is stored as 2^53; every int64 from 2^63-512 up is stored
// as 2^63, which is past int64). The image destinations therefore admit
// exactly [-2^53, 2^53]: check-admits <=> store-writes-exactly <=>
// every-route-reads-back-the-same. Before this rule the funnel admitted the
// whole int64 range on the strength of the sidecar and eav_data persisted
// the rounded image (#205 float64 ceiling, #612 for the past-int64 slice).

// maxBigintImage is the largest magnitude a float64 image keeps exactly.
const maxBigintImage = 1 << 53

// bigintImageRange is the allowed range quoted in the refusal.
const bigintImageRange = "[-9007199254740992, 9007199254740992]"

// checkBigintImageFit refuses a declared bigint whose destination keeps only
// the float64 image when the value is past 2^53 in magnitude, that is, when
// what is stored and read back may not be the caller's value. dest labels the
// destination in the message. The value judged is the exact sidecar when the
// funnel filled it (2^53+1 has the image 2^53, which a check on the image
// alone would admit) and the image otherwise, since the image is then all
// the store has; the message names that value and the image it became.
func checkBigintImageFit(attr *model.EAVRecord, vt forma.ValueType, dest string) error {
	if vt != forma.ValueTypeBigInt || attr.ValueNumeric == nil {
		return nil
	}
	image := *attr.ValueNumeric
	value := formatFitValue(image)
	fits := math.Abs(image) <= maxBigintImage
	if attr.ValueInt64 != nil {
		exact := *attr.ValueInt64
		value = strconv.FormatInt(exact, 10)
		fits = exact >= -maxBigintImage && exact <= maxBigintImage
	}
	if fits {
		return nil
	}
	return fmt.Errorf("bigint value %s does not fit %s, which keeps only its float64 image %s (allowed %s, where the image is exact)",
		value, dest, formatFitValue(image), bigintImageRange)
}

// int64FromBigintImage is the read-side conversion of a persisted float64
// image of a bigint (eav_data.value_numeric, a double_* column). A whole
// image inside the int64 range converts exactly, and is returned as the
// stored value even past 2^53: such an image is what the table holds and
// every tier reads it the same way, although the write funnel no longer
// admits it (the #501 census names those rows). Anything else, a fraction or
// a magnitude int64() would wrap (platform-dependent: MinInt64 on amd64,
// MaxInt64 on arm64 for 2^63), names no bigint and is a read-path
// consistency error rather than a made-up value.
func int64FromBigintImage(image float64) (int64, error) {
	if !isWholeNumber(image) {
		return 0, fmt.Errorf("stored bigint image %s is not a whole number", formatFitValue(image))
	}
	if !inInt64Range(image) {
		return 0, fmt.Errorf("stored bigint image %s is outside the bigint range [-9223372036854775808, 9223372036854775807]", formatFitValue(image))
	}
	return int64(image), nil
}

// isWholeNumber reports whether a float64 is a finite integer. NaN fails the
// equality; the infinities fail the finiteness test.
func isWholeNumber(value float64) bool {
	return !math.IsInf(value, 0) && value == math.Trunc(value)
}
