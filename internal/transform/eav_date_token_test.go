package transform

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// tokenRecord is the EAVRecord model.ParseAttributesJSON builds for a
// value_numeric token: the exact token beside its float64 image.
func tokenRecord(t *testing.T, token string) model.EAVRecord {
	t.Helper()
	var record model.PersistentRecord
	attrsJSON := `[{"schema_id":100,"attr_id":9,"value_numeric":` + token + `}]`
	if strings.HasPrefix(token, "N") || strings.Contains(token, "Infinity") {
		attrsJSON = `[{"schema_id":100,"attr_id":9,"value_numeric":"` + token + `"}]`
	}
	require.NoError(t, model.ParseAttributesJSON([]byte(attrsJSON), &record))
	require.Len(t, record.OtherAttributes, 1)
	return record.OtherAttributes[0]
}

// #592 regression matrix, transform leg: the date arm judges the token the
// read query emitted, so an image whose float64 rounds onto a valid instant
// (2^53+1 → 2^53, 9007199254740991.5 → 2^53) is refused rather than read
// as that instant, and every spelling of an admitted value (Postgres
// NUMERIC digits with a scale, the DOUBLE PRECISION exponent form) reads as
// the same instant.
func TestDateTime_ReadJudgesTheEmittedToken(t *testing.T) {
	admitted := map[string]int64{
		"0":                     0,
		"-1":                    -1,
		"1704067200123":         1704067200123,
		"1704067200123.0":       1704067200123,
		"1704067200123.000":     1704067200123,
		"1.704067200123e+12":    1704067200123,
		"9007199254740991":      9007199254740991,
		"-9007199254740991":     -9007199254740991,
		"9007199254740992":      9007199254740992,
		"-9007199254740992":     -9007199254740992,
		"9.007199254740992e+15": 9007199254740992,
	}
	for token, ms := range admitted {
		t.Run("admitted "+token, func(t *testing.T) {
			for _, vt := range []forma.ValueType{forma.ValueTypeDate, forma.ValueTypeDateTime} {
				got, err := extractValueFromEAVRecord(tokenRecord(t, token), vt)
				require.NoError(t, err)
				require.Equal(t, time.UnixMilli(ms).UTC(), got)
			}
		})
	}

	const pastFloat64Image = "is outside the epoch milliseconds a float64 image keeps exactly (up to 9007199254740992, 2^53)"
	const fraction = "(not a whole number of epoch milliseconds) names no epoch millisecond instant"
	const beyond = "(beyond any epoch millisecond instant) names no epoch millisecond instant"
	refused := map[string]string{
		"9007199254740993":      "stored value 9007199254740993 (287396-10-12T08:59:00.993Z) " + pastFloat64Image,
		"-9007199254740993":     "stored value -9007199254740993 (-283457-03-21T15:00:59.007Z) " + pastFloat64Image,
		"9007199254740994":      pastFloat64Image,
		"9.007199254740994e+15": pastFloat64Image,
		"9223372036854775807":   pastFloat64Image,
		"-9223372036854775808":  pastFloat64Image,
		"9223372036854775808":   "stored value 9223372036854775808 " + beyond,
		"1e300":                 "stored value 1e300 " + beyond,
		"-1e300":                beyond,
		"1e400":                 "stored value 1e400 " + beyond,
		"1000.5":                "stored value 1000.5 " + fraction,
		"9007199254740991.5":    "stored value 9007199254740991.5 " + fraction,
		"0.001":                 fraction,
		"NaN":                   "stored value NaN " + fraction,
		"Infinity":              "stored value Infinity " + fraction,
		"-Infinity":             "stored value -Infinity " + fraction,
	}
	for token, want := range refused {
		t.Run("refused "+token, func(t *testing.T) {
			got, err := extractValueFromEAVRecord(tokenRecord(t, token), forma.ValueTypeDateTime)
			require.Error(t, err)
			require.Nil(t, got, "a refused image is never read as absent or as another instant")
			require.Contains(t, err.Error(), want)
			require.Contains(t, err.Error(), "datetime value of attribute 9 in value_numeric")
			require.NotErrorIs(t, err, forma.ErrInvalidInput, "a stored image is the operator's, not the caller's")
		})
	}
}

// The token and the float64 rule agree wherever the float64 keeps the
// value: a record without a token (a double_* column, a record built in
// memory) is judged on its float64 by the same contract.
func TestDateTime_TokenAndFloatRulesAgree(t *testing.T) {
	for _, image := range []float64{0, -1, 1704067200123, 9007199254740992, -9007199254740992, 9007199254740994, 1000.5, math.NaN(), math.Inf(-1), 1e300} {
		spelling := formatFitValue(image)
		switch {
		case math.IsNaN(image):
			spelling = "NaN"
		case math.IsInf(image, -1):
			spelling = "-Infinity"
		}
		token := tokenRecord(t, spelling)
		v := image
		fromToken, tokenErr := extractValueFromEAVRecord(token, forma.ValueTypeDate)
		fromFloat, floatErr := extractValueFromEAVRecord(model.EAVRecord{AttrID: 9, ValueNumeric: &v}, forma.ValueTypeDate)
		require.Equal(t, floatErr == nil, tokenErr == nil, "image %v: float %v, token %v", image, floatErr, tokenErr)
		require.Equal(t, fromFloat, fromToken)
	}
}

// R5 (#592): with the special tokens no longer dropped by the parser, a
// stored NaN or infinity reaches the smallint, integer and numeric arms,
// which refuse it as a read-path consistency error naming the attribute.
// It used to read as absent, so the client saw null and an update erased
// the stored value. bigint and bool already refused it.
func TestNumericArms_RefuseNonFiniteImages(t *testing.T) {
	for _, token := range []string{"NaN", "Infinity", "-Infinity", "1e400"} {
		for _, vt := range []forma.ValueType{forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeNumeric, forma.ValueTypeBigInt, forma.ValueTypeBool} {
			t.Run(token+" "+string(vt), func(t *testing.T) {
				got, err := extractValueFromEAVRecord(tokenRecord(t, token), vt)
				require.Error(t, err)
				require.NotErrorIs(t, err, forma.ErrInvalidInput)
				if vt == forma.ValueTypeSmallInt || vt == forma.ValueTypeInteger || vt == forma.ValueTypeNumeric {
					require.Nil(t, got)
					require.Equal(t, "stored "+string(vt)+" image "+token+" of attribute 9 in value_numeric has no finite float64 value", err.Error())
				}
			})
		}
	}

	// Finite tokens keep their conversions in those arms (#384), and the
	// numeric arm reads the float64 of a token past 2^53 as it always did.
	for token, want := range map[string]any{"12": int16(12), "1000.5": int16(1000)} {
		got, err := extractValueFromEAVRecord(tokenRecord(t, token), forma.ValueTypeSmallInt)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	got, err := extractValueFromEAVRecord(tokenRecord(t, "9007199254740993"), forma.ValueTypeNumeric)
	require.NoError(t, err)
	require.Equal(t, float64(9007199254740992), got)
}
