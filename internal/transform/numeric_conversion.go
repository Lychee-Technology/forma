package transform

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/lychee-technology/forma/internal/numutil"
)

func toFloat64ForEAV(value any) (float64, error) {
	switch v := value.(type) {
	case *float64:
		return requiredFloat64FromPointer(v, "float64")
	case *float32:
		return requiredFloat64FromPointer(v, "float32")
	case *int:
		return requiredFloat64FromPointer(v, "int")
	case *int16:
		return requiredFloat64FromPointer(v, "int16")
	case *int32:
		return requiredFloat64FromPointer(v, "int32")
	case *int64:
		return requiredFloat64FromPointer(v, "int64")
	case string:
		return parseTrimmedFloat64(v)
	case *string:
		str, err := derefPointer(v, "string")
		if err != nil {
			return 0, err
		}
		return parseTrimmedFloat64(str)
	default:
		return numutil.Float64(value)
	}
}

func parseTrimmedFloat64(value string) (float64, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, fmt.Errorf("empty string")
	}
	parsed, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return 0, fmt.Errorf("parse float: %w", err)
	}
	return parsed, nil
}

func requiredFloat64FromPointer[T numutil.NumericScalar](value *T, typeName string) (float64, error) {
	scalar, err := derefPointer(value, typeName)
	if err != nil {
		return 0, err
	}
	return numutil.Float64(scalar)
}

// finiteForEAV rejects NaN and ±Inf after any numeric coercion. ValueNumeric
// feeds JSON read paths that cannot represent them — json.Marshal fails — so a
// stored non-finite would 500 every subsequent read of the row (#322). The
// guard sits after coercion because the spellings multiply before it:
// strconv.ParseFloat accepts "NaN"/"Inf"/"Infinity" strings, and json.Number
// can carry them too.
//
// It deliberately does not live inside numutil.Float64: that helper also
// serves the DuckDB read/query path, and hardening a shared function for one
// caller's write semantics is the #301 trap.
func finiteForEAV(value float64) (float64, error) {
	if isNonFinite(value) {
		return 0, fmt.Errorf("non-finite number %v is not storable; a finite value is required", value)
	}
	return value, nil
}

// isNonFinite is the shared predicate behind both guards in this file. They
// share the test and deliberately not the prose: the two rejections are about
// different things, and a caller reading one should not be handed the other's
// vocabulary.
func isNonFinite(value float64) bool {
	return math.IsNaN(value) || math.IsInf(value, 0)
}

// finiteBoolInput guards the bool funnels, which reach storage through the same
// ValueNumeric column as the numeric ones (boolToFloat64) and so need the same
// rejection. Coercion cannot stand in for it: no non-finite has a truth value,
// and the two funnels do not even agree on the one they invent — toBool's
// `!= 0` turns NaN into true, toBoolForEAV's float64ToBool threshold turns the
// same NaN into false. Absorbed under report-only mode that silently persisted
// a bool the caller never wrote (#322, PR #403 review).
//
// The message is its own rather than finiteForEAV's: "is not storable" is
// numeric prose, and the fault here is not that the value cannot be stored but
// that it does not denote true or false. That reading also has to hold on the
// read path — extractValueFromEAVRecord sends a stored ValueNumeric back
// through toBoolForEAV — where the subject is a persisted value, not input.
func finiteBoolInput(value float64) error {
	if isNonFinite(value) {
		return fmt.Errorf("non-finite value %v has no truth value; a finite value is required", value)
	}
	return nil
}
