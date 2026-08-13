package transform

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// TestPopulateTypedValueRejectsNonFiniteBool pins the bool half of #322's
// transform guard. A bool attribute stores boolToFloat64 in ValueNumeric, so a
// non-finite handed in for a bool used to be coerced silently — toBool answers
// `NaN != 0` == true — and the row was written as if the caller had said so.
// Rejecting is the only honest answer: no non-finite has a truth value.
//
// toBool has no float32 or pointer cases, so only the shapes it accepts are
// exercised here; toBoolForEAV's wider set is covered by the sibling test.
func TestPopulateTypedValueRejectsNonFiniteBool(t *testing.T) {
	meta := forma.AttributeMetadata{AttributeID: 3, ValueType: forma.ValueTypeBool}
	for name, value := range map[string]any{
		"float64 NaN":      math.NaN(),
		"float64 +Inf":     math.Inf(1),
		"json.Number NaN":  json.Number("NaN"),
		"json.Number -Inf": json.Number("-Inf"),
	} {
		t.Run(name, func(t *testing.T) {
			var attr model.EAVRecord
			_, err := populateTypedValue(&attr, "active", value, meta)
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			require.Contains(t, err.Error(), "active",
				"the rejection must name the attribute — that is what makes it actionable")
			require.Contains(t, err.Error(), "non-finite")
			require.Nil(t, attr.ValueNumeric, "nothing may be staged for storage")
		})
	}
}

// TestToEAVRecordRejectsNonFiniteBool pins the second bool funnel. It matters
// beyond symmetry: toBoolForEAV coerces through float64ToBool's threshold, so
// before the guard the two funnels disagreed about the same NaN — toBool
// persisted true, toBoolForEAV persisted false. Plain error on purpose, like
// the rest of this converter's errors.
func TestToEAVRecordRejectsNonFiniteBool(t *testing.T) {
	c := NewAttributeConverter(nil)
	negInf32 := float32(math.Inf(-1))
	nan64 := math.NaN()
	for name, value := range map[string]any{
		"float64 NaN":     math.NaN(),
		"float64 +Inf":    math.Inf(1),
		"float32 -Inf":    negInf32,
		"*float32 -Inf":   &negInf32,
		"*float64 NaN":    &nan64,
		"json.Number NaN": json.Number("NaN"),
	} {
		t.Run(name, func(t *testing.T) {
			record, err := c.ToEAVRecord(model.EntityAttribute{
				SchemaID: 1, AttrID: 3, ValueType: forma.ValueTypeBool, Value: value,
			}, uuid.New())
			require.Error(t, err)
			require.Contains(t, err.Error(), "non-finite")
			require.Nil(t, record.ValueNumeric, "nothing may be staged for storage")
		})
	}
}
