package transform

import (
	"encoding/json"
	"errors"
	"math"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPopulateTypedValue_List(t *testing.T) {
	listMeta := forma.AttributeMetadata{
		AttributeName: "tags", AttributeID: 18, ValueType: forma.ValueTypeList,
	}

	t.Run("text element lands in value_text (default items_type)", func(t *testing.T) {
		attr := model.EAVRecord{ArrayIndices: "0"}
		set, err := populateTypedValue(&attr, "tags", "alpha", listMeta)
		require.NoError(t, err)
		require.True(t, set)
		require.NotNil(t, attr.ValueText)
		assert.Equal(t, "alpha", *attr.ValueText)
		assert.Nil(t, attr.ValueNumeric)
	})

	t.Run("integer element lands in value_numeric (items_type integer)", func(t *testing.T) {
		intMeta := listMeta
		intMeta.ItemsType = forma.ValueTypeInteger
		attr := model.EAVRecord{ArrayIndices: "1"}
		set, err := populateTypedValue(&attr, "tags", float64(7), intMeta)
		require.NoError(t, err)
		require.True(t, set)
		require.NotNil(t, attr.ValueNumeric)
		assert.Equal(t, float64(7), *attr.ValueNumeric)
	})

	t.Run("multi-dimensional indices rejected", func(t *testing.T) {
		attr := model.EAVRecord{ArrayIndices: "0,1"}
		_, err := populateTypedValue(&attr, "tags", "x", listMeta)
		require.Error(t, err)
		assert.True(t, errors.Is(err, forma.ErrInvalidInput), "want ErrInvalidInput, got %v", err)
		assert.Contains(t, err.Error(), "multi-dimensional")
		assert.Contains(t, err.Error(), "attrID=18")
	})

	t.Run("scalar payload (empty indices) rejected", func(t *testing.T) {
		attr := model.EAVRecord{ArrayIndices: ""}
		_, err := populateTypedValue(&attr, "tags", "scalar", listMeta)
		require.Error(t, err)
		assert.True(t, errors.Is(err, forma.ErrInvalidInput), "want ErrInvalidInput, got %v", err)
		assert.Contains(t, err.Error(), "array value")
	})

	t.Run("invalid element for items_type surfaces the element error", func(t *testing.T) {
		intMeta := listMeta
		intMeta.ItemsType = forma.ValueTypeInteger
		attr := model.EAVRecord{ArrayIndices: "0"}
		_, err := populateTypedValue(&attr, "tags", "not-a-number", intMeta)
		require.Error(t, err)
		assert.True(t, errors.Is(err, forma.ErrInvalidInput), "want ErrInvalidInput, got %v", err)
	})
}

// TestPopulateTypedValueRejectsNonFiniteNumbers pins #322's transform-layer
// guard. ValueNumeric feeds JSON read paths that cannot represent NaN/Inf
// (json.Marshal fails), so a stored non-finite poisons every subsequent read
// of the row. The string spellings matter: strconv.ParseFloat accepts them,
// which made a report-only HTTP update able to store NaN before this guard.
func TestPopulateTypedValueRejectsNonFiniteNumbers(t *testing.T) {
	for _, valueType := range []forma.ValueType{
		forma.ValueTypeNumeric, forma.ValueTypeBigInt, forma.ValueTypeInteger, forma.ValueTypeSmallInt,
	} {
		meta := forma.AttributeMetadata{AttributeID: 7, ValueType: valueType}
		for name, value := range map[string]any{
			"float64 NaN":     math.NaN(),
			"float64 +Inf":    math.Inf(1),
			"float32 -Inf":    float32(math.Inf(-1)),
			"string NaN":      "NaN",
			"string Infinity": "-Infinity",
			"json.Number NaN": json.Number("NaN"),
		} {
			t.Run(string(valueType)+"/"+name, func(t *testing.T) {
				var attr model.EAVRecord
				_, err := populateTypedValue(&attr, "score", value, meta)
				require.ErrorIs(t, err, forma.ErrInvalidInput)
				require.Contains(t, err.Error(), "score",
					"the rejection must name the attribute — that is what makes it actionable")
				require.Contains(t, err.Error(), "non-finite")
				require.Nil(t, attr.ValueNumeric, "nothing may be staged for storage")
			})
		}
	}
}

// TestPopulateTypedValueRejectsNonFiniteListElement pins the list funnel: array
// elements recurse into populateTypedValue with the items type, and must hit
// the same guard.
func TestPopulateTypedValueRejectsNonFiniteListElement(t *testing.T) {
	meta := forma.AttributeMetadata{
		AttributeID: 7,
		ValueType:   forma.ValueTypeList,
		ItemsType:   forma.ValueTypeNumeric,
	}
	attr := model.EAVRecord{ArrayIndices: "0"}
	_, err := populateTypedValue(&attr, "scores", math.NaN(), meta)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.Contains(t, err.Error(), "non-finite")
}
