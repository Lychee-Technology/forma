package transform

import (
	"errors"
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
