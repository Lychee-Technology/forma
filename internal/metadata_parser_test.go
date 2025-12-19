package internal

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseAttributeMetadata(t *testing.T) {
	source := "test-source"
	tests := []struct {
		name      string
		attrName  string
		attrData  map[string]any
		expectErr string
		expect    func(t *testing.T, meta forma.AttributeMetadata)
	}{
		{
			name:     "success with binding and trimmed encoding",
			attrName: "foo",
			attrData: map[string]any{
				"attributeID": 1.0,
				"valueType":   "text",
				"column_binding": map[string]any{
					"col_name": "text_01",
					"encoding": " bool_text ",
				},
			},
			expect: func(t *testing.T, meta forma.AttributeMetadata) {
				assert.Equal(t, int16(1), meta.AttributeID)
				assert.Equal(t, forma.ValueType("text"), meta.ValueType)
				if assert.NotNil(t, meta.ColumnBinding) {
					assert.Equal(t, forma.MainColumn("text_01"), meta.ColumnBinding.ColumnName)
					assert.Equal(t, forma.MainColumnEncodingBoolText, meta.ColumnBinding.Encoding)
				}
			},
		},
		{
			name:     "success with nil binding when absent",
			attrName: "bar",
			attrData: map[string]any{
				"attributeID": 2.0,
				"valueType":   "integer",
			},
			expect: func(t *testing.T, meta forma.AttributeMetadata) {
				assert.Equal(t, int16(2), meta.AttributeID)
				assert.Equal(t, forma.ValueType("integer"), meta.ValueType)
				assert.Nil(t, meta.ColumnBinding)
			},
		},
		{
			name:      "error missing attributeID",
			attrName:  "missingID",
			attrData:  map[string]any{"valueType": "text"},
			expectErr: "attributeID",
		},
		{
			name:      "error invalid valueType",
			attrName:  "missingValueType",
			attrData:  map[string]any{"attributeID": 3.0},
			expectErr: "valueType",
		},
		{
			name:     "error from binding parsing",
			attrName: "badBinding",
			attrData: map[string]any{
				"attributeID": 4.0,
				"valueType":   "text",
				"column_binding": map[string]any{
					"col_name": "",
				},
			},
			expectErr: "columnName",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			meta, err := parseAttributeMetadata(tt.attrName, tt.attrData, source)
			if tt.expectErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, tt.expect)
			tt.expect(t, meta)
		})
	}
}

func TestExtractMainColumnBinding(t *testing.T) {
	source := "test-source"
	attrName := "foo"

	t.Run("returns binding when map present", func(t *testing.T) {
		raw := map[string]any{
			"column_binding": map[string]any{
				"col_name": "text_02",
			},
		}
		binding, err := extractMainColumnBinding(attrName, raw, source)
		require.NoError(t, err)
		if assert.NotNil(t, binding) {
			assert.Equal(t, forma.MainColumn("text_02"), binding.ColumnName)
			assert.Equal(t, forma.MainColumnEncodingDefault, binding.Encoding)
		}
	})

	t.Run("returns nil when binding missing or wrong type", func(t *testing.T) {
		raw := map[string]any{
			"column_binding": "not-a-map",
		}
		binding, err := extractMainColumnBinding(attrName, raw, source)
		require.NoError(t, err)
		assert.Nil(t, binding)
	})
}

func TestParseBindingObject(t *testing.T) {
	source := "test-source"
	attrName := "foo"

	t.Run("valid binding with trimmed encoding", func(t *testing.T) {
		raw := map[string]any{
			"col_name": "text_03",
			"encoding": " unix_ms ",
		}
		binding, err := parseBindingObject(attrName, raw, source)
		require.NoError(t, err)
		require.NotNil(t, binding)
		assert.Equal(t, forma.MainColumn("text_03"), binding.ColumnName)
		assert.Equal(t, forma.MainColumnEncodingUnixMs, binding.Encoding)
	})

	t.Run("error when col_name empty", func(t *testing.T) {
		raw := map[string]any{
			"col_name": "",
		}
		binding, err := parseBindingObject(attrName, raw, source)
		require.Error(t, err)
		assert.Nil(t, binding)
		assert.Contains(t, err.Error(), "invalid columnName")
	})
}

func TestNormalizeColumnEncoding(t *testing.T) {
	assert.Equal(t, forma.MainColumnEncodingDefault, normalizeColumnEncoding(""))
	assert.Equal(t, forma.MainColumnEncodingUnixMs, normalizeColumnEncoding(" unix_ms "))
}
