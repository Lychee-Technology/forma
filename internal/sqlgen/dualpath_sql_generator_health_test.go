package sqlgen

import (
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

func TestHealth_BasicSmoke(t *testing.T) {
	// text
	cacheText := forma.SchemaAttributeCache{
		"name": forma.AttributeMetadata{
			AttributeID: 1,
			ValueType:   forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{
				ColumnName: forma.MainColumn("text_01"),
			},
		},
	}
	kvText := &forma.KvCondition{Attr: "name", Value: "starts_with:Al"}
	// duck clause should be produced
	dc, darr, err := buildDuckClause(kvText, cacheText)
	require.NoError(t, err)
	require.NotEmpty(t, dc)
	require.NotEmpty(t, darr)

	// numeric
	cacheNum := forma.SchemaAttributeCache{
		"amount": forma.AttributeMetadata{
			AttributeID: 2,
			ValueType:   forma.ValueTypeNumeric,
			ColumnBinding: &forma.MainColumnBinding{
				ColumnName: forma.MainColumn("double_01"),
			},
		},
	}
	kvNum := &forma.KvCondition{Attr: "amount", Value: "gt:10"}
	dc, darr, err = buildDuckClause(kvNum, cacheNum)
	require.NoError(t, err)
	require.NotEmpty(t, dc)
	require.NotEmpty(t, darr)

	// date
	cacheDate := forma.SchemaAttributeCache{
		"createdAt": forma.AttributeMetadata{
			AttributeID: 3,
			ValueType:   forma.ValueTypeDateTime,
			ColumnBinding: &forma.MainColumnBinding{
				ColumnName: forma.MainColumn("bigint_01"),
				Encoding:   forma.MainColumnEncodingUnixMs,
			},
		},
	}
	// use ISO string
	now := time.Now().UTC().Format(time.RFC3339)
	kvDate := &forma.KvCondition{Attr: "createdAt", Value: "gte:" + now}
	dc, darr, err = buildDuckClause(kvDate, cacheDate)
	require.NoError(t, err)
	require.NotEmpty(t, dc)
	require.NotEmpty(t, darr)

	// bool
	cacheBool := forma.SchemaAttributeCache{
		"active": forma.AttributeMetadata{
			AttributeID: 4,
			ValueType:   forma.ValueTypeBool,
			ColumnBinding: &forma.MainColumnBinding{
				ColumnName: forma.MainColumn("text_02"),
				Encoding:   forma.MainColumnEncodingBoolText,
			},
		},
	}
	kvBool := &forma.KvCondition{Attr: "active", Value: "equals:1"}
	dc, darr, err = buildDuckClause(kvBool, cacheBool)
	require.NoError(t, err)
	require.NotEmpty(t, dc)
	require.NotEmpty(t, darr)
}

func TestHealth_UnsupportedOperator(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"foo": forma.AttributeMetadata{
			AttributeID: 10,
			ValueType:   forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{
				ColumnName: forma.MainColumn("text_99"),
			},
		},
	}
	kv := &forma.KvCondition{Attr: "foo", Value: "unknownop:bar"}
	// buildDuckClause should return error for unsupported operator
	_, _, err := buildDuckClause(kv, cache)
	require.Error(t, err)
}

func TestHealth_MissingColumnBinding(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"orphan": forma.AttributeMetadata{
			AttributeID: 20,
			ValueType:   forma.ValueTypeText,
			// no ColumnBinding
		},
	}
	kv := &forma.KvCondition{Attr: "orphan", Value: "equals:val"}
	// duck clause still produced
	dc, darr, err := buildDuckClause(kv, cache)
	require.NoError(t, err)
	require.Equal(t, "orphan = ?", dc)
	require.Equal(t, []any{"val"}, darr)
}
