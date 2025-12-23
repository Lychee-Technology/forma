package internal

import (
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

func TestHealth_BasicSmoke(t *testing.T) {
	paramIndex := 0

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
	// classify should allow text starts_with
	use, _ := classifyPredicate(kvText, cacheText["name"])
	require.True(t, use)
	// pg main clause should be produced
	sql, args, err := buildPgMainClause(kvText, cacheText, &paramIndex)
	require.NoError(t, err)
	require.NotEmpty(t, sql)
	require.NotEmpty(t, args)
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
	use, _ = classifyPredicate(kvNum, cacheNum["amount"])
	require.True(t, use)
	sql, args, err = buildPgMainClause(kvNum, cacheNum, &paramIndex)
	require.NoError(t, err)
	require.NotEmpty(t, sql)
	require.NotEmpty(t, args)
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
	use, _ = classifyPredicate(kvDate, cacheDate["createdAt"])
	require.True(t, use)
	sql, args, err = buildPgMainClause(kvDate, cacheDate, &paramIndex)
	require.NoError(t, err)
	require.NotEmpty(t, sql)
	require.NotEmpty(t, args)
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
	use, _ = classifyPredicate(kvBool, cacheBool["active"])
	require.True(t, use)
	sql, args, err = buildPgMainClause(kvBool, cacheBool, &paramIndex)
	require.NoError(t, err)
	require.NotEmpty(t, sql)
	require.NotEmpty(t, args)
	dc, darr, err = buildDuckClause(kvBool, cacheBool)
	require.NoError(t, err)
	require.NotEmpty(t, dc)
	require.NotEmpty(t, darr)
}

func TestHealth_UnsupportedOperator(t *testing.T) {
	paramIndex := 0
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

	// buildPgMainClause should also error due to unsupported operator mapping
	_, _, err = buildPgMainClause(kv, cache, &paramIndex)
	require.Error(t, err)
}

func TestHealth_MissingColumnBinding(t *testing.T) {
	paramIndex := 0
	cache := forma.SchemaAttributeCache{
		"orphan": forma.AttributeMetadata{
			AttributeID: 20,
			ValueType:   forma.ValueTypeText,
			// no ColumnBinding
		},
	}
	kv := &forma.KvCondition{Attr: "orphan", Value: "equals:val"}
	use, _ := classifyPredicate(kv, cache["orphan"])
	require.False(t, use)
	sql, args, err := buildPgMainClause(kv, cache, &paramIndex)
	require.NoError(t, err)
	require.Equal(t, "", sql)
	require.Nil(t, args)
	// duck clause still produced
	dc, darr, err := buildDuckClause(kv, cache)
	require.NoError(t, err)
	require.Equal(t, "orphan = ?", dc)
	require.Equal(t, []any{"val"}, darr)
}

func TestHealth_BoolEncoding(t *testing.T) {
	paramIndex := 0
	// bool -> int encoding
	cacheInt := forma.SchemaAttributeCache{
		"flag": forma.AttributeMetadata{
			AttributeID: 30,
			ValueType:   forma.ValueTypeBool,
			ColumnBinding: &forma.MainColumnBinding{
				ColumnName: forma.MainColumn("smallint_01"),
				Encoding:   forma.MainColumnEncodingBoolInt,
			},
		},
	}
	kv := &forma.KvCondition{Attr: "flag", Value: "equals:1"}
	sql, args, err := buildPgMainClause(kv, cacheInt, &paramIndex)
	require.NoError(t, err)
	require.NotEmpty(t, sql)
	require.Len(t, args, 1)
	// Expect int64(1)
	require.EqualValues(t, int64(1), args[0])

	// bool -> text encoding
	cacheText := forma.SchemaAttributeCache{
		"flag": forma.AttributeMetadata{
			AttributeID: 31,
			ValueType:   forma.ValueTypeBool,
			ColumnBinding: &forma.MainColumnBinding{
				ColumnName: forma.MainColumn("text_03"),
				Encoding:   forma.MainColumnEncodingBoolText,
			},
		},
	}
	paramIndex = 0
	sql, args, err = buildPgMainClause(kv, cacheText, &paramIndex)
	require.NoError(t, err)
	require.NotEmpty(t, sql)
	require.Len(t, args, 1)
	require.Equal(t, "1", args[0])
}
