package sqlgen

import (
	"strings"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

func TestToDualClauses_NilCondition(t *testing.T) {
	paramIndex := 0
	cache := forma.SchemaAttributeCache{}

	dc, err := ToDualClauses(nil, "eav_table", 1, cache, &paramIndex)
	require.NoError(t, err)
	require.Equal(t, "", dc.PgClause)
	require.Nil(t, dc.PgArgs)
	require.Equal(t, "1=1", dc.DuckClause)
	require.Nil(t, dc.DuckArgs)
}

func TestToDualClauses_SimpleKv_NoColumnBinding(t *testing.T) {
	paramIndex := 0
	cache := forma.SchemaAttributeCache{
		"name": forma.AttributeMetadata{
			AttributeID: 7,
			ValueType:   forma.ValueTypeText,
			// ColumnBinding nil -> no main column mapping
		},
	}

	cond := &forma.KvCondition{Attr: "name", Value: "equals:Alice"}
	dc, err := ToDualClauses(cond, "eav_table", 1, cache, &paramIndex)
	require.NoError(t, err)

	// DuckDB side: attribute name should be used directly
	require.Equal(t, "name = ?", dc.DuckClause)
	require.Equal(t, []any{"Alice"}, dc.DuckArgs)

	// Postgres side: should produce an EXISTS-style clause and two args (attr_id + value)
	require.NotEmpty(t, dc.PgClause)
	require.GreaterOrEqual(t, len(dc.PgArgs), 2)
	require.Equal(t, "Alice", dc.PgArgs[1])
}

func TestToDualClauses_SimpleKv_WithColumnBinding(t *testing.T) {
	paramIndex := 0
	cache := forma.SchemaAttributeCache{
		"username": forma.AttributeMetadata{
			AttributeID: 11,
			ValueType:   forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{
				ColumnName: forma.MainColumn("text_01"),
			},
		},
	}

	cond := &forma.KvCondition{Attr: "username", Value: "Alice"} // default equals
	dc, err := ToDualClauses(cond, "eav_table", 1, cache, &paramIndex)
	require.NoError(t, err)

	// DuckDB side references the attribute name (the DuckDB CTEs are aliased by
	// attribute name), not the physical entity_main column (#167).
	require.Equal(t, "username = ?", dc.DuckClause)
	require.Equal(t, []any{"Alice"}, dc.DuckArgs)

	// Postgres side still present
	require.NotEmpty(t, dc.PgClause)
	require.GreaterOrEqual(t, len(dc.PgArgs), 2)
}

func TestToDualClauses_NestedAndOr_GroupingAndOrdering(t *testing.T) {
	// Given: a nested composite condition A AND (B OR C) with column bindings
	cache := forma.SchemaAttributeCache{
		"a": forma.AttributeMetadata{
			AttributeID:   1,
			ValueType:     forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_01")},
		},
		"b": forma.AttributeMetadata{
			AttributeID:   2,
			ValueType:     forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_02")},
		},
		"c": forma.AttributeMetadata{
			AttributeID:   3,
			ValueType:     forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_03")},
		},
	}

	a := &forma.KvCondition{Attr: "a", Value: "equals:A"}
	b := &forma.KvCondition{Attr: "b", Value: "equals:B"}
	c := &forma.KvCondition{Attr: "c", Value: "equals:C"}

	inner := &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: []forma.Condition{b, c}}
	root := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{a, inner}}

	// When: building the DuckDB clause
	duckClause, duckArgs, err := buildDuckClause(root, cache)
	require.NoError(t, err)

	// Then: it preserves grouping, uses positional "?" placeholders only, and
	// binds the three leaves in left-to-right order A, B, C.
	require.Contains(t, duckClause, "AND")
	require.Contains(t, duckClause, "OR")
	require.Equal(t, 3, strings.Count(duckClause, "?"), "expected three positional ? placeholders")
	require.NotContains(t, duckClause, "$", "the DuckDB clause must not use $n placeholders (#161)")
	require.Equal(t, []any{"A", "B", "C"}, duckArgs)
}

// Given an empty composite condition, when the DuckDB clause is built,
// then it uses the no-op behavior.
func TestToDualClauses_EmptyComposite_NoOpBehavior(t *testing.T) {
	cache := forma.SchemaAttributeCache{}

	empty := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{}}

	// When: building DuckDB clause for an empty composite
	duckClause, duckArgs, err := buildDuckClause(empty, cache)
	require.NoError(t, err)

	// Then: DuckDB should produce the 1=1 no-op clause and no args
	require.Equal(t, "1=1", duckClause)
	require.Nil(t, duckArgs)
}

// Given a bound attribute with an operator the DSL does not define, when the
// dual clauses are built, then a clear invalid-input error is returned.
func TestToDualClauses_BoundAttributeUnsupportedOperator_ReturnsError(t *testing.T) {
	paramIndex := 0
	cache := forma.SchemaAttributeCache{
		"title": forma.AttributeMetadata{
			AttributeID:   10,
			ValueType:     forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_05")},
		},
	}

	// Use a text attribute but pass an operator the DSL does not define
	cond := &forma.KvCondition{Attr: "title", Value: "similar_to:foo"}

	dc, err := ToDualClauses(cond, "eav_table", 1, cache, &paramIndex)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported operator")
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.Equal(t, DualClauses{}, dc)
}

// Given a datetime attribute stored on the main table with Unix-ms encoding,
// when the entity_main operand is converted (the hybrid builder's bind), then
// it is int64 unix milliseconds.
func TestConvertPgMainValue_DateMainColumnEncoding_UnixMsArgument(t *testing.T) {
	// RFC3339 literal
	ts := "2020-01-02T03:04:05Z"

	cache := forma.SchemaAttributeCache{
		"ts": forma.AttributeMetadata{
			AttributeID: 50,
			ValueType:   forma.ValueTypeDateTime,
			ColumnBinding: &forma.MainColumnBinding{
				ColumnName: forma.MainColumn("bigint_02"),
				Encoding:   forma.MainColumnEncodingUnixMs,
			},
		},
	}

	arg, err := ConvertPgMainValue(ts, "ts", cache["ts"])
	require.NoError(t, err)

	// The argument should be an int64 representing unix milliseconds
	got, ok := arg.(int64)
	require.True(t, ok, "expected pg main arg to be int64 unix ms")

	parsed, err := time.Parse(time.RFC3339Nano, ts)
	require.NoError(t, err)
	require.Equal(t, parsed.UnixMilli(), got)
}
