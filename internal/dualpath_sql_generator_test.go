package internal

import (
	"strings"
	"testing"

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

	// PgMainClause should be empty because no main column binding exists
	require.Equal(t, "", dc.PgMainClause)

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

	// DuckDB side should use column binding name
	require.Equal(t, "text_01 = ?", dc.DuckClause)
	require.Equal(t, []any{"Alice"}, dc.DuckArgs)

	// Postgres side still present
	require.NotEmpty(t, dc.PgClause)
	require.GreaterOrEqual(t, len(dc.PgArgs), 2)
}

func TestToDualClauses_NestedAndOr_GroupingAndOrdering(t *testing.T) {
	// Given: a nested composite condition A AND (B OR C) with column bindings
	paramIndex := 0
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

	// When: building Postgres main clause
	pgClause, pgArgs, err := buildPgMainClause(root, cache, &paramIndex)
	require.NoError(t, err)
	require.NotEmpty(t, pgClause)

	// Then: grouping operators are present and placeholder ordering is $1 then $2 then $3
	require.Contains(t, pgClause, "AND")
	require.Contains(t, pgClause, "OR")
	i1 := strings.Index(pgClause, "$1")
	i2 := strings.Index(pgClause, "$2")
	i3 := strings.Index(pgClause, "$3")
	require.True(t, i1 >= 0 && i2 > i1 && i3 > i2, "placeholders should appear in order $1,$2,$3")
	require.Equal(t, []any{"A", "B", "C"}, pgArgs)

	// And: DuckDB clause preserves grouping and argument ordering
	duckClause, duckArgs, err := buildDuckClause(root, cache)
	require.NoError(t, err)
	require.NotEmpty(t, duckClause)
	require.Contains(t, duckClause, "AND")
	require.Contains(t, duckClause, "OR")
	// DuckDB uses ? placeholders; args should be in same logical order
	require.Equal(t, []any{"A", "B", "C"}, duckArgs)
}

// Given an empty composite condition, when main and DuckDB clauses are built,
// then both sides use their no-op behavior consistently.
func TestToDualClauses_EmptyComposite_NoOpBehavior(t *testing.T) {
	paramIndex := 0
	cache := forma.SchemaAttributeCache{}

	empty := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{}}

	// When: building Postgres main clause for an empty composite
	pgClause, pgArgs, err := buildPgMainClause(empty, cache, &paramIndex)
	require.NoError(t, err)

	// Then: Postgres main pushdown should be a no-op (empty clause, no args)
	require.Equal(t, "", pgClause)
	require.Nil(t, pgArgs)

	// When: building DuckDB clause for an empty composite
	duckClause, duckArgs, err := buildDuckClause(empty, cache)
	require.NoError(t, err)

	// Then: DuckDB should produce the 1=1 no-op clause and no args
	require.Equal(t, "1=1", duckClause)
	require.Nil(t, duckArgs)
}

// Given an unknown attribute, when Postgres main pushdown is built,
// then it is ignored rather than treated as a hard failure.
func TestToDualClauses_UnknownAttribute_IgnoredForPgMain(t *testing.T) {
	paramIndex := 0
	cache := forma.SchemaAttributeCache{}

	cond := &forma.KvCondition{Attr: "missing_attr", Value: "equals:val"}

	// When: building Postgres main clause for an unknown attribute
	pgClause, pgArgs, err := buildPgMainClause(cond, cache, &paramIndex)
	require.NoError(t, err)

	// Then: the predicate should be skipped (no clause, no args)
	require.Equal(t, "", pgClause)
	require.Nil(t, pgArgs)
}
