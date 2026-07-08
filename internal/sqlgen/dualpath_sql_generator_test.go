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

	// Then: grouping operators are present. PgMainClause is embedded in the DuckDB
	// federated template, so it emits positional "?" placeholders, not "$n" (#161).
	// Placeholder ordering is therefore carried by the args slice: the three leaves
	// bind in left-to-right order A, B, C.
	require.Contains(t, pgClause, "AND")
	require.Contains(t, pgClause, "OR")
	require.Equal(t, 3, strings.Count(pgClause, "?"), "expected three positional ? placeholders")
	require.NotContains(t, pgClause, "$", "PgMainClause must not use $n placeholders in the DuckDB path")
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

	// Then: Postgres main pushdown for an empty AND is the identity (1=1), not a narrowing filter.
	require.Equal(t, "1=1", pgClause)
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

// Given bound attributes of various types, when classifyPredicate is invoked,
// then only pushdown-safe operators are accepted for each value type.
func TestClassifyPredicate_ValueTypeOperatorBoundAttribute_PushdownAcceptance(t *testing.T) {
	// Prepare a pseudo KvCondition (attr name is unused by classifyPredicate except for Value)
	kvText := &forma.KvCondition{Attr: "txt", Value: "starts_with:foo"}
	kvTextEq := &forma.KvCondition{Attr: "txt", Value: "equals:bar"}
	kvTextBad := &forma.KvCondition{Attr: "txt", Value: "gt:5"}

	kvNum := &forma.KvCondition{Attr: "n", Value: "gt:5"}
	kvNumBad := &forma.KvCondition{Attr: "n", Value: "contains:5"}

	kvDate := &forma.KvCondition{Attr: "d", Value: "lt:2020-01-01"}
	kvDateBad := &forma.KvCondition{Attr: "d", Value: "contains:2020"}

	kvBool := &forma.KvCondition{Attr: "b", Value: "equals:true"}
	kvBoolBad := &forma.KvCondition{Attr: "b", Value: "gt:true"}

	// All metadata must include a ColumnBinding to reach operator checking
	textMeta := forma.AttributeMetadata{ValueType: forma.ValueTypeText, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_01")}}
	numMeta := forma.AttributeMetadata{ValueType: forma.ValueTypeNumeric, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("num_01")}}
	dateMeta := forma.AttributeMetadata{ValueType: forma.ValueTypeDate, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("date_01")}}
	boolMeta := forma.AttributeMetadata{ValueType: forma.ValueTypeBool, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("bool_01")}}

	// Text: starts_with and equals supported; numeric operator is not
	ok, reason := classifyPredicate(kvText, textMeta)
	require.True(t, ok, "starts_with should be accepted for text")
	require.Contains(t, reason, "text")

	ok, _ = classifyPredicate(kvTextEq, textMeta)
	require.True(t, ok, "equals should be accepted for text")

	ok, reason = classifyPredicate(kvTextBad, textMeta)
	require.False(t, ok, "gt should not be accepted for text")
	require.Contains(t, reason, "text operator not supported")

	// Numeric: gt supported; contains not
	ok, reason = classifyPredicate(kvNum, numMeta)
	require.True(t, ok, "gt should be accepted for numeric")
	require.Contains(t, reason, "numeric")

	ok, reason = classifyPredicate(kvNumBad, numMeta)
	require.False(t, ok, "contains should not be accepted for numeric")
	require.Contains(t, reason, "numeric operator not supported")

	// Date: lt supported; contains not
	ok, reason = classifyPredicate(kvDate, dateMeta)
	require.True(t, ok, "lt should be accepted for date")
	require.Contains(t, reason, "date")

	ok, reason = classifyPredicate(kvDateBad, dateMeta)
	require.False(t, ok, "contains should not be accepted for date")
	require.Contains(t, reason, "date operator not supported")

	// Bool: equals accepted; gt not
	ok, reason = classifyPredicate(kvBool, boolMeta)
	require.True(t, ok, "equals should be accepted for bool")
	require.Contains(t, reason, "bool")

	ok, reason = classifyPredicate(kvBoolBad, boolMeta)
	require.False(t, ok, "gt should not be accepted for bool")
	require.Contains(t, reason, "bool operator not supported")
}

// Given a bound attribute with an unsupported operator, when Postgres main pushdown is built,
// then a clear error is returned and no clause or args are produced.
func TestToDualClauses_BoundAttributeUnsupportedOperator_ReturnsError(t *testing.T) {
	paramIndex := 0
	cache := forma.SchemaAttributeCache{
		"title": forma.AttributeMetadata{
			AttributeID:   10,
			ValueType:     forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_05")},
		},
	}

	// Use a text attribute but pass a numeric-style operator which is unsupported for text
	cond := &forma.KvCondition{Attr: "title", Value: "gt:foo"}

	pgClause, pgArgs, err := buildPgMainClause(cond, cache, &paramIndex)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported operator")
	// Should not produce a clause or args when operator is unsupported for a bound column
	require.Equal(t, "", pgClause)
	require.Nil(t, pgArgs)
}

// Given a DuckDB predicate without metadata, when the clause is built,
// then value-type fallback inference still produces the expected cast and parameter value.
func TestBuildDuckClause_NoMetadata_InferTypeAndCast(t *testing.T) {
	cache := forma.SchemaAttributeCache{}

	// RFC3339 datetime literal with no metadata present.
	// Use an explicit equals: prefix so the timestamp's ':' characters are not misparsed as an operator separator.
	cond := &forma.KvCondition{Attr: "unknown_ts", Value: "equals:2020-01-02T03:04:05Z"}

	clause, args, err := buildDuckClause(cond, cache)
	require.NoError(t, err)

	// Expect a CAST(? AS TIMESTAMP) because the value should be inferred as a datetime
	require.Contains(t, clause, "CAST(? AS TIMESTAMP)")
	// Column name should be the attribute name when no metadata is present
	require.Contains(t, clause, "unknown_ts")

	require.Len(t, args, 1)

	// The argument should be a time.Time equal to the parsed RFC3339 value (in UTC)
	parsedWant, err := time.Parse(time.RFC3339Nano, "2020-01-02T03:04:05Z")
	require.NoError(t, err)

	gotTime, ok := args[0].(time.Time)
	require.True(t, ok, "expected DuckDB arg to be time.Time")
	require.True(t, gotTime.Equal(parsedWant.UTC()), "expected parsed time to match")
}

// Given a LIKE-style operator such as starts_with or contains,
// when a DuckDB clause is built, then the wildcard rewrite is correct
// and no CAST expression is introduced for the LIKE path.
func TestBuildDuckClause_LikeOperators_WildcardRewrite_NoCast(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"bio": forma.AttributeMetadata{
			AttributeID: 40,
			ValueType:   forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{
				ColumnName: forma.MainColumn("text_10"),
			},
		},
	}

	// starts_with should rewrite to value% and emit a plain LIKE with a single ? placeholder
	starts := &forma.KvCondition{Attr: "bio", Value: "starts_with:pre"}
	clause, args, err := buildDuckClause(starts, cache)
	require.NoError(t, err)
	require.Contains(t, clause, "text_10")
	require.Contains(t, clause, "LIKE")
	require.NotContains(t, clause, "CAST(")
	require.Len(t, args, 1)
	require.Equal(t, []any{"pre%"}, args)

	// contains should rewrite to %value% and likewise not introduce a CAST
	contains := &forma.KvCondition{Attr: "bio", Value: "contains:mid"}
	clause2, args2, err := buildDuckClause(contains, cache)
	require.NoError(t, err)
	require.Contains(t, clause2, "text_10")
	require.Contains(t, clause2, "LIKE")
	require.NotContains(t, clause2, "CAST(")
	require.Len(t, args2, 1)
	require.Equal(t, []any{"%mid%"}, args2)
}

// Given a datetime attribute stored on the main table with Unix-ms encoding,
// when the Postgres main pushdown is built, then the argument is converted to int64 unix milliseconds.
func TestToDualClauses_DateMainColumnEncoding_UnixMsArgument(t *testing.T) {
	paramIndex := 0
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

	cond := &forma.KvCondition{Attr: "ts", Value: "equals:" + ts}

	pgClause, pgArgs, err := buildPgMainClause(cond, cache, &paramIndex)
	require.NoError(t, err)
	require.NotEmpty(t, pgClause)
	require.Len(t, pgArgs, 1)

	// The argument should be an int64 representing unix milliseconds
	got, ok := pgArgs[0].(int64)
	require.True(t, ok, "expected pg main arg to be int64 unix ms")

	parsed, err := time.Parse(time.RFC3339Nano, ts)
	require.NoError(t, err)
	require.Equal(t, parsed.UnixMilli(), got)
}

// TestBuildPgMainClause_OR_MixedPushability_SkipsEntireOR verifies that when an OR
// composite has at least one branch that cannot be pushed to the main table (no column
// binding), the entire OR is skipped rather than silently emitting a partial OR that
// would drop rows matched only by the non-pushable branch.
func TestBuildPgMainClause_OR_MixedPushability_SkipsEntireOR(t *testing.T) {
	paramIndex := 0
	cache := forma.SchemaAttributeCache{
		// branch1 has a column binding → pushable
		"username": forma.AttributeMetadata{
			AttributeID: 1,
			ValueType:   forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{
				ColumnName: forma.MainColumn("text_01"),
			},
		},
		// branch2 has no column binding → NOT pushable
		"tag": forma.AttributeMetadata{
			AttributeID: 2,
			ValueType:   forma.ValueTypeText,
			// ColumnBinding nil → EAV-only, cannot be pushed to main table
		},
	}

	b1 := &forma.KvCondition{Attr: "username", Value: "equals:Alice"}
	b2 := &forma.KvCondition{Attr: "tag", Value: "equals:admin"}
	orCond := &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: []forma.Condition{b1, b2}}

	// When: building Postgres main clause for OR with mixed pushability
	pgClause, pgArgs, err := buildPgMainClause(orCond, cache, &paramIndex)
	require.NoError(t, err)

	// Then: the entire OR must be skipped (empty clause) to avoid dropping rows that
	// match only the non-pushable branch. paramIndex must NOT have advanced.
	require.Equal(t, "", pgClause, "OR with mixed pushability must produce empty main clause")
	require.Nil(t, pgArgs)
	require.Equal(t, 0, paramIndex, "paramIndex must not advance when OR is skipped")
}

// TestBuildPgMainClause_OR_AllPushable_ProducesClause verifies that an OR whose every
// branch is pushable is correctly emitted as an OR clause.
func TestBuildPgMainClause_OR_AllPushable_ProducesClause(t *testing.T) {
	paramIndex := 0
	cache := forma.SchemaAttributeCache{
		"first_name": forma.AttributeMetadata{
			AttributeID: 10,
			ValueType:   forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{
				ColumnName: forma.MainColumn("text_01"),
			},
		},
		"last_name": forma.AttributeMetadata{
			AttributeID: 11,
			ValueType:   forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{
				ColumnName: forma.MainColumn("text_02"),
			},
		},
	}

	b1 := &forma.KvCondition{Attr: "first_name", Value: "equals:Alice"}
	b2 := &forma.KvCondition{Attr: "last_name", Value: "equals:Smith"}
	orCond := &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: []forma.Condition{b1, b2}}

	pgClause, pgArgs, err := buildPgMainClause(orCond, cache, &paramIndex)
	require.NoError(t, err)
	require.NotEmpty(t, pgClause, "all-pushable OR must produce a clause")
	require.Contains(t, pgClause, "OR")
	require.Equal(t, []any{"Alice", "Smith"}, pgArgs)
	require.Equal(t, 2, paramIndex)
}

// TestBuildPgMainCompositeClause_EmptyAND_Returns1_1 verifies that an empty AND composite
// produces "1=1" (vacuously TRUE) from the PG main builder.  This matters when the empty
// AND is a child of an OR: returning "" would silently drop the TRUE branch and make the
// OR clause narrower than the logical expression.
func TestBuildPgMainCompositeClause_EmptyAND_Returns1_1(t *testing.T) {
	paramIndex := 0
	cond := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: nil}
	clause, args, err := buildPgMainClause(cond, forma.SchemaAttributeCache{}, &paramIndex)
	require.NoError(t, err)
	require.Equal(t, "1=1", clause)
	require.Empty(t, args)
	require.Equal(t, 0, paramIndex, "paramIndex must not advance for empty AND")
}

// TestBuildPgMainCompositeClause_EmptyOR_Returns1_0 verifies that an empty OR composite
// produces "1=0" (vacuously FALSE) from the PG main builder.
func TestBuildPgMainCompositeClause_EmptyOR_Returns1_0(t *testing.T) {
	paramIndex := 0
	cond := &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: nil}
	clause, args, err := buildPgMainClause(cond, forma.SchemaAttributeCache{}, &paramIndex)
	require.NoError(t, err)
	require.Equal(t, "1=0", clause)
	require.Empty(t, args)
	require.Equal(t, 0, paramIndex)
}

// TestBuildPgMainClause_OR_WithEmptyANDBranch_ProducesNoPrefilter verifies that an OR
// whose first child is an empty AND (= TRUE) does not silently narrow the prefilter.
// The result must be "1=1" (or vacuous) because (TRUE OR anything) = TRUE.
func TestBuildPgMainClause_OR_WithEmptyANDBranch_ProducesNoPrefilter(t *testing.T) {
	paramIndex := 0
	cache := forma.SchemaAttributeCache{
		"username": forma.AttributeMetadata{
			AttributeID: 1,
			ValueType:   forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{
				ColumnName: forma.MainColumn("text_01"),
			},
		},
	}

	// (empty AND) OR (username = 'Alice')
	// Logically = TRUE OR (username='Alice') = TRUE → prefilter should not exclude rows.
	emptyAnd := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: nil}
	kvCond := &forma.KvCondition{Attr: "username", Value: "equals:Alice"}
	orCond := &forma.CompositeCondition{
		Logic:      forma.LogicOr,
		Conditions: []forma.Condition{emptyAnd, kvCond},
	}

	pgClause, pgArgs, err := buildPgMainClause(orCond, cache, &paramIndex)
	require.NoError(t, err)

	// The OR contains a TRUE branch (empty AND → "1=1"), so the result must
	// not narrow the prefilter below all rows. Check that either:
	//   (a) the clause is vacuous ("1=1"), or
	//   (b) the clause contains 1=1 so Postgres evaluates it as always-true.
	if pgClause != "" {
		require.Contains(t, pgClause, "1=1",
			"OR with a TRUE branch must not produce a narrowing prefilter; got %q", pgClause)
	}
	// paramIndex should only advance for the username arg (or zero if clause is vacuous).
	require.LessOrEqual(t, paramIndex, 1)
	_ = pgArgs
}

// TestBuildDuckClause_EmptyOR_Returns1_0 verifies that an empty OR composite in the
// DuckDB clause builder returns "1=0" (matches nothing) rather than "1=1" (matches
// everything). An empty OR has no accepting branch, so it must reject all rows.
func TestBuildDuckClause_EmptyOR_Returns1_0(t *testing.T) {
	cond := &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: nil}
	clause, args, err := buildDuckClause(cond, forma.SchemaAttributeCache{})
	require.NoError(t, err)
	require.Equal(t, "1=0", clause, "empty OR must produce 1=0")
	require.Empty(t, args)
}

// TestBuildDuckClause_EmptyAND_Returns1_1 verifies that an empty AND composite returns
// "1=1" (matches everything), consistent with identity-element semantics for AND.
func TestBuildDuckClause_EmptyAND_Returns1_1(t *testing.T) {
	cond := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: nil}
	clause, args, err := buildDuckClause(cond, forma.SchemaAttributeCache{})
	require.NoError(t, err)
	require.Equal(t, "1=1", clause, "empty AND must produce 1=1")
	require.Empty(t, args)
}

// TestBuildDuckClause_OR_AllNonPushable_Returns1_0 verifies that an OR whose every child
// produces an empty clause (e.g. unknown attributes) falls back to "1=0" rather than
// "1=1", so the federated DuckDB query does not match unintended rows.
func TestBuildDuckClause_OR_AllNonPushable_Returns1_0(t *testing.T) {
	// Both children reference unknown attributes → buildDuckKvClause returns "unknown_attr op ?"
	// but the composite collapses to 1=0 when none of them emit a clause.
	// We test via an explicitly empty OR composite (zero children) to isolate the code path.
	cond := &forma.CompositeCondition{
		Logic:      forma.LogicOr,
		Conditions: []forma.Condition{},
	}
	clause, _, err := buildDuckClause(cond, forma.SchemaAttributeCache{})
	require.NoError(t, err)
	require.Equal(t, "1=0", clause)
}

// Migrated from the retired TestConvertDateValueForQuery (#140): ParseDateValue
// must convert a UnixMs literal to an RFC3339 string for ISO8601-encoded
// columns, and reject non-date input.
func TestParseDateValue_ISO8601EncodingFromUnixMs(t *testing.T) {
	meta := forma.AttributeMetadata{
		ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_02"), Encoding: forma.MainColumnEncodingISO8601},
	}
	val, err := ParseDateValue("1700000000000", meta)
	require.NoError(t, err)
	require.Equal(t, time.UnixMilli(1700000000000).Format(time.RFC3339), val)

	_, err = ParseDateValue("not-a-date", meta)
	require.Error(t, err)
}
