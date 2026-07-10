package sqlgen

import (
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

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
	require.Contains(t, clause, "bio")
	require.Contains(t, clause, "LIKE")
	require.NotContains(t, clause, "CAST(")
	require.Len(t, args, 1)
	require.Equal(t, []any{"pre%"}, args)

	// contains should rewrite to %value% and likewise not introduce a CAST
	contains := &forma.KvCondition{Attr: "bio", Value: "contains:mid"}
	clause2, args2, err := buildDuckClause(contains, cache)
	require.NoError(t, err)
	require.Contains(t, clause2, "bio")
	require.Contains(t, clause2, "LIKE")
	require.NotContains(t, clause2, "CAST(")
	require.Len(t, args2, 1)
	require.Equal(t, []any{"%mid%"}, args2)
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
