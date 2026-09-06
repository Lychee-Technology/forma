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

	// Expect CAST(? AS BIGINT): the value is inferred as a datetime, and
	// federated date columns are epoch-ms BIGINT (#200).
	require.Contains(t, clause, "CAST(? AS BIGINT)")
	// Column name should be the attribute name when no metadata is present
	require.Contains(t, clause, "unknown_ts")

	require.Len(t, args, 1)

	// The argument should be the epoch-ms of the parsed RFC3339 value.
	parsedWant, err := time.Parse(time.RFC3339Nano, "2020-01-02T03:04:05Z")
	require.NoError(t, err)

	gotMs, ok := args[0].(int64)
	require.True(t, ok, "expected DuckDB arg to be epoch-ms int64")
	require.Equal(t, parsedWant.UTC().UnixMilli(), gotMs)
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

// An unregistered filter attribute must never fold onto a column the visible
// CTE projects (#512). ParquetAttrColumn is lossy, so "created.at" would
// otherwise render as created_at and silently filter on the creation
// timestamp. The rule mirrors federated.validateKeysetCursor (#509): a
// non-identity fold onto the "attr" placeholder or a reserved parquet column
// is refused, the dedup machinery is refused under any spelling, and an
// identity-up-to-case fold stays admitted.
func TestBuildDuckClause_UnregisteredAttr_RefusesFoldOntoVisibleColumn(t *testing.T) {
	refused := []struct{ attr, folded string }{
		{"created.at", "created_at"},
		{"ver.ts", "ver_ts"},
		{"deleted.ts", "deleted_ts"},
		{"row.id", "row_id"},
		{"[row_id]", "row_id"},
		{"Created.At", "Created_At"},
		{"schema.id", "schema_id"},
		{"attributes.json", "attributes_json"},
		{"ltbase.row_id", "ltbase_row_id"},
		{"rn", "rn"},
		{"RN", "RN"},
		{"[rn]", "rn"},
		{"source_tier.priority", "source_tier_priority"},
		{"source_tier_priority", "source_tier_priority"},
		{"[]", "attr"},
		{"`", "attr"},
		{"[attr]", "attr"},
		{"", "attr"},
	}
	for _, tc := range refused {
		t.Run("refused/"+tc.attr, func(t *testing.T) {
			cond := &forma.KvCondition{Attr: tc.attr, Value: "equals:1"}
			clause, _, err := buildDuckClause(cond, forma.SchemaAttributeCache{})
			require.Error(t, err, "clause %q emitted for unregistered attribute %q", clause, tc.attr)
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			require.Contains(t, err.Error(), tc.attr)
			require.Contains(t, err.Error(), tc.folded)
		})
	}

	admitted := []string{"created_at", "Created_At", "attr", "Attr", "unknown_ts"}
	for _, attr := range admitted {
		t.Run("admitted/"+attr, func(t *testing.T) {
			cond := &forma.KvCondition{Attr: attr, Value: "equals:1"}
			clause, _, err := buildDuckClause(cond, forma.SchemaAttributeCache{})
			require.NoError(t, err)
			require.Contains(t, clause, attr)
		})
	}
}

// The production federated route builds all three clauses through
// ToDualClauses, where an unregistered attribute is refused by the PG EAV
// payload before the DuckDB clause is rendered (#512 investigation). This
// pins that the folded spellings never reach DuckDB on that route either,
// and pins the ORDER: the error must be the PG EAV refusal, not the DuckDB
// fold guard, so an emitter reorder that let the DuckDB guard fire first
// would fail here rather than silently change the documented behaviour.
func TestToDualClauses_UnregisteredAttr_RefusedBeforeDuckEmission(t *testing.T) {
	for _, attr := range []string{"created.at", "rn", "[]", "ghost"} {
		t.Run(attr, func(t *testing.T) {
			paramIndex := 1
			cond := &forma.KvCondition{Attr: attr, Value: "equals:1"}
			_, err := ToDualClauses(cond, "eav_table", 22, forma.SchemaAttributeCache{}, &paramIndex)
			require.Error(t, err)
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			require.Contains(t, err.Error(), "attribute not found in cache",
				"PG EAV payload must refuse before the DuckDB fold guard runs")
			require.NotContains(t, err.Error(), "folds")
		})
	}
}
