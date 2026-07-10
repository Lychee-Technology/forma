package sqlgen

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

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
