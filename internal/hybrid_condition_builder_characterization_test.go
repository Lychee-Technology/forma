package internal

import (
	"fmt"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// These characterization tests lock in the current SQL output of the hybrid
// condition builder so that the unified walker migration can verify byte-for-byte
// equivalence.

type hybridTestHelper struct {
	repo          *DBPersistentRecordRepository
	eavTable      string
	mainTable     string
	initArgIndex  int
	useMainAnchor bool
}

func newHybridTestHelper(useMainAnchor bool) hybridTestHelper {
	cache := NewMetadataCache()
	cache.schemaNameToID["test"] = 1
	cache.schemaIDToName[1] = "test"
	cache.schemaCaches[1] = forma.SchemaAttributeCache{
		"name": {
			AttributeID: 7,
			ValueType:   forma.ValueTypeText,
		},
		"age": {
			AttributeID:   8,
			ValueType:     forma.ValueTypeInteger,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("integer_01")},
		},
		"tag": {
			AttributeID: 9,
			ValueType:   forma.ValueTypeText,
		},
	}
	return hybridTestHelper{
		repo: &DBPersistentRecordRepository{
			metadataCache: cache,
		},
		eavTable:      "eav_data",
		mainTable:     "entity_main",
		initArgIndex:  1,
		useMainAnchor: useMainAnchor,
	}
}

func (h hybridTestHelper) build(cond forma.Condition) (string, []any, error) {
	return h.repo.buildHybridConditions(
		h.eavTable, h.mainTable,
		AttributeQuery{SchemaID: 1, Condition: cond},
		h.initArgIndex,
		h.useMainAnchor,
	)
}

func (h hybridTestHelper) buildFromQuery(query AttributeQuery) (string, []any, error) {
	return h.repo.buildHybridConditions(
		h.eavTable, h.mainTable, query,
		h.initArgIndex,
		h.useMainAnchor,
	)
}

// TestHybrid_MainColumn_AnchorTrue asserts that when useMainTableAsAnchor=true, a
// main-table column produces a direct m.* predicate with $N placeholder.
// The argCounter starts at initArgIndex (1), so the value placeholder is $2.
func TestHybrid_MainColumn_AnchorTrue(t *testing.T) {
	h := newHybridTestHelper(true)
	cond := &forma.KvCondition{Attr: "age", Value: "equals:25"}
	clause, args, err := h.build(cond)
	require.NoError(t, err)
	require.Equal(t, "m.\"integer_01\" = $2", clause)
	// tryParseNumber for integer returns int64
	require.Equal(t, []any{int64(25)}, args)
}

// TestHybrid_MainColumn_AnchorFalse asserts that when useMainTableAsAnchor=false, a
// main-table column produces an EXISTS subquery targeting t.row_id.
func TestHybrid_MainColumn_AnchorFalse(t *testing.T) {
	h := newHybridTestHelper(false)
	cond := &forma.KvCondition{Attr: "age", Value: "equals:25"}
	clause, args, err := h.build(cond)
	require.NoError(t, err)
	expected := fmt.Sprintf(
		"EXISTS (SELECT 1 FROM %s m WHERE m.ltbase_row_id = t.row_id AND m.\"integer_01\" = $2)",
		sanitizeIdentifier(h.mainTable),
	)
	require.Equal(t, expected, clause)
	require.Equal(t, []any{int64(25)}, args)
}

// TestHybrid_EAVLeaf_AnchorFalse asserts that an EAV-only attribute with
// anchor=false produces an EXISTS clause with e.* rewritten to t.*.
// argCounter starts at 1; EAV emits two params (attr_id at $2, value at $3).
func TestHybrid_EAVLeaf_AnchorFalse(t *testing.T) {
	h := newHybridTestHelper(false)
	cond := &forma.KvCondition{Attr: "name", Value: "equals:Alice"}
	clause, args, err := h.build(cond)
	require.NoError(t, err)
	require.Contains(t, clause, "x.schema_id = t.schema_id")
	require.Contains(t, clause, "x.row_id = t.row_id")
	require.NotContains(t, clause, "e.row_id")
	require.NotContains(t, clause, "e.schema_id")
	require.Len(t, args, 2)
	require.Equal(t, int16(7), args[0])
	require.Equal(t, "Alice", args[1])
}

// TestHybrid_EAVLeaf_AnchorTrue asserts that an EAV-only attribute with
// anchor=true rewrites e.* to m.ltbase_*.
func TestHybrid_EAVLeaf_AnchorTrue(t *testing.T) {
	h := newHybridTestHelper(true)
	cond := &forma.KvCondition{Attr: "name", Value: "equals:Alice"}
	clause, args, err := h.build(cond)
	require.NoError(t, err)
	require.Contains(t, clause, "x.schema_id = m.ltbase_schema_id")
	require.Contains(t, clause, "x.row_id = m.ltbase_row_id")
	require.NotContains(t, clause, "e.row_id")
	require.NotContains(t, clause, "e.schema_id")
	require.Len(t, args, 2)
	require.Equal(t, int16(7), args[0])
}

// TestHybrid_MixedMainAndEAV_AND asserts that mixing a main-table column and an
// EAV-only attribute under AND produces correct join: sub-clauses wrapped in (),
// joined with AND, and $N placeholders continuous across boundaries.
func TestHybrid_MixedMainAndEAV_AND(t *testing.T) {
	h := newHybridTestHelper(false)
	ageCond := &forma.KvCondition{Attr: "age", Value: "gt:18"}
	nameCond := &forma.KvCondition{Attr: "name", Value: "equals:Alice"}
	root := &forma.CompositeCondition{
		Logic:      forma.LogicAnd,
		Conditions: []forma.Condition{ageCond, nameCond},
	}
	clause, args, err := h.build(root)
	require.NoError(t, err)

	require.Contains(t, clause, "AND")
	require.Contains(t, clause, "EXISTS")
	require.Contains(t, clause, "m.ltbase_row_id = t.row_id")
	require.Contains(t, clause, "x.schema_id = t.schema_id")
	require.Contains(t, clause, "x.row_id = t.row_id")

	// Placeholders cross boundaries: $2 (age value), $3 (eav attr_id), $4 (eav value)
	require.Contains(t, clause, "$2")
	require.Contains(t, clause, "$3")
	require.Contains(t, clause, "$4")

	// Args: [int64(18), int16(8), "Alice"]
	require.Len(t, args, 3)
	require.Equal(t, int64(18), args[0])
	require.Equal(t, int16(7), args[1])
	require.Equal(t, "Alice", args[2])
}

// TestHybrid_EmptyCompositeAndNestedOR asserts correct handling of empty composite
// children and nested OR logic.
func TestHybrid_EmptyCompositeAndNestedOR(t *testing.T) {
	h := newHybridTestHelper(false)

	emptyAnd := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: nil}
	ageCond := &forma.KvCondition{Attr: "age", Value: "equals:25"}

	// A composite with empty AND child + real leaf under AND
	root := &forma.CompositeCondition{
		Logic:      forma.LogicAnd,
		Conditions: []forma.Condition{emptyAnd, ageCond},
	}

	clause, args, err := h.build(root)
	require.NoError(t, err)
	// empty AND produces "1=1" which becomes a wrapped sub-clause "(1=1)"
	require.Contains(t, clause, "1=1")
	require.Contains(t, clause, "AND")
	require.Contains(t, clause, "EXISTS")
	require.Len(t, args, 1)
	require.Equal(t, int64(25), args[0])

	// Mixed with nested OR
	nameCond := &forma.KvCondition{Attr: "name", Value: "equals:Alice"}
	tagCond := &forma.KvCondition{Attr: "tag", Value: "equals:admin"}
	nestedOr := &forma.CompositeCondition{
		Logic:      forma.LogicOr,
		Conditions: []forma.Condition{nameCond, tagCond},
	}
	root2 := &forma.CompositeCondition{
		Logic:      forma.LogicAnd,
		Conditions: []forma.Condition{ageCond, nestedOr},
	}

	clause2, args2, err := h.build(root2)
	require.NoError(t, err)
	require.Contains(t, clause2, "OR")
	require.Contains(t, clause2, "AND")
	// Placeholders: $2 (age), $3 (attr_id name), $4 (value name), $5 (attr_id tag), $6 (value tag)
	require.Contains(t, clause2, "$2")
	require.Contains(t, clause2, "$3")
	require.Contains(t, clause2, "$4")
	require.Contains(t, clause2, "$5")
	require.Contains(t, clause2, "$6")
	// args: [int64(25), int16(7), "Alice", int16(9), "admin"]
	require.Len(t, args2, 5)
	require.Equal(t, int64(25), args2[0])
	require.Equal(t, int16(7), args2[1])
	require.Equal(t, "Alice", args2[2])
	require.Equal(t, int16(9), args2[3])
	require.Equal(t, "admin", args2[4])
}

// TestHybrid_NilCondition asserts that a nil condition produces "1=1".
func TestHybrid_NilCondition(t *testing.T) {
	h := newHybridTestHelper(false)
	query := AttributeQuery{SchemaID: 1, Condition: nil}
	clause, args, err := h.buildFromQuery(query)
	require.NoError(t, err)
	require.Equal(t, "1=1", clause)
	require.Nil(t, args)
}

// TestHybrid_AnchorTrue_EAVAliasRewrite verifies the exact alias rewrite for anchor=true.
func TestHybrid_AnchorTrue_EAVAliasRewrite(t *testing.T) {
	h := newHybridTestHelper(true)
	cond := &forma.KvCondition{Attr: "name", Value: "contains:foo"}
	clause, args, err := h.build(cond)
	require.NoError(t, err)
	require.Contains(t, clause, "m.ltbase_schema_id")
	require.Contains(t, clause, "m.ltbase_row_id")
	require.Contains(t, clause, "value_text LIKE")
	require.Len(t, args, 2)
	// EAV: attr_id gets $2, value gets $3
	require.Equal(t, int16(7), args[0])
	require.Equal(t, "%foo%", args[1])
}

// TestHybrid_MainTableLikeOperator ensures LIKE operators on main-table columns work correctly.
func TestHybrid_MainTableLikeOperator(t *testing.T) {
	h := newHybridTestHelper(true)
	cond := &forma.KvCondition{Attr: "text_01", Value: "starts_with:A"}
	clause, args, err := h.build(cond)
	require.NoError(t, err)
	require.Equal(t, "m.\"text_01\" LIKE $2", clause)
	require.Equal(t, []any{"A%"}, args)
}
