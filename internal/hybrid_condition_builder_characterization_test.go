package internal

import (
	"fmt"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"

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
	cache := schemameta.NewMetadataCache()
	if err := cache.RegisterSchema("test", 1, forma.SchemaAttributeCache{
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
		"created": {
			AttributeID:   10,
			ValueType:     forma.ValueTypeDateTime,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("bigint_01"), Encoding: forma.MainColumnEncodingUnixMs},
		},
		"created_iso": {
			AttributeID:   11,
			ValueType:     forma.ValueTypeDateTime,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_02"), Encoding: forma.MainColumnEncodingISO8601},
		},
		"active_int": {
			AttributeID:   12,
			ValueType:     forma.ValueTypeBool,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("smallint_01"), Encoding: forma.MainColumnEncodingBoolInt},
		},
		"active_text": {
			AttributeID:   13,
			ValueType:     forma.ValueTypeBool,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_03"), Encoding: forma.MainColumnEncodingBoolText},
		},
		"score": {
			AttributeID:   14,
			ValueType:     forma.ValueTypeNumeric,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("double_01")},
		},
		"badcol": {
			AttributeID:   15,
			ValueType:     forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_99")},
		},
	}); err != nil {
		panic(err)
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
		model.AttributeQuery{SchemaID: 1, Condition: cond},
		h.initArgIndex,
		h.useMainAnchor,
	)
}

func (h hybridTestHelper) buildFromQuery(query model.AttributeQuery) (string, []any, error) {
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
	require.Equal(t,
		"EXISTS (SELECT 1 FROM eav_data x WHERE x.schema_id = t.schema_id AND x.row_id = t.row_id AND x.attr_id = $2 AND x.value_text = $3)",
		clause)
	require.Equal(t, []any{int16(7), "Alice"}, args)
}

// TestHybrid_EAVLeaf_AnchorTrue asserts that an EAV-only attribute with
// anchor=true rewrites e.* to m.ltbase_*.
func TestHybrid_EAVLeaf_AnchorTrue(t *testing.T) {
	h := newHybridTestHelper(true)
	cond := &forma.KvCondition{Attr: "name", Value: "equals:Alice"}
	clause, args, err := h.build(cond)
	require.NoError(t, err)
	require.Equal(t,
		"EXISTS (SELECT 1 FROM eav_data x WHERE x.schema_id = m.ltbase_schema_id AND x.row_id = m.ltbase_row_id AND x.attr_id = $2 AND x.value_text = $3)",
		clause)
	require.Equal(t, []any{int16(7), "Alice"}, args)
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

	// Sub-clauses each wrapped in (), joined with AND, no outer parens; $N
	// placeholders continuous across the main→EAV boundary ($2 age, $3 attr_id, $4 value).
	require.Equal(t,
		`(EXISTS (SELECT 1 FROM "entity_main" m WHERE m.ltbase_row_id = t.row_id AND m."integer_01" > $2)) AND (EXISTS (SELECT 1 FROM eav_data x WHERE x.schema_id = t.schema_id AND x.row_id = t.row_id AND x.attr_id = $3 AND x.value_text = $4))`,
		clause)
	// Args: [int64(18), int16(7) (attr_id of "name"), "Alice"]
	require.Equal(t, []any{int64(18), int16(7), "Alice"}, args)
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
	require.Equal(t,
		`(1=1) AND (EXISTS (SELECT 1 FROM "entity_main" m WHERE m.ltbase_row_id = t.row_id AND m."integer_01" = $2))`,
		clause)
	require.Equal(t, []any{int64(25)}, args)

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
	// Nested OR wrapped as one AND-child; placeholders $2 (age), $3/$4 (name), $5/$6 (tag).
	require.Equal(t,
		`(EXISTS (SELECT 1 FROM "entity_main" m WHERE m.ltbase_row_id = t.row_id AND m."integer_01" = $2)) AND ((EXISTS (SELECT 1 FROM eav_data x WHERE x.schema_id = t.schema_id AND x.row_id = t.row_id AND x.attr_id = $3 AND x.value_text = $4)) OR (EXISTS (SELECT 1 FROM eav_data x WHERE x.schema_id = t.schema_id AND x.row_id = t.row_id AND x.attr_id = $5 AND x.value_text = $6)))`,
		clause2)
	require.Equal(t, []any{int64(25), int16(7), "Alice", int16(9), "admin"}, args2)
}

// TestHybrid_NilCondition asserts that a nil condition produces "1=1".
func TestHybrid_NilCondition(t *testing.T) {
	h := newHybridTestHelper(false)
	query := model.AttributeQuery{SchemaID: 1, Condition: nil}
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
	require.Equal(t,
		"EXISTS (SELECT 1 FROM eav_data x WHERE x.schema_id = m.ltbase_schema_id AND x.row_id = m.ltbase_row_id AND x.attr_id = $2 AND x.value_text LIKE $3)",
		clause)
	require.Equal(t, []any{int16(7), "%foo%"}, args)
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

// --- #140 characterization: pin main-table leaf conversion semantics before
// switching to the canonical conditionexpr/sqlgen stack. ---

func TestHybrid_DateTimeUnixMsEncoding(t *testing.T) {
	h := newHybridTestHelper(true)
	date := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	cond := &forma.KvCondition{Attr: "created", Value: "equals:" + date.Format(time.RFC3339)}
	clause, args, err := h.build(cond)
	require.NoError(t, err)
	require.Equal(t, "m.\"bigint_01\" = $2", clause)
	require.Equal(t, []any{date.UnixMilli()}, args)
}

func TestHybrid_DateTimeISO8601Encoding(t *testing.T) {
	h := newHybridTestHelper(true)
	cond := &forma.KvCondition{Attr: "created_iso", Value: "equals:2024-01-02T03:04:05Z"}
	clause, args, err := h.build(cond)
	require.NoError(t, err)
	require.Equal(t, "m.\"text_02\" = $2", clause)
	require.Equal(t, []any{"2024-01-02T03:04:05Z"}, args)
}

func TestHybrid_BoolIntEncoding(t *testing.T) {
	h := newHybridTestHelper(true)
	cond := &forma.KvCondition{Attr: "active_int", Value: "equals:1"}
	clause, args, err := h.build(cond)
	require.NoError(t, err)
	require.Equal(t, "m.\"smallint_01\" = $2", clause)
	require.Equal(t, []any{int64(1)}, args)
}

func TestHybrid_BoolTextEncoding(t *testing.T) {
	h := newHybridTestHelper(true)
	cond := &forma.KvCondition{Attr: "active_text", Value: "equals:1"}
	clause, args, err := h.build(cond)
	require.NoError(t, err)
	require.Equal(t, "m.\"text_03\" = $2", clause)
	require.Equal(t, []any{"1"}, args)
}

func TestHybrid_UnsupportedOperatorErrors(t *testing.T) {
	h := newHybridTestHelper(true)
	cond := &forma.KvCondition{Attr: "age", Value: "nope:1"}
	_, _, err := h.build(cond)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported operator")
}

func TestHybrid_RawColumnComposite(t *testing.T) {
	h := newHybridTestHelper(true)
	root := &forma.CompositeCondition{
		Logic: forma.LogicAnd,
		Conditions: []forma.Condition{
			&forma.KvCondition{Attr: "text_01", Value: "hello"},
			&forma.KvCondition{Attr: "bigint_01", Value: "gt:42"},
		},
	}
	clause, args, err := h.build(root)
	require.NoError(t, err)
	require.Equal(t, "(m.\"text_01\" = $2) AND (m.\"bigint_01\" > $3)", clause)
	require.Equal(t, []any{"hello", int64(42)}, args)
}

func TestHybrid_EqAliasNormalization(t *testing.T) {
	h := newHybridTestHelper(true)
	cond := &forma.KvCondition{Attr: "age", Value: "eq:10"}
	clause, args, err := h.build(cond)
	require.NoError(t, err)
	require.Equal(t, "m.\"integer_01\" = $2", clause)
	require.Equal(t, []any{int64(10)}, args)
}

func TestHybrid_DoubleColumnIntegerLiteral(t *testing.T) {
	h := newHybridTestHelper(true)
	cond := &forma.KvCondition{Attr: "score", Value: "equals:25"}
	clause, args, err := h.build(cond)
	require.NoError(t, err)
	require.Equal(t, "m.\"double_01\" = $2", clause)
	require.Equal(t, []any{int64(25)}, args)
}

func TestHybrid_UnknownBoundColumnErrors(t *testing.T) {
	h := newHybridTestHelper(true)
	cond := &forma.KvCondition{Attr: "badcol", Value: "equals:x"}
	_, _, err := h.build(cond)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown main table column")
}
