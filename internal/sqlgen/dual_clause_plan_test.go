package sqlgen

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

func dualPlanTestCache() forma.SchemaAttributeCache {
	return forma.SchemaAttributeCache{
		"name": {AttributeID: 5, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_01")}},
		"age": {AttributeID: 6, ValueType: forma.ValueTypeInteger,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("integer_01")}},
		"tag":   {AttributeID: 7, ValueType: forma.ValueTypeText},
		"score": {AttributeID: 8, ValueType: forma.ValueTypeNumeric},
	}
}

// requirePlanBindEquivalence is the #142 phase-4 contract: Plan+Bind must
// reproduce ToDualClauses exactly — clauses, all three arg slices in order,
// and paramIndex advancement.
func requirePlanBindEquivalence(t *testing.T, cond forma.Condition, cache forma.SchemaAttributeCache) {
	t.Helper()

	direct := 0
	want, wantErr := ToDualClauses(cond, "eav_table", 7, cache, &direct)

	plan, planErr := PlanDualClauses(cond, "eav_table", 7, cache, 0)
	if wantErr != nil {
		require.Error(t, planErr)
		return
	}
	require.NoError(t, planErr)

	bound := 0
	got, err := plan.Bind(cond, cache, &bound)
	require.NoError(t, err)

	require.Equal(t, want.PgClause, got.PgClause)
	require.Equal(t, want.PgMainClause, got.PgMainClause)
	require.Equal(t, want.DuckClause, got.DuckClause)
	require.Equal(t, want.PgArgs, got.PgArgs)
	require.Equal(t, want.PgMainArgs, got.PgMainArgs)
	require.Equal(t, want.DuckArgs, got.DuckArgs)
	require.Equal(t, direct, bound, "paramIndex advancement must match")
}

func TestPlanBindEquivalenceMatrix(t *testing.T) {
	cache := dualPlanTestCache()
	cases := map[string]forma.Condition{
		"single main leaf":   &forma.KvCondition{Attr: "age", Value: "gt:10"},
		"single eav leaf":    &forma.KvCondition{Attr: "tag", Value: "equals:x"},
		"mixed and":          &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{&forma.KvCondition{Attr: "age", Value: "gt:10"}, &forma.KvCondition{Attr: "tag", Value: "equals:x"}}},
		"or all pushable":    &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: []forma.Condition{&forma.KvCondition{Attr: "age", Value: "gt:10"}, &forma.KvCondition{Attr: "name", Value: "starts_with:A"}}},
		"or vetoed":          &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: []forma.Condition{&forma.KvCondition{Attr: "age", Value: "gt:10"}, &forma.KvCondition{Attr: "tag", Value: "equals:x"}}},
		"nested and-or":      &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{&forma.KvCondition{Attr: "name", Value: "equals:bob"}, &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: []forma.Condition{&forma.KvCondition{Attr: "age", Value: "gte:18"}, &forma.KvCondition{Attr: "score", Value: "lt:3.5"}}}}},
		"repeated predicate": &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{&forma.KvCondition{Attr: "age", Value: "gt:10"}, &forma.KvCondition{Attr: "age", Value: "lt:90"}}},
		"empty and":          &forma.CompositeCondition{Logic: forma.LogicAnd},
		"empty or":           &forma.CompositeCondition{Logic: forma.LogicOr},
		"nil condition":      nil,
		"unsupported op":     &forma.KvCondition{Attr: "age", Value: "contains:1"},
	}
	for name, cond := range cases {
		t.Run(name, func(t *testing.T) {
			requirePlanBindEquivalence(t, cond, cache)
		})
	}
}

// TestPlanBindReusesPlanAcrossValues is the point of the cache: same shape,
// different operands — one plan, fresh args.
func TestPlanBindReusesPlanAcrossValues(t *testing.T) {
	cache := dualPlanTestCache()
	shapeA := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
		&forma.KvCondition{Attr: "age", Value: "gt:10"},
		&forma.KvCondition{Attr: "tag", Value: "equals:x"},
	}}
	shapeB := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
		&forma.KvCondition{Attr: "age", Value: "gt:77"},
		&forma.KvCondition{Attr: "tag", Value: "equals:zzz"},
	}}

	plan, err := PlanDualClauses(shapeA, "eav_table", 7, cache, 0)
	require.NoError(t, err)

	idx := 0
	boundB, err := plan.Bind(shapeB, cache, &idx)
	require.NoError(t, err)

	// Clauses come from the plan; args carry request B's values.
	directIdx := 0
	directB, err := ToDualClauses(shapeB, "eav_table", 7, cache, &directIdx)
	require.NoError(t, err)
	require.Equal(t, directB, boundB)
	require.Contains(t, boundB.DuckArgs, "zzz")
}

// TestPlanBindDirtyIDsAndCursorAbsent documents that binding involves only
// condition operands: dirty IDs and keyset cursors are engine-level per
// request inputs and never flow through the dual-clause plan.
func TestPlanBindParamSpan(t *testing.T) {
	cache := dualPlanTestCache()
	cond := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
		&forma.KvCondition{Attr: "age", Value: "gt:10"},  // main: 1 slot; eav: 2 slots
		&forma.KvCondition{Attr: "tag", Value: "equals:x"}, // eav: 2 slots
	}}
	plan, err := PlanDualClauses(cond, "eav_table", 7, cache, 3)
	require.NoError(t, err)

	idx := 3
	_, err = plan.Bind(cond, cache, &idx)
	require.NoError(t, err)
	require.Equal(t, 3+plan.ParamSpan, idx)
}
