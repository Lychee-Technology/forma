package sqlgen

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// markerEmitter spells a leaf tested on the anchor row as R(attr) and one
// tested on the entity as E(attr), so a test reads which leaves moved onto t.
type markerEmitter struct{}

func (markerEmitter) EmitTypedLeaf(leaf *PredicateLeaf) (string, []any, error) {
	return "E(" + leaf.Attr + ")", nil, nil
}

func (markerEmitter) EmitRowLeaf(leaf *PredicateLeaf) (string, []any, error) {
	return "R(" + leaf.Attr + ")", nil, nil
}

var anchorWalkCache = forma.SchemaAttributeCache{
	"a": {AttributeID: 1, ValueType: forma.ValueTypeText},
	"b": {AttributeID: 2, ValueType: forma.ValueTypeText},
	"c": {AttributeID: 3, ValueType: forma.ValueTypeUUID},
	"m": {AttributeID: 4, ValueType: forma.ValueTypeText,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_01")}},
}

func kv(attr, value string) *forma.KvCondition { return &forma.KvCondition{Attr: attr, Value: value} }

func and(children ...forma.Condition) *forma.CompositeCondition {
	return &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: children}
}

func or(children ...forma.Condition) *forma.CompositeCondition {
	return &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: children}
}

// TestWalkHybridEAVAnchorMovesOnlyEquivalentLeaves pins which leaves test
// the anchor row t: the clause must hold for some t of an entity exactly
// when the condition holds for the entity (#881).
func TestWalkHybridEAVAnchorMovesOnlyEquivalentLeaves(t *testing.T) {
	cases := []struct {
		name string
		cond forma.Condition
		want string
	}{
		{"a single leaf", kv("a", "gt:x"), "R(a)"},
		{"a uuid range", kv("c", "lt:00000000-0000-4000-8000-000000000005"), "R(c)"},
		{"a set leaf", kv("a", `not_in:["x"]`), "R(a)"},
		{"is_not_null holds on the row that holds the value", kv("a", "is_not_null:true"), "R(a)"},
		{"is_null holds without any row", kv("a", "is_null:true"), "E(a)"},
		{"a main leaf tests entity_main", kv("m", "equals:x"), "E(m)"},
		{"and moves its first qualifying child only", and(kv("a", "gt:x"), kv("b", "lt:y")), "(R(a)) AND (E(b))"},
		{"and skips a child that does not qualify", and(kv("m", "equals:x"), kv("a", "is_null:true"), kv("b", "lt:y")),
			"(E(m)) AND (E(a)) AND (R(b))"},
		{"and with no qualifying child is unchanged", and(kv("m", "equals:x"), kv("a", "is_null:true")), "(E(m)) AND (E(a))"},
		{"or moves every child", or(kv("a", "gt:x"), kv("b", "lt:y")), "(R(a)) OR (R(b))"},
		{"or with one child that does not qualify is unchanged", or(kv("a", "gt:x"), kv("b", "is_null:true")),
			"(E(a)) OR (E(b))"},
		{"or of ands moves one leaf of each", or(and(kv("a", "gt:x"), kv("b", "lt:y")), kv("c", "equals:00000000-0000-4000-8000-000000000001")),
			"((R(a)) AND (E(b))) OR (R(c))"},
		{"an and child that is an or qualifies whole", and(kv("m", "equals:x"), or(kv("a", "gt:x"), kv("b", "lt:y"))),
			"(E(m)) AND ((R(a)) OR (R(b)))"},
		{"a failed or leaves no partial choice behind", and(or(kv("a", "gt:x"), kv("b", "is_null:true")), kv("c", "equals:00000000-0000-4000-8000-000000000001")),
			"((E(a)) OR (E(b))) AND (R(c))"},
		{"an empty or renders 1=0 and stays", or(), "1=0"},
		{"an empty or child disqualifies its or", or(kv("a", "gt:x"), or()), "(E(a)) OR (1=0)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _, err := WalkHybridEAVAnchor(tc.cond, anchorWalkCache, markerEmitter{})
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestRenderOnRowIsTheBodyOfRender pins that the anchor-row spelling is the body
// of Render's EXISTS with x read as the given alias, placeholders in Args
// order, so moving a leaf onto t changes nothing but the row it tests.
func TestRenderOnRowIsTheBodyOfRender(t *testing.T) {
	for _, cond := range []*forma.KvCondition{
		kv("a", "gt:x"), kv("c", "gte:00000000-0000-4000-8000-000000000005"), kv("a", `in:["x","y"]`),
		kv("a", `not_in:["x"]`), kv("a", "is_not_null:true"), kv("a", "starts_with:x"),
	} {
		p := normalizeLeaf(cond, anchorWalkCache, targetHybrid).Hybrid.Eav
		require.NoError(t, p.Err, cond.Value)
		require.True(t, p.MatchesOnRow(), cond.Value)
		n := 1
		next := func() string { n++; return "$" + string(rune('0'+n)) }
		exists := p.Render("eav_data", "t.schema_id", "t.row_id", next)
		n = 1
		onRow := p.RenderOnRow("x", next)
		require.Equal(t, "EXISTS (SELECT 1 FROM eav_data x WHERE x.schema_id = t.schema_id AND x.row_id = t.row_id AND "+onRow+")",
			exists, cond.Value)
	}
	isNull := normalizeLeaf(kv("a", "is_null:true"), anchorWalkCache, targetHybrid).Hybrid.Eav
	require.False(t, isNull.MatchesOnRow())
}
