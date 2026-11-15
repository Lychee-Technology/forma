package sqlgen

import (
	"fmt"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// echoLeafEmitter returns the attribute name as SQL and a single string arg.
type echoLeafEmitter struct{}

func (e echoLeafEmitter) EmitLeaf(kv *forma.KvCondition) (string, []any, error) {
	return kv.Attr, []any{kv.Value}, nil
}

// skipLeafEmitter always returns empty SQL, exercising the leaf-skip contract.
type skipLeafEmitter struct{}

func (e skipLeafEmitter) EmitLeaf(kv *forma.KvCondition) (string, []any, error) {
	return "", nil, nil
}

// --- Nil node behaviour ---

func TestWalkCondition_NilNodeError(t *testing.T) {
	_, _, err := walkCondition(nil, pgEavStyle, echoLeafEmitter{})
	require.Error(t, err)
}

func TestWalkCondition_NilNodeTrue(t *testing.T) {
	sql, args, err := walkCondition(nil, duckStyle, echoLeafEmitter{})
	require.NoError(t, err)
	require.Equal(t, "1=1", sql)
	require.Nil(t, args)
}

// --- Empty composite ---

func TestWalkCondition_EmptyAnd(t *testing.T) {
	emptyAnd := &forma.CompositeCondition{Logic: forma.LogicAnd}
	sql, args, err := walkCondition(emptyAnd, pgEavStyle, echoLeafEmitter{})
	require.NoError(t, err)
	require.Equal(t, "1=1", sql)
	require.Nil(t, args)
}

func TestWalkCondition_EmptyOr(t *testing.T) {
	emptyOr := &forma.CompositeCondition{Logic: forma.LogicOr}
	sql, args, err := walkCondition(emptyOr, pgEavStyle, echoLeafEmitter{})
	require.NoError(t, err)
	require.Equal(t, "1=0", sql)
	require.Nil(t, args)

	// DuckDB dual uses same emptyOr semantics
	sql, args, err = walkCondition(emptyOr, duckStyle, echoLeafEmitter{})
	require.NoError(t, err)
	require.Equal(t, "1=0", sql)
	require.Nil(t, args)
}

// --- All-empty behaviour ---

func TestWalkCondition_AllEmptyOmit(t *testing.T) {
	leaf := &forma.KvCondition{Attr: "a", Value: "1"}
	and := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{leaf}}
	// skip emitter makes all children produce ""
	sql, args, err := walkCondition(and, pgEavStyle, skipLeafEmitter{})
	require.NoError(t, err)
	require.Equal(t, "", sql)
	require.Nil(t, args)
}

func TestWalkCondition_AllEmptyIdentity_And(t *testing.T) {
	leaf := &forma.KvCondition{Attr: "a", Value: "1"}
	and := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{leaf}}
	sql, args, err := walkCondition(and, duckStyle, skipLeafEmitter{})
	require.NoError(t, err)
	require.Equal(t, "1=1", sql)
	require.Nil(t, args)
}

func TestWalkCondition_AllEmptyIdentity_Or(t *testing.T) {
	leaf := &forma.KvCondition{Attr: "a", Value: "1"}
	or := &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: []forma.Condition{leaf}}
	sql, args, err := walkCondition(or, duckStyle, skipLeafEmitter{})
	require.NoError(t, err)
	require.Equal(t, "1=0", sql)
	require.Nil(t, args)
}

// --- Single-child outer parens ---

func TestWalkCondition_SingleChild_NoOuterParens(t *testing.T) {
	leaf := &forma.KvCondition{Attr: "a", Value: "1"}
	and := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{leaf}}

	for _, style := range []compositeStyle{pgEavStyle, duckStyle, hybridStyle} {
		t.Run(fmt.Sprintf("style=%v", style.outerParensOnMulti), func(t *testing.T) {
			sql, args, err := walkCondition(and, style, echoLeafEmitter{})
			require.NoError(t, err)
			require.Equal(t, "(a)", sql, "single child should be wrapped but without outer parens")
			require.Equal(t, []any{"1"}, args)
		})
	}
}

// --- Multi-child outer parens: PG wraps, Duck/hybrid do not ---

func TestWalkCondition_MultiChild_PGWraps(t *testing.T) {
	a := &forma.KvCondition{Attr: "a", Value: "1"}
	b := &forma.KvCondition{Attr: "b", Value: "2"}
	and := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{a, b}}

	sql, args, err := walkCondition(and, pgEavStyle, echoLeafEmitter{})
	require.NoError(t, err)
	require.Equal(t, "((a) AND (b))", sql)
	require.Equal(t, []any{"1", "2"}, args)

}

func TestWalkCondition_MultiChild_DuckNoWrap(t *testing.T) {
	a := &forma.KvCondition{Attr: "a", Value: "1"}
	b := &forma.KvCondition{Attr: "b", Value: "2"}
	and := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{a, b}}

	sql, args, err := walkCondition(and, duckStyle, echoLeafEmitter{})
	require.NoError(t, err)
	require.Equal(t, "(a) AND (b)", sql)
	require.Equal(t, []any{"1", "2"}, args)
}

func TestWalkCondition_MultiChild_HybridNoWrap(t *testing.T) {
	a := &forma.KvCondition{Attr: "a", Value: "1"}
	b := &forma.KvCondition{Attr: "b", Value: "2"}
	and := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{a, b}}

	sql, args, err := walkCondition(and, hybridStyle, echoLeafEmitter{})
	require.NoError(t, err)
	require.Equal(t, "(a) AND (b)", sql)
	require.Equal(t, []any{"1", "2"}, args)
}

// --- Leaf skip ---

func TestWalkCondition_LeafSkip_SkipEmitter(t *testing.T) {
	leaf := &forma.KvCondition{Attr: "a", Value: "1"}
	and := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{leaf}}
	sql, args, err := walkCondition(and, pgEavStyle, skipLeafEmitter{})
	require.NoError(t, err)
	// all children skipped → allEmpty = omit → ""
	require.Equal(t, "", sql)
	require.Nil(t, args)
}

func TestWalkCondition_LeafSkip_MixedSkipAndReal(t *testing.T) {
	a := &forma.KvCondition{Attr: "a", Value: "1"}
	b := &forma.KvCondition{Attr: "b", Value: "2"}

	// Custom emitter that only passes through "a"
	selectiveEmitter := &selectiveEmitter{allowAttr: "a"}
	and := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{a, b}}
	sql, args, err := walkCondition(and, pgEavStyle, selectiveEmitter)
	require.NoError(t, err)
	require.Equal(t, "(a)", sql)
	require.Equal(t, []any{"1"}, args)
}

type selectiveEmitter struct {
	allowAttr string
}

func (e *selectiveEmitter) EmitLeaf(kv *forma.KvCondition) (string, []any, error) {
	if kv.Attr == e.allowAttr {
		return kv.Attr, []any{kv.Value}, nil
	}
	return "", nil, nil
}

// --- Unknown Logic ---

func TestWalkCondition_UnknownLogic_Strict(t *testing.T) {
	leaf := &forma.KvCondition{Attr: "a", Value: "1"}
	bad := &forma.CompositeCondition{Logic: "xor", Conditions: []forma.Condition{leaf}}
	_, _, err := walkCondition(bad, pgEavStyle, echoLeafEmitter{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown logic")
}

func TestWalkCondition_UnknownLogic_NonStrict(t *testing.T) {
	leaf := &forma.KvCondition{Attr: "a", Value: "1"}
	bad := &forma.CompositeCondition{Logic: "xor", Conditions: []forma.Condition{leaf}}
	sql, args, err := walkCondition(bad, duckStyle, echoLeafEmitter{})
	require.NoError(t, err)
	// Non-strict styles don't error on unknown logic; they just use default joiner " AND "
	require.Equal(t, "(a)", sql)
	require.Equal(t, []any{"1"}, args)
}

// --- Args order and nesting ---

func TestWalkCondition_ArgsOrder_Nested(t *testing.T) {
	a := &forma.KvCondition{Attr: "a", Value: "1"}
	b := &forma.KvCondition{Attr: "b", Value: "2"}
	c := &forma.KvCondition{Attr: "c", Value: "3"}

	inner := &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: []forma.Condition{b, c}}
	outer := &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{a, inner}}

	sql, args, err := walkCondition(outer, pgEavStyle, echoLeafEmitter{})
	require.NoError(t, err)
	require.Contains(t, sql, "(a)")
	require.Contains(t, sql, "(b)")
	require.Contains(t, sql, "(c)")
	require.Contains(t, sql, "AND")
	require.Contains(t, sql, "OR")
	// Args in traversal order: a, b, c
	require.Equal(t, []any{"1", "2", "3"}, args)
}

// --- Single root KvCondition (leaf) ---

func TestWalkCondition_SingleLeaf(t *testing.T) {
	leaf := &forma.KvCondition{Attr: "x", Value: "val"}
	sql, args, err := walkCondition(leaf, pgEavStyle, echoLeafEmitter{})
	require.NoError(t, err)
	require.Equal(t, "x", sql)
	require.Equal(t, []any{"val"}, args)
}

// --- Hybrid style: emptyOr produces "1=0" ---

func TestWalkCondition_HybridStyle_EmptyOr(t *testing.T) {
	emptyOr := &forma.CompositeCondition{Logic: forma.LogicOr}
	sql, args, err := walkCondition(emptyOr, hybridStyle, echoLeafEmitter{})
	require.NoError(t, err)
	require.Equal(t, "1=0", sql)
	require.Nil(t, args)
}
