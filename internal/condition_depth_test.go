package internal

import (
	"context"
	"os"
	"os/exec"
	"runtime/debug"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The condition nesting bound (#675). Depth counts composite edges from the
// root: the root is at depth 0, and a tree is accepted when every node is at
// depth 1000 or less.

// nestedCondition wraps innermost in depth single-child and composites, so
// innermost sits at depth depth.
func nestedCondition(depth int, innermost forma.Condition) forma.Condition {
	cond := innermost
	for range depth {
		cond = &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{cond}}
	}
	return cond
}

func statusLeaf() forma.Condition {
	return &forma.KvCondition{Attr: "status", Value: "equals:scheduled"}
}

func emptyAnd() forma.Condition {
	return &forma.CompositeCondition{Logic: forma.LogicAnd}
}

// selfCycle is an and whose only child is itself.
func selfCycle() forma.Condition {
	c := &forma.CompositeCondition{Logic: forma.LogicAnd}
	c.Conditions = []forma.Condition{c}
	return c
}

// indirectCycle is A -> B -> A.
func indirectCycle() forma.Condition {
	a := &forma.CompositeCondition{Logic: forma.LogicAnd}
	b := &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: []forma.Condition{a}}
	a.Conditions = []forma.Condition{b}
	return a
}

// branchingCycle holds a leaf and itself twice: a walk that went on to the
// next sibling after meeting the bound would take 2^1000 steps.
func branchingCycle() forma.Condition {
	c := &forma.CompositeCondition{Logic: forma.LogicOr}
	c.Conditions = []forma.Condition{statusLeaf(), c, c}
	return c
}

// refusedConditions are the trees every entry point must refuse.
func refusedConditions() []struct {
	name string
	cond forma.Condition
} {
	return []struct {
		name string
		cond forma.Condition
	}{
		{"a self-referential composite", selfCycle()},
		{"an indirect cycle", indirectCycle()},
		{"a cycle through several children", branchingCycle()},
		{"an acyclic tree one level too deep", nestedCondition(maxConditionNestingDepth+1, statusLeaf())},
	}
}

func TestConditionDepthBoundary(t *testing.T) {
	wide := &forma.CompositeCondition{Logic: forma.LogicOr}
	for range 5000 {
		wide.Conditions = append(wide.Conditions, statusLeaf())
	}
	cases := []struct {
		name    string
		cond    forma.Condition
		refused bool
	}{
		{"no condition", nil, false},
		{"a lone leaf", statusLeaf(), false},
		{"a nil composite", (*forma.CompositeCondition)(nil), false},
		{"a wide tree", wide, false},
		{"the deepest accepted tree: a leaf at depth 1000", nestedCondition(1000, statusLeaf()), false},
		{"the shallowest refused tree: a leaf at depth 1001", nestedCondition(1001, statusLeaf()), true},
		{"an empty and at depth 1000", nestedCondition(1000, emptyAnd()), false},
		{"an empty and at depth 1001", nestedCondition(1001, emptyAnd()), true},
		{"a deep branch behind a shallow sibling", &forma.CompositeCondition{Logic: forma.LogicAnd,
			Conditions: []forma.Condition{statusLeaf(), nestedCondition(1000, statusLeaf())}}, true},
	}
	for _, tc := range refusedConditions() {
		cases = append(cases, struct {
			name    string
			cond    forma.Condition
			refused bool
		}{tc.name, tc.cond, true})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkConditionDepth("condition", tc.cond)
			if !tc.refused {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			message, published := forma.ResolvePublicMessage(err)
			require.True(t, published, "the refusal publishes its message: %v", err)
			assert.Equal(t, "condition nests composite conditions more than 1000 levels deep; the condition is cyclic or too deeply nested", message)
		})
	}
}

// firstConditionAttr serves the refusals of Create and the batches, which
// run before any depth check, so it bounds its own walk and gives up at the
// first node past the bound.
func TestFirstConditionAttrStopsAtTheBound(t *testing.T) {
	leafFirst := &forma.CompositeCondition{Logic: forma.LogicAnd}
	leafFirst.Conditions = []forma.Condition{statusLeaf(), leafFirst}
	leaflessBranching := &forma.CompositeCondition{Logic: forma.LogicAnd}
	leaflessBranching.Conditions = []forma.Condition{leaflessBranching, leaflessBranching}

	assert.Equal(t, "status", firstConditionAttr(nestedCondition(1000, statusLeaf())))
	assert.Equal(t, "", firstConditionAttr(nestedCondition(1001, statusLeaf())))
	assert.Equal(t, "status", firstConditionAttr(leafFirst))
	assert.Equal(t, "", firstConditionAttr(selfCycle()))
	assert.Equal(t, "", firstConditionAttr(indirectCycle()))
	assert.Equal(t, "", firstConditionAttr(leaflessBranching), "a leafless cycle through two children ends at the bound")
}

// A regression here overflows the stack instead of failing an assertion,
// and a fatal stack overflow takes the whole test binary with it. So the
// entry points are run again in a child process whose stack is capped at
// 1 MiB, where an unbounded walk over a cyclic tree dies in milliseconds and
// only the child goes down. The probe proves the cap catches one.
const (
	cappedStackEnv   = "FORMA_CONDITION_DEPTH_CAPPED_STACK"
	cappedStackBytes = 1 << 20
)

func TestCyclicConditionsUnderACappedStack(t *testing.T) {
	switch os.Getenv(cappedStackEnv) {
	case "entry-points":
		debug.SetMaxStack(cappedStackBytes)
		runConditionEntryPoints(t)
		return
	case "unbounded-probe":
		debug.SetMaxStack(cappedStackBytes)
		t.Logf("walked %d nodes", unboundedConditionWalk(selfCycle()))
		return
	}

	t.Run("every entry point refuses within the cap", func(t *testing.T) {
		out, err := runCappedStackChild(t, "entry-points")
		require.NoError(t, err, "the capped child failed:\n%s", out)
	})
	t.Run("the cap catches an unbounded walk", func(t *testing.T) {
		out, err := runCappedStackChild(t, "unbounded-probe")
		require.Error(t, err, "an unbounded walk must not survive the cap:\n%s", out)
		assert.Contains(t, out, "goroutine stack exceeds 1048576-byte limit")
	})
}

// unboundedConditionWalk is the walk #675 removed: it counts nodes with no
// bound, so it never ends on a cycle.
func unboundedConditionWalk(cond forma.Condition) int {
	composite, ok := cond.(*forma.CompositeCondition)
	if !ok {
		return 1
	}
	n := 1
	for _, child := range composite.Conditions {
		n += unboundedConditionWalk(child)
	}
	return n
}

func runCappedStackChild(t *testing.T, mode string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCyclicConditionsUnderACappedStack$", "-test.count=1")
	cmd.Env = append(os.Environ(), cappedStackEnv+"="+mode)
	out, err := cmd.CombinedOutput()
	require.NoError(t, ctx.Err(), "the capped child did not finish:\n%s", out)
	return string(out), err
}
