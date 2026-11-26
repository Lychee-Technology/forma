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

// namedCondition is a condition tree a test feeds to the entry points.
type namedCondition struct {
	name string
	cond forma.Condition
}

// cyclicConditions are the cyclic trees every entry point must refuse. A
// regression on one of them overflows the stack, which kills the whole test
// binary, or walks for ever, so they are only ever walked in the capped,
// time-bounded child of TestCyclicConditionsUnderACappedStack.
func cyclicConditions() []namedCondition {
	return []namedCondition{
		{"a self-referential composite", selfCycle()},
		{"an indirect cycle", indirectCycle()},
		{"a cycle through several children", branchingCycle()},
	}
}

// tooDeepCondition is the finite tree every entry point must refuse: a
// regression on it fails an assertion, so it runs in the test binary itself.
func tooDeepCondition() namedCondition {
	return namedCondition{"an acyclic tree one level too deep", nestedCondition(maxConditionNestingDepth+1, statusLeaf())}
}

const depthRefusalMessage = "condition nests composite conditions more than 1000 levels deep; the condition is cyclic or too deeply nested"

// requireDepthRefusal asserts err is checkConditionDepth's published refusal.
func requireDepthRefusal(t *testing.T, err error) {
	t.Helper()
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	message, published := forma.ResolvePublicMessage(err)
	require.True(t, published, "the refusal publishes its message: %v", err)
	assert.Equal(t, depthRefusalMessage, message)
}

func TestConditionDepthBoundary(t *testing.T) {
	wide := &forma.CompositeCondition{Logic: forma.LogicOr}
	for range 5000 {
		wide.Conditions = append(wide.Conditions, statusLeaf())
	}
	tooDeep := tooDeepCondition()
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
		{tooDeep.name, tooDeep.cond, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkConditionDepth("condition", tc.cond)
			if !tc.refused {
				require.NoError(t, err)
				return
			}
			requireDepthRefusal(t, err)
		})
	}
}

// firstConditionAttr serves the refusals of Create and the batches, which
// run before any depth check, so it bounds its own walk and gives up at the
// first node past the bound. Its cyclic cases are runCyclicDepthWalks'.
func TestFirstConditionAttrStopsAtTheBound(t *testing.T) {
	assert.Equal(t, "status", firstConditionAttr(nestedCondition(1000, statusLeaf())))
	assert.Equal(t, "", firstConditionAttr(nestedCondition(1001, statusLeaf())))
	assert.Equal(t, "", firstConditionAttr(&forma.CompositeCondition{Logic: forma.LogicAnd,
		Conditions: []forma.Condition{nestedCondition(1001, emptyAnd()), statusLeaf()}}), "a node past the bound ends the walk, its siblings included")
}

// runCyclicDepthWalks runs the two bounded walks themselves over cyclic
// trees: checkConditionDepth refuses each, and firstConditionAttr ends on
// each. It runs only in the capped child.
func runCyclicDepthWalks(t *testing.T) {
	for _, tree := range cyclicConditions() {
		t.Run("checkConditionDepth refuses "+tree.name, func(t *testing.T) {
			requireDepthRefusal(t, checkConditionDepth("condition", tree.cond))
		})
	}
	leafFirst := &forma.CompositeCondition{Logic: forma.LogicAnd}
	leafFirst.Conditions = []forma.Condition{statusLeaf(), leafFirst}
	leaflessBranching := &forma.CompositeCondition{Logic: forma.LogicAnd}
	leaflessBranching.Conditions = []forma.Condition{leaflessBranching, leaflessBranching}
	t.Run("firstConditionAttr ends on a cycle", func(t *testing.T) {
		assert.Equal(t, "status", firstConditionAttr(leafFirst))
		assert.Equal(t, "", firstConditionAttr(selfCycle()))
		assert.Equal(t, "", firstConditionAttr(indirectCycle()))
		assert.Equal(t, "status", firstConditionAttr(branchingCycle()))
		assert.Equal(t, "", firstConditionAttr(leaflessBranching), "a leafless cycle through two children ends at the bound")
	})
}

// A regression on a cyclic tree overflows the stack instead of failing an
// assertion, and a fatal stack overflow takes the whole test binary with it;
// a walk that goes on to the next sibling past the bound walks a branching
// cycle for ever. So every cyclic case runs only in a child process whose
// stack is capped at 1 MiB, where an unbounded walk dies in milliseconds,
// and whose own -test.timeout ends a walk that never does: only the child
// goes down, and this test reports it. The probes prove the parent reports
// both kinds of failure and survives them.
const (
	cappedStackEnv   = "FORMA_CONDITION_DEPTH_CAPPED_STACK"
	cappedStackBytes = 1 << 20
	cyclicChildLimit = time.Minute
	probeChildLimit  = 3 * time.Second
)

func TestCyclicConditionsUnderACappedStack(t *testing.T) {
	if mode := os.Getenv(cappedStackEnv); mode != "" {
		debug.SetMaxStack(cappedStackBytes)
		switch mode {
		case "cyclic":
			runCyclicDepthWalks(t)
			for _, tree := range cyclicConditions() {
				runConditionEntryPoints(t, tree)
			}
		case "unbounded-probe":
			t.Logf("walked %d nodes", unboundedConditionWalk(selfCycle()))
		case "hanging-probe":
			t.Logf("walked %d nodes", siblingAfterBoundWalk(branchingCycle(), 0))
		default:
			t.Fatalf("unknown %s mode %q", cappedStackEnv, mode)
		}
		return
	}

	t.Run("every cyclic case passes within the cap", func(t *testing.T) {
		out, err := runCappedStackChild(t, "cyclic", cyclicChildLimit)
		require.NoError(t, err, "the capped child failed:\n%s", out)
		assert.Contains(t, out, "--- PASS: TestCyclicConditionsUnderACappedStack/an_indirect_cycle/Tx/Tx.Upsert")
	})
	t.Run("the parent reports a child that overflows its stack", func(t *testing.T) {
		out, err := runCappedStackChild(t, "unbounded-probe", probeChildLimit)
		require.Error(t, err, "an unbounded walk must not survive the cap:\n%s", out)
		assert.Contains(t, out, "goroutine stack exceeds 1048576-byte limit")
	})
	t.Run("the parent reports a child that never ends", func(t *testing.T) {
		out, err := runCappedStackChild(t, "hanging-probe", probeChildLimit)
		require.Error(t, err, "a walk that never ends must not outlive its limit:\n%s", out)
		assert.Contains(t, out, "panic: test timed out after "+probeChildLimit.String())
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

// siblingAfterBoundWalk bounds its depth but, past the bound, goes on to the
// next sibling instead of ending the walk: on branchingCycle it takes 2^1000
// steps, so it never ends while staying within any stack.
func siblingAfterBoundWalk(cond forma.Condition, depth int) int {
	if depth > maxConditionNestingDepth {
		return 0
	}
	composite, ok := cond.(*forma.CompositeCondition)
	if !ok {
		return 1
	}
	n := 1
	for _, child := range composite.Conditions {
		n += siblingAfterBoundWalk(child, depth+1)
	}
	return n
}

// runCappedStackChild reruns this test binary on TestCyclicConditionsUnderACappedStack
// in mode, bounded by limit: the child's own -test.timeout panics with every
// goroutine's stack once limit passes, and the parent kills a child that is
// still running a while later, so no child outlives its limit by much.
func runCappedStackChild(t *testing.T, mode string, limit time.Duration) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), limit+30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCyclicConditionsUnderACappedStack$",
		"-test.count=1", "-test.v", "-test.timeout="+limit.String())
	cmd.Env = append(os.Environ(), cappedStackEnv+"="+mode)
	out, err := cmd.CombinedOutput()
	require.NoError(t, ctx.Err(), "the capped child outlived its own timeout and was killed:\n%s", out)
	return string(out), err
}
