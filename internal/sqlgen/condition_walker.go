package sqlgen

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
)

// leafEmitter renders a single KvCondition leaf into SQL and args.
// Returning ("", nil, nil) signals "skip this leaf" — used by pg-main to
// silently skip leaves without column bindings.
type leafEmitter interface {
	EmitLeaf(kv *forma.KvCondition) (sql string, args []any, err error)
}

type LeafEmitter = leafEmitter

// compositeGuard optionally intercepts a CompositeCondition before traversing
// its children. pg-main uses this to veto an entire OR branch when any child
// cannot be pushed to the main table.
type compositeGuard interface {
	SkipComposite(c *forma.CompositeCondition) bool
}

type CompositeGuard = compositeGuard

// typedLeafEmitter renders a typed predicate leaf into SQL and args. The
// skip contract matches leafEmitter: ("", nil, nil) drops the leaf.
type typedLeafEmitter interface {
	EmitTypedLeaf(leaf *PredicateLeaf) (sql string, args []any, err error)
}

// typedGuard optionally intercepts a PredicateGroup before traversing its
// children, mirroring compositeGuard on the typed tree.
type typedGuard interface {
	SkipGroup(g *PredicateGroup) bool
}

type allEmptyBehavior int

const (
	allEmptyOmit     allEmptyBehavior = iota // return ""
	allEmptyIdentity                         // return "1=1" / "1=0" per Logic
)

type nilNodeBehavior int

const (
	nilNodeError nilNodeBehavior = iota // return error
	nilNodeSkip                         // return ""
	nilNodeTrue                         // return "1=1"
)

// compositeStyle parameterises the composite traversal semantics.
type compositeStyle struct {
	emptyAnd, emptyOr  string           // e.g. "1=1" / "1=0"
	outerParensOnMulti bool             // wrap whole join in (...) when len(parts) > 1
	allEmpty           allEmptyBehavior // what to return when all children produce ""
	nilNode            nilNodeBehavior  // what to do when cond is nil
	strictLogic        bool             // error on unknown Logic
}

var (
	pgEavStyle = compositeStyle{
		emptyAnd:           "1=1",
		emptyOr:            "1=0",
		outerParensOnMulti: true,
		allEmpty:           allEmptyOmit,
		nilNode:            nilNodeError,
		strictLogic:        true,
	}
	pgMainStyle = compositeStyle{
		emptyAnd:           "1=1",
		emptyOr:            "1=0",
		outerParensOnMulti: true,
		allEmpty:           allEmptyOmit,
		nilNode:            nilNodeSkip,
	}
	duckStyle = compositeStyle{
		emptyAnd:           "1=1",
		emptyOr:            "1=0",
		outerParensOnMulti: false,
		allEmpty:           allEmptyIdentity,
		nilNode:            nilNodeTrue,
	}
	hybridStyle = compositeStyle{
		emptyAnd:           "1=1",
		emptyOr:            "1=0",
		outerParensOnMulti: false,
		allEmpty:           allEmptyOmit,
		nilNode:            nilNodeError,
	}
)

var HybridStyle = hybridStyle

// legacyLeafAdapter feeds the original KvCondition of a typed leaf to a
// legacy leafEmitter, so untyped consumers (hybrid builder) share the typed
// traversal instead of a second walker.
type legacyLeafAdapter struct{ emit leafEmitter }

func (a legacyLeafAdapter) EmitTypedLeaf(leaf *PredicateLeaf) (string, []any, error) {
	return a.emit.EmitLeaf(leaf.Source)
}

// legacyGuardAdapter feeds the source composite of a typed group to a legacy
// compositeGuard.
type legacyGuardAdapter struct{ guard compositeGuard }

func (a legacyGuardAdapter) SkipGroup(g *PredicateGroup) bool {
	return a.guard.SkipComposite(g.Source)
}

func WalkCondition(cond forma.Condition, style compositeStyle, guard CompositeGuard, emit LeafEmitter) (string, []any, error) {
	return walkCondition(cond, style, guard, emit)
}

// walkCondition traverses a forma.Condition AST and emits SQL via the legacy
// leafEmitter. The traversal itself is the typed predicate walker over a
// structure-only normalization (no target payloads), so composite semantics
// exist exactly once.
func walkCondition(cond forma.Condition, style compositeStyle, guard compositeGuard, emit leafEmitter) (string, []any, error) {
	var g typedGuard
	if guard != nil {
		g = legacyGuardAdapter{guard: guard}
	}
	return walkPredicate(normalizePredicates(cond, nil, targetsNone), style, g, legacyLeafAdapter{emit: emit})
}

// walkPredicate traverses a typed predicate tree and emits SQL via the
// typedLeafEmitter. Composite traversal (empty handling, parens, joins,
// guard) is parameterised by style.
func walkPredicate(node PredicateNode, style compositeStyle, guard typedGuard, emit typedLeafEmitter) (string, []any, error) {
	switch n := node.(type) {
	case predicateNilNode:
		switch style.nilNode {
		case nilNodeError:
			return "", nil, fmt.Errorf("nil condition not allowed")
		case nilNodeSkip:
			return "", nil, nil
		case nilNodeTrue:
			return "1=1", nil, nil
		default:
			return "", nil, fmt.Errorf("unknown nilNode behavior")
		}
	case *PredicateGroup:
		return walkPredicateGroup(n, style, guard, emit)
	case *PredicateLeaf:
		return emit.EmitTypedLeaf(n)
	case predicateInvalidNode:
		return "", nil, n.err
	default:
		return "", nil, fmt.Errorf("unsupported predicate node %T", node)
	}
}

func walkPredicateGroup(g *PredicateGroup, style compositeStyle, guard typedGuard, emit typedLeafEmitter) (string, []any, error) {
	// Empty composite produces identity value BEFORE guard check (empty OR must
	// yield "1=0", not be silently vetoed).
	if len(g.Children) == 0 {
		if g.Logic == forma.LogicOr {
			return style.emptyOr, nil, nil
		}
		return style.emptyAnd, nil, nil
	}

	// strictLogic: error on unknown Logic
	joiner := " AND "
	switch g.Logic {
	case forma.LogicAnd:
		joiner = " AND "
	case forma.LogicOr:
		joiner = " OR "
	default:
		if style.strictLogic {
			return "", nil, fmt.Errorf("unknown logic: %s", g.Logic)
		}
	}

	// guard: ask typedGuard whether to skip this node entirely
	if guard != nil && guard.SkipGroup(g) {
		return "", nil, nil
	}

	var parts []string
	var allArgs []any

	for _, child := range g.Children {
		childSQL, childArgs, err := walkPredicate(child, style, guard, emit)
		if err != nil {
			return "", nil, err
		}
		if childSQL == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("(%s)", childSQL))
		allArgs = append(allArgs, childArgs...)
	}

	// All children produced empty SQL
	if len(parts) == 0 {
		switch style.allEmpty {
		case allEmptyOmit:
			return "", nil, nil
		case allEmptyIdentity:
			if g.Logic == forma.LogicOr {
				return "1=0", nil, nil
			}
			return "1=1", nil, nil
		default:
			return "", nil, fmt.Errorf("unknown allEmpty behavior")
		}
	}

	joined := strings.Join(parts, joiner)

	if style.outerParensOnMulti && len(parts) > 1 {
		return "(" + joined + ")", allArgs, nil
	}
	return joined, allArgs, nil
}
