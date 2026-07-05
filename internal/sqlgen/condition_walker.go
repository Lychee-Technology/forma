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

func WalkCondition(cond forma.Condition, style compositeStyle, guard CompositeGuard, emit LeafEmitter) (string, []any, error) {
	return walkCondition(cond, style, guard, emit)
}

// walkCondition traverses a forma.Condition AST and emits SQL via the leafEmitter.
// Composite traversal (empty handling, parens, joins, guard) is parameterised by style.
func walkCondition(cond forma.Condition, style compositeStyle, guard compositeGuard, emit leafEmitter) (string, []any, error) {
	if cond == nil {
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
	}

	switch c := cond.(type) {
	case *forma.CompositeCondition:
		return walkComposite(c, style, guard, emit)
	case *forma.KvCondition:
		return emit.EmitLeaf(c)
	default:
		return "", nil, fmt.Errorf("unsupported condition type %T", cond)
	}
}

func walkComposite(c *forma.CompositeCondition, style compositeStyle, guard compositeGuard, emit leafEmitter) (string, []any, error) {
	// Empty composite produces identity value BEFORE guard check (empty OR must
	// yield "1=0", not be silently vetoed).
	if len(c.Conditions) == 0 {
		if c.Logic == forma.LogicOr {
			return style.emptyOr, nil, nil
		}
		return style.emptyAnd, nil, nil
	}

	// strictLogic: error on unknown Logic
	joiner := " AND "
	switch c.Logic {
	case forma.LogicAnd:
		joiner = " AND "
	case forma.LogicOr:
		joiner = " OR "
	default:
		if style.strictLogic {
			return "", nil, fmt.Errorf("unknown logic: %s", c.Logic)
		}
	}

	// guard: ask compositeGuard whether to skip this node entirely
	if guard != nil && guard.SkipComposite(c) {
		return "", nil, nil
	}

	var parts []string
	var allArgs []any

	for _, child := range c.Conditions {
		childSQL, childArgs, err := walkCondition(child, style, guard, emit)
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
			if c.Logic == forma.LogicOr {
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
