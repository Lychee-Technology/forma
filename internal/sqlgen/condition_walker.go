package sqlgen

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
)

// leafEmitter renders a single KvCondition leaf into SQL and args.
// Returning ("", nil, nil) signals "skip this leaf"; the style's allEmpty
// behavior decides what a composite whose every child skipped renders.
type leafEmitter interface {
	EmitLeaf(kv *forma.KvCondition) (sql string, args []any, err error)
}

// typedLeafEmitter renders a typed predicate leaf into SQL and args. The
// skip contract matches leafEmitter: ("", nil, nil) drops the leaf.
type typedLeafEmitter interface {
	EmitTypedLeaf(leaf *PredicateLeaf) (sql string, args []any, err error)
}

// TypedLeafEmitter is the exported alias external packages (the hybrid
// condition builder) implement to consume typed leaves via WalkHybridCondition.
type TypedLeafEmitter = typedLeafEmitter

type allEmptyBehavior int

const (
	allEmptyOmit     allEmptyBehavior = iota // return ""
	allEmptyIdentity                         // return "1=1" / "1=0" per Logic
)

type nilNodeBehavior int

const (
	nilNodeError nilNodeBehavior = iota // return error
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
	duckStyle = compositeStyle{
		emptyAnd:           "1=1",
		emptyOr:            "1=0",
		outerParensOnMulti: false,
		allEmpty:           allEmptyIdentity,
		nilNode:            nilNodeTrue,
	}
	hybridStyle = compositeStyle{
		emptyAnd: "1=1",
		emptyOr:  "1=0",
		// Safe only because every consumer parenthesizes the injected clause
		// at the template seam (optimizedQuerySQLTemplate anchor sites per
		// #269; the DuckDB template has always wrapped its injections).
		outerParensOnMulti: false,
		allEmpty:           allEmptyOmit,
		nilNode:            nilNodeError,
	}
)

// legacyLeafAdapter feeds the original KvCondition of a typed leaf to a
// legacy leafEmitter, so untyped consumers (hybrid builder) share the typed
// traversal instead of a second walker.
type legacyLeafAdapter struct{ emit leafEmitter }

func (a legacyLeafAdapter) EmitTypedLeaf(leaf *PredicateLeaf) (string, []any, error) {
	return a.emit.EmitLeaf(leaf.Source)
}

// WalkHybridCondition normalizes cond with the hybrid leaf target (parse-once)
// and walks it with the hybrid composite style, emitting each leaf through the
// caller's typed emitter. It is the entrypoint for the hybrid condition
// builder: composite traversal and leaf parsing both live in the shared typed
// walker, so the builder only formats already-resolved leaves.
func WalkHybridCondition(cond forma.Condition, cache forma.SchemaAttributeCache, emit TypedLeafEmitter) (string, []any, error) {
	return walkPredicate(normalizePredicates(cond, cache, targetHybrid), hybridStyle, emit)
}

// walkCondition traverses a forma.Condition AST and emits SQL via the legacy
// leafEmitter. The traversal itself is the typed predicate walker over a
// structure-only normalization (no target payloads), so composite semantics
// exist exactly once.
func walkCondition(cond forma.Condition, style compositeStyle, emit leafEmitter) (string, []any, error) {
	return walkPredicate(normalizePredicates(cond, nil, targetsNone), style, legacyLeafAdapter{emit: emit})
}

// walkPredicate traverses a typed predicate tree and emits SQL via the
// typedLeafEmitter. Composite traversal (empty handling, parens, joins) is
// parameterised by style.
func walkPredicate(node PredicateNode, style compositeStyle, emit typedLeafEmitter) (string, []any, error) {
	switch n := node.(type) {
	case predicateNilNode:
		switch style.nilNode {
		case nilNodeError:
			return "", nil, fmt.Errorf("nil condition not allowed")
		case nilNodeTrue:
			return "1=1", nil, nil
		default:
			return "", nil, fmt.Errorf("unknown nilNode behavior")
		}
	case *PredicateGroup:
		return walkPredicateGroup(n, style, emit)
	case *PredicateLeaf:
		return emit.EmitTypedLeaf(n)
	case predicateInvalidNode:
		return "", nil, n.err
	default:
		return "", nil, fmt.Errorf("unsupported predicate node %T", node)
	}
}

func walkPredicateGroup(g *PredicateGroup, style compositeStyle, emit typedLeafEmitter) (string, []any, error) {
	// An empty composite renders its identity value.
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

	var parts []string
	var allArgs []any

	for _, child := range g.Children {
		childSQL, childArgs, err := walkPredicate(child, style, emit)
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
