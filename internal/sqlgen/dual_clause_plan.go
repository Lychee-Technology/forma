package sqlgen

import (
	"fmt"

	"github.com/lychee-technology/forma"
)

// DualClausePlan is the shape-derived half of ToDualClauses (#142 phase 4):
// the three clause skeletons plus the $N span they consume. Values are bound
// per request through Bind, which re-runs the same walker decision paths with
// value-only emitters — argument order therefore matches ToDualClauses by
// construction, not by parallel bookkeeping.
type DualClausePlan struct {
	PgClause     string
	PgMainClause string
	DuckClause   string
	// ParamSpan is how many $N placeholders the PG fragments consume,
	// starting from the paramIndex the plan was built with.
	ParamSpan int
}

// PlanDualClauses compiles the shape-dependent clause skeletons for condition.
// startParamIndex must equal the paramIndex value Bind will later be called
// with (placeholder numbering is part of the skeleton text).
func PlanDualClauses(condition forma.Condition, eavTable string, schemaID int16, cache forma.SchemaAttributeCache, startParamIndex int) (*DualClausePlan, error) {
	idx := startParamIndex
	dc, err := ToDualClauses(condition, eavTable, schemaID, cache, &idx)
	if err != nil {
		return nil, err
	}
	return &DualClausePlan{
		PgClause:     dc.PgClause,
		PgMainClause: dc.PgMainClause,
		DuckClause:   dc.DuckClause,
		ParamSpan:    idx - startParamIndex,
	}, nil
}

// Bind produces the per-request DualClauses for a condition tree with the
// same shape the plan was compiled from: cached skeletons plus freshly bound
// args. paramIndex is advanced by the plan's span, mirroring ToDualClauses.
// The condition is normalized into the typed predicate tree once; all three
// arg slices bind from that shared tree.
func (p *DualClausePlan) Bind(condition forma.Condition, cache forma.SchemaAttributeCache, paramIndex *int) (DualClauses, error) {
	if p == nil {
		return DualClauses{}, fmt.Errorf("dual clause plan is nil")
	}
	tree := normalizePredicates(condition, cache, targetsAll)
	pgMainArgs, err := bindArgsFromTree(tree, pgMainStyle, pgMainTypedGuard{}, bindPgMainValues)
	if err != nil {
		return DualClauses{}, fmt.Errorf("pg main binding: %w", err)
	}
	pgArgs, err := bindArgsFromTree(tree, pgEavStyle, nil, bindPgEavValues)
	if err != nil {
		return DualClauses{}, fmt.Errorf("pg eav binding: %w", err)
	}
	duckArgs, err := bindArgsFromTree(tree, duckStyle, nil, bindDuckValues)
	if err != nil {
		return DualClauses{}, fmt.Errorf("duck binding: %w", err)
	}
	if paramIndex != nil {
		*paramIndex += p.ParamSpan
	}
	return DualClauses{
		PgClause:     p.PgClause,
		PgArgs:       pgArgs,
		PgMainClause: p.PgMainClause,
		PgMainArgs:   pgMainArgs,
		DuckClause:   p.DuckClause,
		DuckArgs:     duckArgs,
	}, nil
}

// typedBindEmitter adapts a typed-leaf value function to the walker's
// emitter contract: the dummy non-empty SQL keeps the walker's skip/veto
// accounting identical to the string-producing emitters, so args accumulate
// in the same order.
type typedBindEmitter struct {
	values func(leaf *PredicateLeaf) ([]any, bool, error)
}

func (b *typedBindEmitter) EmitTypedLeaf(leaf *PredicateLeaf) (string, []any, error) {
	args, emit, err := b.values(leaf)
	if err != nil || !emit {
		return "", nil, err
	}
	return "x", args, nil
}

// bindArgsFromTree re-walks an already-normalized tree with a value-only
// emitter. A top-level nil condition binds no args (matching the generation
// entrypoints, which skip nil before walking).
func bindArgsFromTree(tree PredicateNode, style compositeStyle, guard typedGuard, values func(leaf *PredicateLeaf) ([]any, bool, error)) ([]any, error) {
	if _, ok := tree.(predicateNilNode); ok {
		return nil, nil
	}
	_, args, err := walkPredicate(tree, style, guard, &typedBindEmitter{values: values})
	return args, err
}

func bindPgMainValues(leaf *PredicateLeaf) ([]any, bool, error) {
	p := leaf.PgMain
	if p.Err != nil || p.Skip {
		return nil, false, p.Err
	}
	return []any{p.Value}, true, nil
}

func bindPgEavValues(leaf *PredicateLeaf) ([]any, bool, error) {
	p := leaf.PgEav
	if p.Err != nil {
		return nil, false, p.Err
	}
	return []any{p.AttrID, p.Value}, true, nil
}

func bindDuckValues(leaf *PredicateLeaf) ([]any, bool, error) {
	p := leaf.Duck
	if p.Err != nil {
		return nil, false, p.Err
	}
	return []any{p.Param}, true, nil
}
