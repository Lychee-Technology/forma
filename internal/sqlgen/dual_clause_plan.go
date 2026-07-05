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
func (p *DualClausePlan) Bind(condition forma.Condition, cache forma.SchemaAttributeCache, paramIndex *int) (DualClauses, error) {
	if p == nil {
		return DualClauses{}, fmt.Errorf("dual clause plan is nil")
	}
	pgMainArgs, err := bindPgMainArgs(condition, cache)
	if err != nil {
		return DualClauses{}, fmt.Errorf("pg main binding: %w", err)
	}
	pgArgs, err := bindPgEavArgs(condition, cache)
	if err != nil {
		return DualClauses{}, fmt.Errorf("pg eav binding: %w", err)
	}
	duckArgs, err := bindDuckArgs(condition, cache)
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

// bindEmitter adapts a value core to the walker's leafEmitter contract: the
// dummy non-empty SQL keeps the walker's skip/veto accounting identical to
// the string-producing emitters, so args accumulate in the same order.
type bindEmitter struct {
	values func(kv *forma.KvCondition) ([]any, bool, error)
}

func (b *bindEmitter) EmitLeaf(kv *forma.KvCondition) (string, []any, error) {
	args, emit, err := b.values(kv)
	if err != nil || !emit {
		return "", nil, err
	}
	return "x", args, nil
}

func bindPgMainArgs(cond forma.Condition, cache forma.SchemaAttributeCache) ([]any, error) {
	if cond == nil {
		return nil, nil
	}
	guard := &pgMainGuard{cache: cache}
	emitter := &bindEmitter{values: func(kv *forma.KvCondition) ([]any, bool, error) {
		val, emit, err := pgMainLeafValue(kv, cache)
		if err != nil || !emit {
			return nil, false, err
		}
		return []any{val}, true, nil
	}}
	_, args, err := walkCondition(cond, pgMainStyle, guard, emitter)
	return args, err
}

func bindPgEavArgs(cond forma.Condition, cache forma.SchemaAttributeCache) ([]any, error) {
	if cond == nil {
		return nil, nil
	}
	emitter := &bindEmitter{values: func(kv *forma.KvCondition) ([]any, bool, error) {
		attrID, val, _, _, err := pgEavLeafValue(kv, cache)
		if err != nil {
			return nil, false, err
		}
		return []any{attrID, val}, true, nil
	}}
	_, args, err := walkCondition(cond, pgEavStyle, nil, emitter)
	return args, err
}

func bindDuckArgs(cond forma.Condition, cache forma.SchemaAttributeCache) ([]any, error) {
	if cond == nil {
		return nil, nil
	}
	emitter := &bindEmitter{values: func(kv *forma.KvCondition) ([]any, bool, error) {
		parts, err := duckLeafValue(kv, cache)
		if err != nil {
			return nil, false, err
		}
		return []any{parts.param}, true, nil
	}}
	_, args, err := walkCondition(cond, duckStyle, nil, emitter)
	return args, err
}
