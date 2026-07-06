package sqlgen

import (
	"fmt"

	"github.com/lychee-technology/forma"
)

// DualClauses contains SQL fragments and argument lists for both Postgres and DuckDB.
type DualClauses struct {
	PgClause     string // existing EAV-based clause (EXISTS...)
	PgArgs       []any
	PgMainClause string // predicates that can be pushed into entity_main (m.*)
	PgMainArgs   []any

	DuckClause string
	DuckArgs   []any
}

// ToDualClauses generates Postgres and DuckDB WHERE fragments for the given condition.
// - PgClause reuses existing SQLGenerator (EAV-based EXISTS expressions).
// - PgMainClause contains predicates suitable for entity_main pushdown.
// - DuckClause maps attributes to column names when available and emits a simple DuckDB-style clause.
// Note: DuckDB placeholders are "?" and args are returned in order. Postgres uses $n placeholders.
//
// The condition tree is normalized into the typed predicate IR exactly once
// (#143); the three emitters below consume that shared tree.
func ToDualClauses(
	condition forma.Condition,
	eavTable string,
	schemaID int16,
	cache forma.SchemaAttributeCache,
	paramIndex *int,
) (DualClauses, error) {
	tree := normalizePredicates(condition, cache, targetsAll)

	// Build pushdown-capable main table predicates first so placeholders ($n) align.
	pgMainClause, pgMainArgs, err := pgMainClauseFromTree(tree, paramIndex)
	if err != nil {
		return DualClauses{}, fmt.Errorf("pg main generation: %w", err)
	}

	// Postgres EAV side: full condition as EXISTS expressions. A nil
	// top-level condition is skipped (only nested nils are an error).
	var pgClause string
	var pgArgs []any
	if condition != nil {
		pgClause, pgArgs, err = pgEavClausesFromTree(tree, eavTable, paramIndex)
		if err != nil {
			return DualClauses{}, fmt.Errorf("pg sql generation: %w", err)
		}
	}

	// DuckDB side: simple column-based predicates using attribute metadata
	duckClause, duckArgs, err := duckClauseFromTree(tree)
	if err != nil {
		return DualClauses{}, fmt.Errorf("duck sql generation: %w", err)
	}

	return DualClauses{
		PgClause:     pgClause,
		PgArgs:       pgArgs,
		PgMainClause: pgMainClause,
		PgMainArgs:   pgMainArgs,
		DuckClause:   duckClause,
		DuckArgs:     duckArgs,
	}, nil
}

// pgMainTypedGuard vetoes OR groups that are not fully pushable to
// entity_main: pushing a partial OR would silently drop rows matched only
// by the non-pushable branch. Pushability is precomputed per group by the
// normalizer.
type pgMainTypedGuard struct{}

func (pgMainTypedGuard) SkipGroup(g *PredicateGroup) bool {
	if g.Logic == forma.LogicOr {
		return !g.FullyPushable
	}
	return false
}

// pgMainTypedEmitter renders typed predicate leaves for PG main table pushdown.
type pgMainTypedEmitter struct {
	paramIndex *int
}

func (e *pgMainTypedEmitter) EmitTypedLeaf(leaf *PredicateLeaf) (string, []any, error) {
	p := leaf.PgMain
	if p.Err != nil {
		return "", nil, p.Err
	}
	if p.Skip {
		return "", nil, nil
	}

	*e.paramIndex++
	ph := fmt.Sprintf("$%d", *e.paramIndex)
	sql := fmt.Sprintf("%s %s %s", p.Column, p.SQLOp, ph)
	return sql, []any{p.Value}, nil
}

// pgMainClauseFromTree walks an already-normalized predicate tree and emits
// a WHERE fragment targeting entity_main (m.*), advancing paramIndex.
func pgMainClauseFromTree(tree PredicateNode, paramIndex *int) (string, []any, error) {
	emitter := &pgMainTypedEmitter{paramIndex: paramIndex}
	return walkPredicate(tree, pgMainStyle, pgMainTypedGuard{}, emitter)
}

// buildPgMainClause traverses the condition tree and emits a WHERE fragment targeting entity_main (m.*)
// It returns the clause string (with $n placeholders) and args slice, advancing paramIndex as needed.
func buildPgMainClause(cond forma.Condition, cache forma.SchemaAttributeCache, paramIndex *int) (string, []any, error) {
	if cond == nil {
		return "", nil, nil
	}
	return pgMainClauseFromTree(normalizePredicates(cond, cache, targetPgMain), paramIndex)
}

func BuildPgMainClause(cond forma.Condition, cache forma.SchemaAttributeCache, paramIndex *int) (string, []any, error) {
	return buildPgMainClause(cond, cache, paramIndex)
}

// duckTypedEmitter renders typed predicate leaves for DuckDB with ? placeholders and CAST expressions.
type duckTypedEmitter struct{}

func (duckTypedEmitter) EmitTypedLeaf(leaf *PredicateLeaf) (string, []any, error) {
	p := leaf.Duck
	if p.Err != nil {
		return "", nil, p.Err
	}

	if p.TextLike {
		clause := fmt.Sprintf("%s %s ?", p.Column, p.SQLOp)
		return clause, []any{p.Param}, nil
	}

	castExpr := CastExpression("?", p.ValueType)
	clause := fmt.Sprintf("%s %s %s", p.Column, p.SQLOp, castExpr)
	return clause, []any{p.Param}, nil
}

// duckClauseFromTree walks an already-normalized predicate tree and produces
// a DuckDB-compatible WHERE clause.
func duckClauseFromTree(tree PredicateNode) (string, []any, error) {
	return walkPredicate(tree, duckStyle, nil, duckTypedEmitter{})
}

// buildDuckClause traverses the condition tree and produces a DuckDB-compatible WHERE clause.
func buildDuckClause(cond forma.Condition, cache forma.SchemaAttributeCache) (string, []any, error) {
	if cond == nil {
		return "1=1", nil, nil
	}
	return duckClauseFromTree(normalizePredicates(cond, cache, targetDuck))
}

func BuildDuckClause(cond forma.Condition, cache forma.SchemaAttributeCache) (string, []any, error) {
	return buildDuckClause(cond, cache)
}
