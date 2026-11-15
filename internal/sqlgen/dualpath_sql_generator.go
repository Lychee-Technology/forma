package sqlgen

import (
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/conditionexpr"
)

// DualClauses contains the WHERE fragments and argument lists a federated
// condition renders to.
type DualClauses struct {
	PgClause string // EAV-based clause (EXISTS...)
	PgArgs   []any

	DuckClause string
	DuckArgs   []any
}

// ToDualClauses generates the Postgres EAV and DuckDB WHERE fragments for the
// given condition.
//   - PgClause renders the condition as EAV EXISTS expressions under the
//     strict operator parse, with "$n" placeholders numbered from paramIndex.
//   - DuckClause maps attributes to their parquet column names and uses
//     DuckDB positional "?".
//
// The federated parquet pass consumes DuckClause; the pgx hot leg renders its
// condition through the PostgreSQL route's own builder. Rendering PgClause as
// well keeps its strict parse among the refusals the federated build raises
// before it reads (translateDuckDBQuery). Args are returned in placeholder
// order for both.
//
// The retired attach hot leg (#858) also consumed an entity_main pushdown
// fragment (PgMainClause) that pre-filtered its PostgreSQL scan; nothing has
// read it since, and #871 retired its generation.
//
// The condition tree is normalized into the typed predicate IR exactly once
// (#143); both emitters below consume that shared tree.
func ToDualClauses(
	condition forma.Condition,
	eavTable string,
	schemaID int16,
	cache forma.SchemaAttributeCache,
	paramIndex *int,
) (DualClauses, error) {
	tree := normalizePredicates(condition, cache, targetsAll)

	// Postgres EAV side: full condition as EXISTS expressions. A nil
	// top-level condition is skipped (only nested nils are an error).
	var pgClause string
	var pgArgs []any
	var err error
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
		PgClause:   pgClause,
		PgArgs:     pgArgs,
		DuckClause: duckClause,
		DuckArgs:   duckArgs,
	}, nil
}

// duckTypedEmitter renders typed predicate leaves for DuckDB with ? placeholders and CAST expressions.
type duckTypedEmitter struct{}

func (duckTypedEmitter) EmitTypedLeaf(leaf *PredicateLeaf) (string, []any, error) {
	p := leaf.Duck
	if p.Err != nil {
		return "", nil, p.Err
	}

	if p.NullTest != "" {
		return p.Column + " " + p.NullTest, nil, nil
	}
	if p.Set != nil {
		return p.Set.Render(), []any{p.Param}, nil
	}

	if p.TextLike {
		clause := fmt.Sprintf("%s %s ?", p.Column, p.SQLOp)
		if p.SQLOp == "LIKE" {
			clause += conditionexpr.DuckLikeEscapeClause
		}
		return clause, []any{p.Param}, nil
	}

	column := p.Column
	if p.ValueType == forma.ValueTypeUUID && p.SQLOp != "=" && p.SQLOp != "!=" {
		// DuckDB compares UUID with VARCHAR for (in)equality only, and a uuid
		// attribute is UUID where its parquet column is a bound column's export
		// and VARCHAR where it is EAV-only. An ordered compare casts the column
		// too; canonical lowercase text orders as the UUID bytes (#676).
		column = CastExpression(column, p.ValueType)
	}
	castExpr := CastExpression("?", p.ValueType)
	clause := fmt.Sprintf("%s %s %s", column, p.SQLOp, castExpr)
	return clause, []any{p.Param}, nil
}

// duckClauseFromTree walks an already-normalized predicate tree and produces
// a DuckDB-compatible WHERE clause.
func duckClauseFromTree(tree PredicateNode) (string, []any, error) {
	return walkPredicate(tree, duckStyle, duckTypedEmitter{})
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
