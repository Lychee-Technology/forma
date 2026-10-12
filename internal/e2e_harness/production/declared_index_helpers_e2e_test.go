//go:build e2e

package production

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma/internal/declaredindex"
)

// Helpers for the #647 declared-index tests: which physical index serves a
// declaration, what the catalog says about it, and which index a query the
// repository generated actually reads through.

// declaredIndexFor returns the physical index planned for the declaration
// "schema.index", from the Env's live registry.
func declaredIndexFor(t *testing.T, env *Env, declaration string) declaredindex.Index {
	t.Helper()
	for _, ix := range plannedDeclaredIndexes(t, env) {
		for _, src := range ix.Sources {
			if src.String() == declaration {
				return ix
			}
		}
	}
	t.Fatalf("no declared index planned for %s", declaration)
	return declaredindex.Index{}
}

func plannedDeclaredIndexes(t *testing.T, env *Env) []declaredindex.Index {
	t.Helper()
	schemas, err := declaredindex.FromRegistry(env.Registry)
	if err != nil {
		t.Fatalf("read declarations from the registry: %v", err)
	}
	planned, err := declaredindex.Plan(env.Tables.EntityMain, schemas)
	if err != nil {
		t.Fatalf("plan declared indexes: %v", err)
	}
	return planned
}

// catalogIndex is one index on entity_main as pg_indexes and pg_index
// describe it, read independently of declaredindex's own catalog query.
type catalogIndex struct {
	def   string
	valid bool
}

func entityMainIndexes(ctx context.Context, t *testing.T, env *Env) map[string]catalogIndex {
	t.Helper()
	rows, err := env.Pool.Query(ctx, `
		SELECT p.indexname::text, p.indexdef, i.indisvalid
		FROM pg_indexes p
		JOIN pg_class c ON c.relname = p.indexname AND c.relnamespace = to_regnamespace(p.schemaname)
		JOIN pg_index i ON i.indexrelid = c.oid
		WHERE p.schemaname = 'public' AND p.tablename = $1`, env.Tables.EntityMain)
	if err != nil {
		t.Fatalf("read entity_main indexes: %v", err)
	}
	found := map[string]catalogIndex{}
	var name, def string
	var valid bool
	if _, err := pgx.ForEachRow(rows, []any{&name, &def, &valid}, func() error {
		found[name] = catalogIndex{def: def, valid: valid}
		return nil
	}); err != nil {
		t.Fatalf("scan entity_main indexes: %v", err)
	}
	return found
}

// recordingPool is the pool the repository under test queries through; it
// keeps every statement it runs so the test can EXPLAIN exactly that SQL with
// exactly those arguments.
type recordingPool struct {
	*pgxpool.Pool
	mu      sync.Mutex
	queries []recordedQuery
}

type recordedQuery struct {
	sql  string
	args []any
}

func (p *recordingPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.mu.Lock()
	p.queries = append(p.queries, recordedQuery{sql: sql, args: slices.Clone(args)})
	p.mu.Unlock()
	return p.Pool.Query(ctx, sql, args...)
}

// takeAnchorQuery returns the one optimized hot-tier query recorded since the
// last call, and clears the record.
func (p *recordingPool) takeAnchorQuery(t *testing.T) recordedQuery {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	var anchors []recordedQuery
	for _, q := range p.queries {
		if strings.Contains(q.sql, "WITH anchor AS") {
			anchors = append(anchors, q)
		}
	}
	p.queries = nil
	if len(anchors) != 1 {
		t.Fatalf("recorded %d optimized queries, want exactly 1", len(anchors))
	}
	return anchors[0]
}

// planNode is the part of an EXPLAIN (FORMAT JSON) node the tests read.
type planNode struct {
	NodeType  string     `json:"Node Type"`
	IndexName string     `json:"Index Name"`
	IndexCond string     `json:"Index Cond"`
	Plans     []planNode `json:"Plans"`
}

// explain returns the plan PostgreSQL chooses for q, run with q's arguments.
func explain(ctx context.Context, t *testing.T, pool *pgxpool.Pool, q recordedQuery) planNode {
	t.Helper()
	var raw []byte
	if err := pool.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+q.sql, q.args...).Scan(&raw); err != nil {
		t.Fatalf("explain the generated query: %v", err)
	}
	var plans []struct {
		Plan planNode `json:"Plan"`
	}
	if err := json.Unmarshal(raw, &plans); err != nil || len(plans) != 1 {
		t.Fatalf("decode EXPLAIN output (%v): %s", err, raw)
	}
	return plans[0].Plan
}

// indexScansOf returns every node of the plan tree that reads through index
// name: an Index Scan, Index Only Scan or Bitmap Index Scan.
func indexScansOf(root planNode, name string) []planNode {
	var found []planNode
	var walk func(planNode)
	walk = func(n planNode) {
		switch n.NodeType {
		case "Index Scan", "Index Only Scan", "Bitmap Index Scan":
			if n.IndexName == name {
				found = append(found, n)
			}
		}
		for _, child := range n.Plans {
			walk(child)
		}
	}
	walk(root)
	return found
}

// describePlan renders the plan tree's node types and index names, for a
// failure message that shows what the planner chose instead.
func describePlan(n planNode, depth int) string {
	line := strings.Repeat("  ", depth) + n.NodeType
	if n.IndexName != "" {
		line += fmt.Sprintf(" using %s (%s)", n.IndexName, n.IndexCond)
	}
	out := line + "\n"
	for _, child := range n.Plans {
		out += describePlan(child, depth+1)
	}
	return out
}
