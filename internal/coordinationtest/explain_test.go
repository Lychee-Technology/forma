package coordinationtest

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/declaredindex"
	"github.com/stretchr/testify/require"
)

// The claim's candidate select and its group-head subquery use the declared
// indexes (#647). The SQL is the statement Forma sent for an adapter
// ClaimBatch, captured by the pool's tracer, explained over 10,000 analyzed
// outbox rows in 100 partitions. Every row is live and few are due: in each
// partition the 5 earliest are due and the other 95 are scheduled for later.
// That is the shape the indexes are for. When most stored rows are published
// instead, the planner rightly reads the few live ones once, with a
// sequential scan, rather than probe the fifo index per candidate.
func TestClaimPlanUsesTheDeclaredIndexes(t *testing.T) {
	requirePostgresMechanisms(t) // the plan is PostgreSQL's
	const partitions, perPartition, due = 100, 100, 5
	e := newEnv(t)
	e.seedBacklog(t, partitions, perPartition, due)
	e.analyze(t)
	var claimed []OutboxMessage
	statements := e.tracer.record(func() { claimed = e.claimBatch(t, "relay", time.Hour, 10) })
	require.Len(t, claimed, 10, "ten partition heads are claimed")

	candidate := candidateSelect(t, e, statements)
	var explained []map[string]any
	require.NoError(t, e.pool.QueryRow(e.ctx, "EXPLAIN (FORMAT JSON) "+candidate.sql, candidate.args...).Scan(&explained))
	require.Len(t, explained, 1)
	plan := explained[0]["Plan"]
	declared := e.declaredIndexes(t)
	fifo := declared.named(t, OutboxSchema, "fifo")

	t.Run("the head subquery reads the fifo index", func(t *testing.T) {
		// PostgreSQL names a repeated alias e_2 and so on.
		earlier := regexp.MustCompile(`^e(_[0-9]+)?$`)
		scans := planNodes(plan, func(n map[string]any) bool {
			alias, _ := n["Alias"].(string)
			return earlier.MatchString(alias) && n["Relation Name"] == e.tables.entityMain
		})
		require.NotEmpty(t, scans, "the plan reads the earlier rows e")
		for _, scan := range scans {
			used := indexNames(scan)
			require.NotEmpty(t, used, "the scan of e is a %v, not an index scan", scan["Node Type"])
			for _, name := range used {
				require.Equal(t, fifo, name, "the scan of e reads %s", declared[name])
			}
		}
	})
	// entity_main is shared, so a branch of the claim condition may read
	// another schema's declared index on the same column: here the due branch
	// reads the cleanup index on bigint_01, where next_attempt_at is stored,
	// rather than the due index led by status. At least one scan must read
	// an index the outbox declares.
	t.Run("the candidate rows are read through declared indexes", func(t *testing.T) {
		// The scans of m that test the claim condition; the others fetch a
		// selected row by its ID.
		status := e.column(t, OutboxSchema, "status")
		candidate := regexp.MustCompile(`^m(_[0-9]+)?$`)
		scans := planNodes(plan, func(n map[string]any) bool {
			alias, _ := n["Alias"].(string)
			return candidate.MatchString(alias) && n["Relation Name"] == e.tables.entityMain && strings.Contains(fmt.Sprint(n), status)
		})
		require.NotEmpty(t, scans, "the plan reads the candidate rows m")
		var outbox []string
		for _, scan := range scans {
			used := indexNames(scan)
			require.NotEmpty(t, used, "the scan of m is a %v, not an index scan", scan["Node Type"])
			for _, name := range used {
				require.Contains(t, declared, name, "the scan of m reads an index no schema declared")
				t.Logf("a scan of m reads %s, declared as %v", name, declared[name])
				if declared.serves(name, OutboxSchema) {
					outbox = append(outbox, name)
				}
			}
		}
		require.NotEmpty(t, outbox, "a scan of m reads an index the outbox declares")
	})
	if t.Failed() {
		t.Logf("candidate select:\n%s\nplan: %v", candidate.sql, plan)
	}
}

// seedBacklog enqueues perPartition pending messages in each partition
// through the public BatchCreate: the first due of each are due, the rest
// scheduled a day ahead.
func (e *env) seedBacklog(t *testing.T, partitions, perPartition, due int) {
	t.Helper()
	base := time.Now().Add(-24 * time.Hour).Truncate(time.Millisecond)
	for p := 0; p < partitions; p += 10 {
		ops := make([]forma.EntityOperation, 0, 10*perPartition)
		for q := p; q < p+10; q++ {
			for i := 0; i < perPartition; i++ {
				at := base.Add(time.Duration(i*partitions+q) * time.Millisecond)
				next := at
				if i >= due {
					next = at.Add(48 * time.Hour)
				}
				ops = append(ops, forma.EntityOperation{Type: forma.OperationCreate,
					EntityIdentifier: forma.EntityIdentifier{SchemaName: OutboxSchema},
					Data: map[string]any{"partition_key": fmt.Sprintf("partition-%03d", q), "topic": "orders", "status": OutboxPending,
						"enqueued_at": rfc3339(at), "next_attempt_at": rfc3339(next), "attempts": 0, "lock_generation": 0}})
			}
		}
		result, err := e.em.BatchCreate(e.ctx, &forma.BatchOperation{Operations: ops, Atomic: true})
		require.NoError(t, err)
		require.Len(t, result.Successful, len(ops))
	}
}

// candidateSelect picks the claim's candidate select among the statements
// a claim sent: the read that tests the group head and locks nothing.
func candidateSelect(t *testing.T, e *env, statements []tracedStatement) tracedStatement {
	t.Helper()
	head := regexp.MustCompile(`NOT EXISTS \(SELECT 1 FROM \S*` + regexp.QuoteMeta(e.tables.entityMain) + `"? e WHERE`)
	var found []tracedStatement
	for _, s := range statements {
		if head.MatchString(s.sql) && !strings.Contains(s.sql, "FOR UPDATE") {
			found = append(found, s)
		}
	}
	require.Len(t, found, 1, "one candidate select among %d statements", len(statements))
	return found[0]
}

// declaredIndexes maps each declared index of entity_main to the
// declarations it serves, as schema.name.
type declaredIndexes map[string][]string

func (e *env) declaredIndexes(t *testing.T) declaredIndexes {
	t.Helper()
	declarations, err := declaredindex.FromRegistry(e.registry)
	require.NoError(t, err)
	planned, err := declaredindex.Plan(e.tables.entityMain, declarations)
	require.NoError(t, err)
	indexes := declaredIndexes{}
	for _, ix := range planned {
		for _, source := range ix.Sources {
			indexes[ix.Name] = append(indexes[ix.Name], source.Schema+"."+source.Index)
		}
	}
	return indexes
}

// named answers the index that serves schema's declaration name.
func (d declaredIndexes) named(t *testing.T, schema, name string) string {
	t.Helper()
	for index, sources := range d {
		if slices.Contains(sources, schema+"."+name) {
			return index
		}
	}
	t.Fatalf("schema %s declares no index %s", schema, name)
	return ""
}

// serves reports whether index serves a declaration of schema.
func (d declaredIndexes) serves(index, schema string) bool {
	return slices.ContainsFunc(d[index], func(source string) bool { return strings.HasPrefix(source, schema+".") })
}

// indexNames answers the indexes a plan node and its children read.
func indexNames(node map[string]any) []string {
	var names []string
	for _, n := range planNodes(node, func(n map[string]any) bool { return n["Index Name"] != nil }) {
		names = append(names, n["Index Name"].(string))
	}
	return names
}

// planNodes answers the nodes of an EXPLAIN (FORMAT JSON) plan tree, root
// included, that match.
func planNodes(plan any, match func(map[string]any) bool) []map[string]any {
	node, ok := plan.(map[string]any)
	if !ok {
		return nil
	}
	var found []map[string]any
	if match(node) {
		found = append(found, node)
	}
	children, _ := node["Plans"].([]any)
	for _, child := range children {
		found = append(found, planNodes(child, match)...)
	}
	return found
}
