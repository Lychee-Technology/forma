//go:build e2e

package production

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// eavPlanEvidenceVar names a directory. When it is set, the #815 benchmark
// writes each size's record there as rows-<N>.md: the setup, the full result
// table, and the generated SQL, arguments and EXPLAIN (ANALYZE, BUFFERS,
// VERBOSE) text of every textPlanCase. The test logs the same record either way.
const eavPlanEvidenceVar = "FORMA_EAV_PLAN_EVIDENCE"

// textPlanCases are the probes, by eav_data load, statistics state and probe
// name, whose plan trees back docs/eav-text-range-plans.md: the overestimated
// mixed token gt and its one-attribute control, its note lt mirror, the
// skewed load's token gt, the pre-#881 anchor, the pre-#728 SQL, the richer
// statistics, a justified scan, a half range planned without statistics, and
// an in list planned on statistics that never saw the attribute.
var textPlanCases = map[[3]string]bool{
	{"note + token", "fresh", "token gt 5"}:                                                    true,
	{"token only", "fresh", "token gt 5"}:                                                      true,
	{"note + token", "fresh", "note lt 5"}:                                                     true,
	{"note + token", "fresh", "token gt half"}:                                                 true,
	{"note + token", "missing", "token gt half"}:                                               true,
	{"note + token", "stale (note only)", "token in"}:                                          true,
	{"note + token on 1 row in 20", "fresh", "token gt 5"}:                                     true,
	{"note + token, pre-#881 anchor with the outer eav_data scan", "missing", "token gt half"}: true,
	{"note + token, pre-#881 anchor with the outer eav_data scan", "fresh", "token gt 5"}:      true,
	{"note + token", "fresh, target 10000 + mcv(attr_id, value_text)", "token gt 5"}:           true,
	{"note + token, pre-#728 SQL on a default-collation column", "fresh", "token gt 5"}:        true,
}

// recordSetup records what a reader needs to reproduce the run: the server,
// the collations, the attribute ids and the index DDL.
func (b *eavPlanBench) recordSetup() {
	b.t.Helper()
	var version, collate string
	if err := b.env.Pool.QueryRow(b.ctx, `SELECT version(), datcollate::text FROM pg_database WHERE datname = current_database()`).
		Scan(&version, &collate); err != nil {
		b.t.Fatalf("read the server version: %v", err)
	}
	rows, err := b.env.Pool.Query(b.ctx, `SELECT indexdef FROM pg_indexes WHERE tablename IN ('eav_data', 'entity_main') ORDER BY indexname`)
	if err != nil {
		b.t.Fatalf("read index definitions: %v", err)
	}
	var defs []string
	for rows.Next() {
		var def string
		if err := rows.Scan(&def); err != nil {
			b.t.Fatalf("scan index definitions: %v", err)
		}
		defs = append(defs, def+";")
	}
	if err := rows.Err(); err != nil {
		b.t.Fatalf("read index definitions: %v", err)
	}
	b.setup = fmt.Sprintf("- Server: %s\n- Database collation: `%s`; `eav_data.value_text`: `%s`\n"+
		"- `e2e_wide` rows: %d; `attr_id` note = %d, token = %d\n\nIndexes:\n\n```sql\n%s\n```\n",
		version, collate, columnCollation(b.ctx, b.t, b.env, "eav_data", "value_text"), b.rows,
		b.attrIDs["note"], b.attrIDs["token"], strings.Join(defs, "\n"))
	b.t.Logf("rows=%d setup:\n%s", b.rows, b.setup)
}

// recordTextPlan records q, its arguments and its EXPLAIN (ANALYZE, BUFFERS,
// VERBOSE) in PostgreSQL's own text form: VERBOSE adds each node's output
// columns and schema-qualified relations, so a reader can tell which
// eav_data alias and subquery a node belongs to (#881). It is its own execution of q, run just before
// the JSON one the result table reads, so its timings and buffers can differ
// from the table's slightly. The SQL is recorded as sent, less the spaces that
// end some of its lines.
func (b *eavPlanBench) recordTextPlan(dataset, stats, probe string, q recordedQuery) {
	b.t.Helper()
	rows, err := b.env.Pool.Query(b.ctx, "EXPLAIN (ANALYZE, BUFFERS, VERBOSE) "+q.sql, q.args...)
	if err != nil {
		b.t.Fatalf("explain %s: %v", probe, err)
	}
	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			b.t.Fatalf("scan the plan of %s: %v", probe, err)
		}
		lines = append(lines, line)
	}
	if err := rows.Err(); err != nil {
		b.t.Fatalf("explain %s: %v", probe, err)
	}
	var args []string
	for i, arg := range q.args {
		args = append(args, fmt.Sprintf("- `$%d` = `%#v`", i+1, arg))
	}
	plan := fmt.Sprintf("### `%s`: %s, statistics %s\n\nArguments:\n\n%s\n\n```sql\n%s\n```\n\n```text\n%s\n```\n",
		probe, dataset, stats, strings.Join(args, "\n"), trimLineEnds(q.sql), strings.Join(lines, "\n"))
	b.plans = append(b.plans, plan)
	b.t.Logf("rows=%d plan:\n%s", b.rows, plan)
}

// recordEvidence logs the result table and, when eavPlanEvidenceVar is set,
// writes the size's whole record. It fails if a textPlanCase never ran, so
// the list follows the test's datasets.
func (b *eavPlanBench) recordEvidence() {
	b.t.Helper()
	table := renderPlanRows(b.report)
	b.t.Logf("rows=%d plans:\n%s", b.rows, table)
	if len(b.plans) != len(textPlanCases) {
		b.t.Errorf("recorded %d plan trees of the %d textPlanCases: %v", len(b.plans), len(textPlanCases),
			slices.Collect(maps.Keys(textPlanCases)))
	}
	dir := os.Getenv(eavPlanEvidenceVar)
	if dir == "" {
		return
	}
	doc := fmt.Sprintf("## N = %d\n\n%s\n## Results\n\n%s\n## Plans\n\n%s", b.rows, b.setup, table, strings.Join(b.plans, "\n"))
	path := filepath.Join(dir, fmt.Sprintf("rows-%d.md", b.rows))
	if err := os.WriteFile(path, []byte(doc), 0o644); err != nil {
		b.t.Fatalf("write the evidence of rows=%d to %s: %v", b.rows, path, err)
	}
}

// trimLineEnds drops the white space around sql and at the end of its lines.
func trimLineEnds(sql string) string {
	lines := strings.Split(strings.TrimSpace(sql), "\n")
	for i, line := range lines {
		lines[i] = strings.TrimRight(line, " \t")
	}
	return strings.Join(lines, "\n")
}
