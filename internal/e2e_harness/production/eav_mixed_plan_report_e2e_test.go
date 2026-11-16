//go:build e2e

package production

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// planRow is what the #815 benchmark reports of one EXPLAIN (ANALYZE,
// BUFFERS) run of a probe's generated SQL.
type planRow struct {
	dataset, stats, probe string
	matches               int
	seeks                 bool    // the plan seeks the probe's index on its column
	access                string  // the node that applies the probe's predicate
	estimated, actual     float64 // that node's rows per loop
	seqScans              string  // the relations the plan scans sequentially
	// scanOutput is, by relation, the rows the plan's scans returned after
	// their filters (Actual Rows × Actual Loops); scanFiltered is the rows
	// those scans read and discarded by a Filter or an index recheck. Neither
	// counts index entries a btree passes over inside an Index Cond, so
	// neither is the scans' whole work: the buffers are.
	scanOutput, scanFiltered string
	buffers                  int64
	millis                   float64
}

// summarizePlan reads a planRow from out. The predicate node is the first
// executed scan, in plan order, whose condition or filter names column.
func summarizePlan(out explainOutput, column string) planRow {
	row := planRow{
		buffers: out.Plan.SharedHit + out.Plan.SharedRead,
		millis:  out.ExecutionTime,
	}
	seq := map[string]int{}
	output, filtered := map[string]float64{}, map[string]float64{}
	walkExecuted(out.Plan, func(n planNode) {
		if n.NodeType == "Seq Scan" {
			seq[n.RelationName]++
		}
		if n.RelationName != "" {
			output[n.RelationName] += n.ActualRows * n.ActualLoops
			filtered[n.RelationName] += (n.RemovedByFilter + n.RemovedByRecheck) * n.ActualLoops
		}
		if row.access != "" || n.RelationName == "" || !nodeNames(n, column) {
			return
		}
		row.access, row.estimated, row.actual = accessPath(n), n.PlanRows, n.ActualRows
	})
	var scans []string
	for _, rel := range slices.Sorted(maps.Keys(seq)) {
		scans = append(scans, fmt.Sprintf("%s ×%d", rel, seq[rel]))
	}
	row.seqScans = strings.Join(scans, ", ")
	row.scanOutput, row.scanFiltered = byRelation(output), byRelation(filtered)
	return row
}

// byRelation renders rows counted by relation as "eav_data n, entity_main m",
// leaving out the relations with none.
func byRelation(rows map[string]float64) string {
	var parts []string
	for _, rel := range slices.Sorted(maps.Keys(rows)) {
		if rows[rel] > 0 {
			parts = append(parts, fmt.Sprintf("%s %.0f", rel, rows[rel]))
		}
	}
	return strings.Join(parts, ", ")
}

// walkExecuted visits the nodes of the tree that ran at least once, in plan
// order.
func walkExecuted(n planNode, visit func(planNode)) {
	if n.ActualLoops == 0 {
		return
	}
	visit(n)
	for _, child := range n.Plans {
		walkExecuted(child, visit)
	}
}

func nodeNames(n planNode, column string) bool {
	return strings.Contains(n.IndexCond, column) || strings.Contains(n.RecheckCond, column) || strings.Contains(n.Filter, column)
}

// accessPath names a scan and the index it reads, if any.
func accessPath(n planNode) string {
	index := n.IndexName
	for _, child := range n.Plans {
		if child.NodeType == "Bitmap Index Scan" {
			index = child.IndexName
		}
	}
	if index == "" {
		return n.NodeType + " on " + n.RelationName
	}
	return n.NodeType + " on " + index
}

// renderPlanRows renders rows as a Markdown table. The q-error is the larger
// of estimated/actual and actual/estimated, each at least one row.
func renderPlanRows(rows []planRow) string {
	var sb strings.Builder
	sb.WriteString("| eav_data | statistics | probe | matches | predicate access | seeks | est | actual | q-error | seq scans " +
		"| scan output rows | rows filtered by scans | buffers | ms |\n")
	sb.WriteString("| --- | --- | --- | ---: | --- | --- | ---: | ---: | ---: | --- | --- | --- | ---: | ---: |\n")
	for _, r := range rows {
		est, act := max(r.estimated, 1), max(r.actual, 1)
		seeks := map[bool]string{true: "yes", false: "no"}[r.seeks]
		fmt.Fprintf(&sb, "| %s | %s | %s | %d | %s | %s | %.0f | %.0f | %.0f | %s | %s | %s | %d | %.2f |\n",
			r.dataset, r.stats, r.probe, r.matches, r.access, seeks, r.estimated, r.actual, max(est/act, act/est),
			r.seqScans, r.scanOutput, r.scanFiltered, r.buffers, r.millis)
	}
	return sb.String()
}
