//go:build e2e

package production

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// TestMixedEAVTextRangePlans is #815's benchmark of the plans PostgreSQL picks
// for text and uuid attributes that share eav_data.value_text. PR #860 showed
// init-db's one eav_data text index, keyed under the column's C collation,
// seeks equals, in and the four ranges with one attribute loaded at a time;
// with uuid tokens and t-prefixed notes stored together, token gt and note lt
// were planned differently. This test separates the two questions that
// observation mixes:
//
//   - Eligibility. Every selective probe, in every load and statistics state,
//     must seek the index with value_text in its Index Cond, in its own plan
//     or with sequential scans priced out (see eligibilityProbe). That is the
//     collation contract, and it holds or fails apart from the statistics.
//   - Selection. Under each load and statistics state the test records, for the
//     generated SQL under EXPLAIN (ANALYZE, BUFFERS), the predicate's access
//     path, its estimated and actual rows, the plan's sequential scans, the
//     rows its scans returned and filtered out by relation, its buffers and
//     its time. Plan choice
//     is reported, not asserted: a scan is the right plan for half of an
//     attribute, and which plan a misestimate yields depends on costs the test
//     does not pin.
//
// Two passes report what would change the picture: one with richer
// statistics, and one with the anchor's single-leaf EAV branch rewritten
// without its outer eav_data scan (#881). A last pass moves value_text back to
// the database default and runs the pre-#728 SQL, without COLLATE "C". Its
// estimates must match the compliant table's: a misestimate that survives the
// change of collation comes from the statistics, not from the collation.
//
// Every probe's rows are checked against a bytewise comparison done in Go. The
// tables log as Markdown, with the generated SQL, arguments and full plan text
// of the textPlanCases; FORMA_EAV_PLAN_ROWS runs other sizes, and
// FORMA_EAV_PLAN_EVIDENCE writes the same record to files. The recorded results
// and their reading are in docs/eav-text-range-plans.md.
func TestMixedEAVTextRangePlans(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, rows := range eavPlanSizes(t) {
		t.Run(fmt.Sprintf("rows=%d", rows), func(t *testing.T) {
			b := newEAVPlanBench(ctx, t, rows)
			b.recordSetup()

			mixed := eavLoad{"note + token", map[string]int{"note": 1, "token": 1}}
			b.load(mixed)
			b.requireNoStatistics()
			b.run(mixed.name, "missing", "note", "token")
			b.analyze()
			fresh := b.run(mixed.name, "fresh", "note", "token")
			b.runProbes(mixed.name, "fresh", b.compositeProbes())

			b.load(eavLoad{"note only", map[string]int{"note": 1}})
			b.analyze()
			b.run("note only", "fresh", "note")
			b.add(eavLoad{"token", map[string]int{"token": 1}})
			b.run(mixed.name, "stale (note only)", "note", "token")

			b.load(eavLoad{"token only", map[string]int{"token": 1}})
			b.analyze()
			b.run("token only", "fresh", "token")

			skew := eavLoad{"note + token on 1 row in 20", map[string]int{"note": 1, "token": 20}}
			b.load(skew)
			b.analyze()
			b.run(skew.name, "fresh", "note", "token")

			b.load(mixed)
			b.analyze()
			b.withoutOuterScan(mixed.name)
			b.extendedStatistics(mixed.name)
			b.preCollationShape(mixed.name, fresh)
			b.recordEvidence()
		})
	}
}

// run runs the eavProbes of each attr and returns their rows by probe name.
func (b *eavPlanBench) run(dataset, stats string, attrs ...string) map[string]planRow {
	b.t.Helper()
	var probes []eavProbe
	for _, attr := range attrs {
		probes = append(probes, b.eavProbes(attr)...)
	}
	return b.runProbes(dataset, stats, probes)
}

// runProbes queries, explains and records each probe, and fails a probe whose
// index is not eligible.
func (b *eavPlanBench) runProbes(dataset, stats string, probes []eavProbe) map[string]planRow {
	b.t.Helper()
	got := map[string]planRow{}
	for _, p := range probes {
		q := b.query(p)
		row := b.explainRow(dataset, stats, p, q)
		got[p.name] = row
		if eligibilityProbe(p, stats) && !row.seeks && !b.forcedSeek(q, p) {
			b.t.Errorf("%s (%s, statistics %s): %s is not eligible to seek %s", p.name, dataset, stats, p.index, p.column)
		}
	}
	return got
}

func (b *eavPlanBench) explainRow(dataset, stats string, p eavProbe, q recordedQuery) planRow {
	b.t.Helper()
	if textPlanCases[[3]string{dataset, stats, p.name}] {
		b.recordTextPlan(dataset, stats, p.name, q)
	}
	var raw []byte
	if err := b.env.Pool.QueryRow(b.ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+q.sql, q.args...).Scan(&raw); err != nil {
		b.t.Fatalf("explain %s: %v", p.name, err)
	}
	out := decodeExplain(b.t, raw)
	row := summarizePlan(out, p.column)
	row.dataset, row.stats, row.probe, row.matches = dataset, stats, p.name, len(p.want)
	row.seeks = anyIndexCondMentions(indexScansOf(out.Plan, p.index), p.column)
	b.report = append(b.report, row)
	return row
}

// withoutOuterScan records the ranges with the anchor's first branch, which
// reads every eav_data row of the schema as t and keeps those of a row with a
// matching x, rewritten to read x alone. For one EAV leaf both select the
// same row ids; whether the rewrite holds for every condition is #881's
// question, not this one's. The pass only measures what the outer scan costs
// once the predicate is overestimated, and logs instead when the generated
// SQL no longer has the branch.
func (b *eavPlanBench) withoutOuterScan(dataset string) {
	b.t.Helper()
	for _, attr := range []string{"note", "token"} {
		for _, p := range b.eavProbes(attr) {
			if !p.ranged {
				continue
			}
			q := b.query(p)
			if n := len(outerEAVBranch.FindAllStringIndex(q.sql, -1)); n != 1 {
				b.t.Logf("the anchor of %s reads eav_data as an outer t %d times, not once; skipping the rewrite", p.name, n)
				return
			}
			q.sql = outerEAVBranch.ReplaceAllString(q.sql, `SELECT DISTINCT x.row_id FROM "eav_data" x WHERE x.schema_id = $$1 AND $1`)
			b.explainRow(dataset+", anchor without the outer eav_data scan", "fresh", p, q)
		}
	}
}

// outerEAVBranch is the anchor's first branch for one EAV leaf; its group is
// the leaf's attribute and value compare.
var outerEAVBranch = regexp.MustCompile(`SELECT DISTINCT t\.row_id\s+FROM "eav_data" t\s+WHERE t\.schema_id = \$1 AND \(EXISTS \(SELECT 1 FROM "eav_data" x ` +
	`WHERE x\.schema_id = t\.schema_id AND x\.row_id = t\.row_id AND (x\.attr_id = \$2 AND x\.value_text COLLATE "C" [<>=]+ \$3)\)\)`)

// extendedStatistics records the ranges with the planner's statistics at
// their richest: value_text's statistics target at its maximum, and an mcv
// extended statistic on (attr_id, value_text). Neither is part of init-db; the
// pass shows whether statistics tuning is a remedy.
func (b *eavPlanBench) extendedStatistics(dataset string) {
	b.t.Helper()
	b.env.ExecSQL(b.ctx, `ALTER TABLE eav_data ALTER COLUMN value_text SET STATISTICS 10000`)
	b.env.ExecSQL(b.ctx, `CREATE STATISTICS eav_data_attr_text (mcv) ON attr_id, value_text FROM eav_data`)
	b.analyze()
	var probes []eavProbe
	for _, attr := range []string{"note", "token"} {
		for _, p := range b.eavProbes(attr) {
			if p.ranged {
				probes = append(probes, p)
			}
		}
	}
	b.runProbes(dataset, "fresh, target 10000 + mcv(attr_id, value_text)", probes)
	b.env.ExecSQL(b.ctx, `DROP STATISTICS eav_data_attr_text`)
	b.env.ExecSQL(b.ctx, `ALTER TABLE eav_data ALTER COLUMN value_text SET STATISTICS -1`)
}

// preCollationShape moves value_text to the database default, as before #816,
// and runs each range probe's SQL as before #728, without COLLATE "C". The
// planner must estimate the predicate as it did on the compliant table. The
// probes' rows still come from the current SQL.
func (b *eavPlanBench) preCollationShape(dataset string, compliant map[string]planRow) {
	b.t.Helper()
	b.env.ExecSQL(b.ctx, `ALTER TABLE eav_data ALTER COLUMN value_text TYPE text COLLATE "default"`)
	b.analyze()
	dataset += ", pre-#728 SQL on a default-collation column"
	for _, attr := range []string{"note", "token"} {
		for _, p := range b.eavProbes(attr) {
			if !p.ranged {
				continue
			}
			q := b.query(p)
			q.sql = strings.ReplaceAll(q.sql, `value_text COLLATE "C"`, "value_text")
			row := b.explainRow(dataset, "fresh", p, q)
			if want := compliant[p.name]; !similarEstimate(row.estimated, want.estimated, 2*b.rows) {
				b.t.Errorf("%s: the predicate is estimated at %.0f rows before #728 and %.0f under the C contract; "+
					"the collation changed the estimate", p.name, row.estimated, want.estimated)
			}
			if eligibilityProbe(p, "fresh") && !row.seeks && !b.forcedSeek(q, p) {
				b.t.Errorf("%s: the pre-#728 SQL does not seek the default-collation index", p.name)
			}
		}
	}
}

// similarEstimate allows for ANALYZE's sampling, which moves a bound in the
// histogram's last bucket between a few rows and a few hundred: estimates
// under 1% of the eavRows rows all count as selective, and larger ones must
// agree within a factor of two.
func similarEstimate(a, b float64, eavRows int) bool {
	if max(a, b) < float64(eavRows)/100 {
		return true
	}
	return a <= 2*b && b <= 2*a
}

// explainOutput is one EXPLAIN (FORMAT JSON) result.
type explainOutput struct {
	Plan          planNode `json:"Plan"`
	ExecutionTime float64  `json:"Execution Time"`
}

func decodeExplain(t *testing.T, raw []byte) explainOutput {
	t.Helper()
	var out []explainOutput
	if err := json.Unmarshal(raw, &out); err != nil || len(out) != 1 {
		t.Fatalf("decode EXPLAIN output (%v): %s", err, raw)
	}
	return out[0]
}
