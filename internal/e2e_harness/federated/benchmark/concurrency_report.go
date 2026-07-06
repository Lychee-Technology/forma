package benchmark

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// concurrencyStabilityAssertions are the PR #94 fixed-concurrency assertions
// whose pass rates the concurrency report surfaces. They only fire when a
// batch runs with concurrency >= 2.
var concurrencyStabilityAssertions = []string{
	"concurrent-run-total-records-stable",
	"concurrent-run-page-row-ids-stable",
	"concurrent-run-failure-kind-stable",
	"concurrent-run-route-engine-stable",
}

// ConcurrencyReport aggregates N single-concurrency benchmark summaries into
// one publishable artifact comparing latency percentiles and concurrency
// stability assertions across concurrency levels (#104).
type ConcurrencyReport struct {
	Metadata   ArtifactMetadata           `json:"metadata"`
	Provenance *RunProvenance             `json:"provenance,omitempty"`
	Levels     []int                      `json:"levels"`
	Comparable bool                       `json:"comparable"`
	Warnings   []string                   `json:"warnings,omitempty"`
	Summary    []ConcurrencyLevelSummary  `json:"summary"`
	Workloads  []WorkloadConcurrencyTrend `json:"workloads,omitempty"`
}

// ConcurrencyLevelSummary is one whole-run row of the concurrency matrix.
// The *DeltaVsBase fields carry the delta against the lowest concurrency
// level in the report (zero on that baseline row itself).
type ConcurrencyLevelSummary struct {
	Concurrency         int                      `json:"concurrency"`
	ExecutionCount      int                      `json:"execution_count"`
	Passed              bool                     `json:"passed"`
	FailureCount        int                      `json:"failure_count,omitempty"`
	P50                 time.Duration            `json:"p50"`
	P95                 time.Duration            `json:"p95"`
	P99                 time.Duration            `json:"p99"`
	P50DeltaVsBase      time.Duration            `json:"p50_delta_vs_base,omitempty"`
	P95DeltaVsBase      time.Duration            `json:"p95_delta_vs_base,omitempty"`
	P99DeltaVsBase      time.Duration            `json:"p99_delta_vs_base,omitempty"`
	QPS                 float64                  `json:"qps"`
	StabilityAssertions map[string]AssertionStat `json:"stability_assertions,omitempty"`
}

// WorkloadConcurrencyTrend tracks one workload across concurrency levels.
type WorkloadConcurrencyTrend struct {
	Name         string                     `json:"name"`
	TargetSchema string                     `json:"target_schema,omitempty"`
	Levels       []WorkloadConcurrencyLevel `json:"levels"`
}

// WorkloadConcurrencyLevel is one workload row at one concurrency level.
// The *DeltaVsBase fields carry the delta against the workload's own row at
// its lowest concurrency level (zero on that baseline row itself).
type WorkloadConcurrencyLevel struct {
	Concurrency         int                      `json:"concurrency"`
	ExecutionCount      int                      `json:"execution_count"`
	Passed              bool                     `json:"passed"`
	P50                 time.Duration            `json:"p50"`
	P95                 time.Duration            `json:"p95"`
	P99                 time.Duration            `json:"p99"`
	P50DeltaVsBase      time.Duration            `json:"p50_delta_vs_base,omitempty"`
	P95DeltaVsBase      time.Duration            `json:"p95_delta_vs_base,omitempty"`
	P99DeltaVsBase      time.Duration            `json:"p99_delta_vs_base,omitempty"`
	QPS                 float64                  `json:"qps"`
	StabilityAssertions map[string]AssertionStat `json:"stability_assertions,omitempty"`
}

// BuildConcurrencyReport aggregates one summary per concurrency level into a
// ConcurrencyReport. Summaries missing a provenance concurrency count as
// level 1 (pre-#104 artifacts); duplicate levels are an input error.
func BuildConcurrencyReport(summaries []SummaryReport) (ConcurrencyReport, error) {
	if len(summaries) == 0 {
		return ConcurrencyReport{}, fmt.Errorf("no summaries provided")
	}

	ordered := make([]SummaryReport, len(summaries))
	copy(ordered, summaries)
	sort.SliceStable(ordered, func(i, j int) bool {
		return summaryConcurrency(ordered[i]) < summaryConcurrency(ordered[j])
	})

	seen := map[int]bool{}
	report := ConcurrencyReport{
		Metadata:   ordered[0].Metadata,
		Provenance: ordered[0].Provenance,
		Comparable: true,
	}
	baseIdentity := concurrencyAgnosticIdentity(ordered[0])

	workloadIndex := map[string]int{}

	for _, summary := range ordered {
		level := summaryConcurrency(summary)
		if seen[level] {
			return ConcurrencyReport{}, fmt.Errorf("duplicate concurrency level %d", level)
		}
		seen[level] = true
		report.Levels = append(report.Levels, level)

		if identity := concurrencyAgnosticIdentity(summary); identity != baseIdentity {
			report.Comparable = false
			report.Warnings = append(report.Warnings,
				fmt.Sprintf("run at concurrency %d is not comparable to the first run: %s vs %s", level, identity, baseIdentity))
		}

		report.Summary = append(report.Summary, ConcurrencyLevelSummary{
			Concurrency:         level,
			ExecutionCount:      summary.ExecutionCount,
			Passed:              summary.Passed,
			FailureCount:        summary.FailureCount,
			P50:                 summary.P50,
			P95:                 summary.P95,
			P99:                 summary.P99,
			QPS:                 summary.QPS,
			StabilityAssertions: filterStabilityAssertions(summary.AssertionStats),
		})

		for _, w := range summary.Workloads {
			idx, ok := workloadIndex[w.Name]
			if !ok {
				idx = len(report.Workloads)
				workloadIndex[w.Name] = idx
				report.Workloads = append(report.Workloads, WorkloadConcurrencyTrend{
					Name:         w.Name,
					TargetSchema: w.TargetSchema,
				})
			}
			report.Workloads[idx].Levels = append(report.Workloads[idx].Levels, WorkloadConcurrencyLevel{
				Concurrency:         level,
				ExecutionCount:      w.ExecutionCount,
				Passed:              w.Passed,
				P50:                 w.P50,
				P95:                 w.P95,
				P99:                 w.P99,
				QPS:                 w.QPS,
				StabilityAssertions: filterStabilityAssertions(w.AssertionStats),
			})
		}
	}

	applyConcurrencyDeltas(&report)

	return report, nil
}

// applyConcurrencyDeltas fills the *DeltaVsBase fields against the lowest
// concurrency level present (overall) and each workload's own lowest-level
// row. The baseline rows keep zero deltas.
func applyConcurrencyDeltas(report *ConcurrencyReport) {
	if len(report.Summary) > 0 {
		base := report.Summary[0]
		for i := range report.Summary[1:] {
			row := &report.Summary[i+1]
			row.P50DeltaVsBase = row.P50 - base.P50
			row.P95DeltaVsBase = row.P95 - base.P95
			row.P99DeltaVsBase = row.P99 - base.P99
		}
	}
	for w := range report.Workloads {
		levels := report.Workloads[w].Levels
		if len(levels) == 0 {
			continue
		}
		base := levels[0]
		for i := range levels[1:] {
			row := &levels[i+1]
			row.P50DeltaVsBase = row.P50 - base.P50
			row.P95DeltaVsBase = row.P95 - base.P95
			row.P99DeltaVsBase = row.P99 - base.P99
		}
	}
}

// CollectConcurrencySummaries walks dir for benchmark-summary.json files for
// evidence aggregation. Unlike ReadTrendHistory it fails loudly on unreadable
// or malformed files (a silently dropped file would silently drop a
// concurrency level from the evidence) and does not require provenance
// timestamps (provenance-less artifacts count as C=1).
func CollectConcurrencySummaries(dir string) ([]SummaryReport, error) {
	var summaries []SummaryReport
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || info.Name() != "benchmark-summary.json" {
			return nil
		}
		summary, readErr := ReadSummaryReport(path)
		if readErr != nil {
			return fmt.Errorf("read summary %s: %w", path, readErr)
		}
		summaries = append(summaries, summary)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return summaries, nil
}

func summaryConcurrency(summary SummaryReport) int {
	if summary.Provenance != nil && summary.Provenance.Concurrency > 1 {
		return summary.Provenance.Concurrency
	}
	return 1
}

// concurrencyAgnosticIdentity is buildComparabilityIdentity with the
// concurrency segment removed: sweep runs differ in concurrency by design.
func concurrencyAgnosticIdentity(summary SummaryReport) string {
	if summary.Provenance == nil {
		return summary.Metadata.BenchmarkID
	}
	pc := *summary.Provenance
	pc.Concurrency = 0
	stripped := summary
	stripped.Provenance = &pc
	return buildComparabilityIdentity(stripped)
}

func filterStabilityAssertions(stats map[string]AssertionStat) map[string]AssertionStat {
	var filtered map[string]AssertionStat
	for _, name := range concurrencyStabilityAssertions {
		if stat, ok := stats[name]; ok {
			if filtered == nil {
				filtered = make(map[string]AssertionStat, len(concurrencyStabilityAssertions))
			}
			filtered[name] = stat
		}
	}
	return filtered
}

// FormatConcurrencyMarkdown renders the report as a single publishable
// markdown document.
func FormatConcurrencyMarkdown(report ConcurrencyReport) string {
	var b strings.Builder
	b.WriteString("# Concurrency Benchmark Report\n\n")

	b.WriteString(fmt.Sprintf("- benchmark_id: %s\n", report.Metadata.BenchmarkID))
	if p := report.Provenance; p != nil {
		b.WriteString(fmt.Sprintf("- mode/scale/distribution/tier: %s/%s/%s/%s\n", p.Mode, p.Scale, p.Distribution, p.TierProfile))
		if p.GitSHA != "" {
			b.WriteString(fmt.Sprintf("- git: %s (%s)\n", p.GitSHA, p.GitRef))
		}
	}
	env := report.Metadata.Environment
	b.WriteString(fmt.Sprintf("- environment: %s/%s, %d CPUs", env.GOOS, env.GOARCH, env.NumCPU))
	if env.Hostname != "" {
		b.WriteString(fmt.Sprintf(" (%s)", env.Hostname))
	}
	b.WriteString("\n")
	b.WriteString(fmt.Sprintf("- levels: %s\n", joinLevels(report.Levels)))

	if !report.Comparable {
		b.WriteString("\n**WARNING: runs are not directly comparable**\n")
		for _, w := range report.Warnings {
			b.WriteString(fmt.Sprintf("- %s\n", w))
		}
	}

	b.WriteString("\n## Overall\n\n")
	b.WriteString("| Concurrency | P50 | P95 | P99 | P50Δ | P95Δ | P99Δ | QPS | Executions | Passed |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|---|---|\n")
	for i, row := range report.Summary {
		b.WriteString(fmt.Sprintf("| %d | %s | %s | %s | %s | %s | %s | %.2f | %d | %t |\n",
			row.Concurrency, row.P50, row.P95, row.P99,
			formatDeltaCell(row.P50DeltaVsBase, i == 0),
			formatDeltaCell(row.P95DeltaVsBase, i == 0),
			formatDeltaCell(row.P99DeltaVsBase, i == 0),
			row.QPS, row.ExecutionCount, row.Passed))
	}

	for _, workload := range report.Workloads {
		b.WriteString(fmt.Sprintf("\n## %s\n\n", workload.Name))
		if workload.TargetSchema != "" {
			b.WriteString(fmt.Sprintf("target schema: %s\n\n", workload.TargetSchema))
		}
		b.WriteString("| Concurrency | P50 | P95 | P99 | P50Δ | P95Δ | P99Δ | QPS | Executions | Passed | Stability |\n")
		b.WriteString("|---|---|---|---|---|---|---|---|---|---|---|\n")
		for i, row := range workload.Levels {
			b.WriteString(fmt.Sprintf("| %d | %s | %s | %s | %s | %s | %s | %.2f | %d | %t | %s |\n",
				row.Concurrency, row.P50, row.P95, row.P99,
				formatDeltaCell(row.P50DeltaVsBase, i == 0),
				formatDeltaCell(row.P95DeltaVsBase, i == 0),
				formatDeltaCell(row.P99DeltaVsBase, i == 0),
				row.QPS, row.ExecutionCount, row.Passed,
				formatStabilityCell(row.StabilityAssertions)))
		}
	}

	writeStabilitySection(&b, report)

	return b.String()
}

// writeStabilitySection reports the concurrency stability assertion outcome
// in three distinct states: failures, missing evidence (a C>1 row without
// assertion stats is NOT a pass), and all-passed.
func writeStabilitySection(b *strings.Builder, report ConcurrencyReport) {
	b.WriteString("\n## Concurrency Stability Assertions\n\n")

	unstable := false
	observed := 0
	var missing []string
	for _, workload := range report.Workloads {
		for _, row := range workload.Levels {
			if row.Concurrency <= 1 {
				continue
			}
			if len(row.StabilityAssertions) == 0 {
				missing = append(missing, fmt.Sprintf("%s @ C=%d", workload.Name, row.Concurrency))
				continue
			}
			observed++
			for _, name := range concurrencyStabilityAssertions {
				if stat, ok := row.StabilityAssertions[name]; ok && stat.Failed > 0 {
					b.WriteString(fmt.Sprintf("- FAIL %s @ C=%d: %s %d/%d passed\n",
						workload.Name, row.Concurrency, name, stat.Passed, stat.Passed+stat.Failed))
					unstable = true
				}
			}
		}
	}

	for _, m := range missing {
		b.WriteString(fmt.Sprintf("- MISSING evidence: %s has no concurrency stability assertion stats\n", m))
	}

	switch {
	case observed == 0 && len(missing) == 0:
		b.WriteString("n/a: all runs are sequential (C=1); the assertions only fire at C>=2.\n")
	case !unstable && len(missing) == 0:
		b.WriteString("All concurrency stability assertions passed at every level (C=1 rows are n/a: the assertions only fire at C>=2).\n")
	}
}

// formatDeltaCell renders a signed delta; the baseline row shows "—".
func formatDeltaCell(d time.Duration, isBase bool) string {
	if isBase {
		return "—"
	}
	if d > 0 {
		return "+" + d.String()
	}
	return d.String()
}

func joinLevels(levels []int) string {
	parts := make([]string, 0, len(levels))
	for _, l := range levels {
		parts = append(parts, fmt.Sprintf("C=%d", l))
	}
	return strings.Join(parts, ", ")
}

// formatStabilityCell renders the four assertion pass counts compactly;
// "n/a" for C=1 rows where the assertions never fire.
func formatStabilityCell(stats map[string]AssertionStat) string {
	if len(stats) == 0 {
		return "n/a"
	}
	parts := make([]string, 0, len(concurrencyStabilityAssertions))
	for _, name := range concurrencyStabilityAssertions {
		stat, ok := stats[name]
		if !ok {
			continue
		}
		short := strings.TrimSuffix(strings.TrimPrefix(name, "concurrent-run-"), "-stable")
		parts = append(parts, fmt.Sprintf("%s %d/%d", short, stat.Passed, stat.Passed+stat.Failed))
	}
	return strings.Join(parts, ", ")
}

// WriteConcurrencyReport writes the JSON and/or markdown artifacts (empty
// path skips that artifact).
func WriteConcurrencyReport(jsonPath, mdPath string, report ConcurrencyReport) error {
	if jsonPath != "" {
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return fmt.Errorf("marshal concurrency report: %w", err)
		}
		if err := os.WriteFile(jsonPath, data, 0o644); err != nil {
			return fmt.Errorf("write concurrency report json: %w", err)
		}
	}
	if mdPath != "" {
		if err := os.WriteFile(mdPath, []byte(FormatConcurrencyMarkdown(report)), 0o644); err != nil {
			return fmt.Errorf("write concurrency report markdown: %w", err)
		}
	}
	return nil
}
