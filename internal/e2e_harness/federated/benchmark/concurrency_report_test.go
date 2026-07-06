package benchmark

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func concurrencySummaryFixture(concurrency int, p99 time.Duration) SummaryReport {
	stats := map[string]AssertionStat{}
	if concurrency > 1 {
		stats = map[string]AssertionStat{
			"concurrent-run-total-records-stable": {Passed: 2},
			"concurrent-run-page-row-ids-stable":  {Passed: 2},
			"concurrent-run-failure-kind-stable":  {Passed: 2},
			"concurrent-run-route-engine-stable":  {Passed: 1, Failed: 1},
			"result-count-matches":                {Passed: 4},
		}
	}
	return SummaryReport{
		Metadata: ArtifactMetadata{BenchmarkID: "bench", Environment: EnvironmentMetadata{NumCPU: 8}},
		Provenance: &RunProvenance{
			Mode:         string(ExecutionModeLive),
			Scale:        string(ScaleSmall),
			Distribution: string(DistributionUniform),
			TierProfile:  DefaultTierMixProfile().Name,
			Concurrency:  concurrency,
		},
		Passed:         true,
		ExecutionCount: 2 * concurrency,
		P50:            10 * time.Millisecond,
		P95:            p99 / 2,
		P99:            p99,
		QPS:            100 / float64(concurrency),
		AssertionStats: stats,
		Workloads: []WorkloadSummary{{
			Name:           "baseline-page-1",
			TargetSchema:   "trade",
			Passed:         true,
			ExecutionCount: 2 * concurrency,
			P50:            10 * time.Millisecond,
			P95:            p99 / 2,
			P99:            p99,
			QPS:            50 / float64(concurrency),
			AssertionStats: stats,
		}},
	}
}

func TestBuildConcurrencyReport(t *testing.T) {
	summaries := []SummaryReport{
		concurrencySummaryFixture(4, 80*time.Millisecond),
		concurrencySummaryFixture(1, 20*time.Millisecond),
		concurrencySummaryFixture(2, 40*time.Millisecond),
	}

	report, err := BuildConcurrencyReport(summaries)
	if err != nil {
		t.Fatalf("build concurrency report: %v", err)
	}

	if len(report.Levels) != 3 || report.Levels[0] != 1 || report.Levels[1] != 2 || report.Levels[2] != 4 {
		t.Fatalf("expected ascending levels [1 2 4], got %v", report.Levels)
	}
	if !report.Comparable || len(report.Warnings) != 0 {
		t.Fatalf("expected comparable runs without warnings, got comparable=%t warnings=%v", report.Comparable, report.Warnings)
	}
	if len(report.Summary) != 3 || report.Summary[2].Concurrency != 4 || report.Summary[2].P99 != 80*time.Millisecond {
		t.Fatalf("unexpected summary rows: %+v", report.Summary)
	}
	if len(report.Workloads) != 1 || report.Workloads[0].Name != "baseline-page-1" {
		t.Fatalf("expected one workload trend, got %+v", report.Workloads)
	}
	levels := report.Workloads[0].Levels
	if len(levels) != 3 || levels[2].Concurrency != 4 || levels[2].P99 != 80*time.Millisecond {
		t.Fatalf("unexpected workload levels: %+v", levels)
	}
	// C=1 rows must not carry concurrency stability assertions (they never fire).
	if len(levels[0].StabilityAssertions) != 0 {
		t.Fatalf("expected no stability assertions at C=1, got %v", levels[0].StabilityAssertions)
	}
	// Only the four concurrent-run-* assertions are stability assertions.
	stats := levels[2].StabilityAssertions
	if len(stats) != 4 {
		t.Fatalf("expected exactly four stability assertions at C=4, got %v", stats)
	}
	if stats["concurrent-run-route-engine-stable"].Failed != 1 {
		t.Fatalf("expected route-engine stability failure to surface, got %v", stats)
	}
}

func TestBuildConcurrencyReportRejectsDuplicateLevels(t *testing.T) {
	summaries := []SummaryReport{
		concurrencySummaryFixture(2, 40*time.Millisecond),
		concurrencySummaryFixture(2, 44*time.Millisecond),
	}
	if _, err := BuildConcurrencyReport(summaries); err == nil {
		t.Fatalf("expected duplicate concurrency levels to be rejected")
	}
}

func TestBuildConcurrencyReportMissingConcurrencyDefaultsToOne(t *testing.T) {
	legacy := concurrencySummaryFixture(1, 20*time.Millisecond)
	legacy.Provenance.Concurrency = 0

	report, err := BuildConcurrencyReport([]SummaryReport{legacy, concurrencySummaryFixture(2, 40*time.Millisecond)})
	if err != nil {
		t.Fatalf("build concurrency report: %v", err)
	}
	if len(report.Levels) != 2 || report.Levels[0] != 1 {
		t.Fatalf("expected missing concurrency to default to level 1, got %v", report.Levels)
	}
}

func TestBuildConcurrencyReportFlagsIncomparableRuns(t *testing.T) {
	other := concurrencySummaryFixture(2, 40*time.Millisecond)
	other.Provenance.Scale = string(ScaleMedium)

	report, err := BuildConcurrencyReport([]SummaryReport{concurrencySummaryFixture(1, 20*time.Millisecond), other})
	if err != nil {
		t.Fatalf("build concurrency report: %v", err)
	}
	if report.Comparable {
		t.Fatalf("expected mismatched scale to mark the report incomparable")
	}
	if len(report.Warnings) == 0 {
		t.Fatalf("expected warnings for incomparable runs")
	}
}

// TestBuildConcurrencyReportComputesDeltasVsLowestLevel: the original #104
// acceptance wording asks for p50/p95/p99 *deltas*; each row carries its
// delta against the lowest level present (zero on the baseline row itself).
func TestBuildConcurrencyReportComputesDeltasVsLowestLevel(t *testing.T) {
	report, err := BuildConcurrencyReport([]SummaryReport{
		concurrencySummaryFixture(1, 20*time.Millisecond),
		concurrencySummaryFixture(2, 40*time.Millisecond),
		concurrencySummaryFixture(4, 80*time.Millisecond),
	})
	if err != nil {
		t.Fatalf("build concurrency report: %v", err)
	}

	if report.Summary[0].P99DeltaVsBase != 0 {
		t.Fatalf("expected zero delta on the baseline row, got %s", report.Summary[0].P99DeltaVsBase)
	}
	if report.Summary[2].P99DeltaVsBase != 60*time.Millisecond {
		t.Fatalf("expected overall P99 delta of 60ms at C=4, got %s", report.Summary[2].P99DeltaVsBase)
	}
	levels := report.Workloads[0].Levels
	if levels[1].P99DeltaVsBase != 20*time.Millisecond || levels[2].P99DeltaVsBase != 60*time.Millisecond {
		t.Fatalf("expected workload P99 deltas of 20ms/60ms, got %s/%s", levels[1].P99DeltaVsBase, levels[2].P99DeltaVsBase)
	}
}

// TestFormatConcurrencyMarkdownStabilityEvidenceStates: missing stability
// evidence must be distinct from passed evidence.
func TestFormatConcurrencyMarkdownStabilityEvidenceStates(t *testing.T) {
	t.Run("all sequential runs report n/a not passed", func(t *testing.T) {
		report, err := BuildConcurrencyReport([]SummaryReport{concurrencySummaryFixture(1, 20*time.Millisecond)})
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		md := FormatConcurrencyMarkdown(report)
		if strings.Contains(md, "All concurrency stability assertions passed") {
			t.Fatalf("all-sequential report must not claim assertions passed:\n%s", md)
		}
		if !strings.Contains(md, "sequential") {
			t.Fatalf("expected sequential n/a note:\n%s", md)
		}
	})

	t.Run("missing stats at C>1 reported as missing evidence", func(t *testing.T) {
		noStats := concurrencySummaryFixture(4, 80*time.Millisecond)
		noStats.AssertionStats = nil
		noStats.Workloads[0].AssertionStats = nil

		report, err := BuildConcurrencyReport([]SummaryReport{
			concurrencySummaryFixture(1, 20*time.Millisecond),
			noStats,
		})
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		md := FormatConcurrencyMarkdown(report)
		if strings.Contains(md, "All concurrency stability assertions passed") {
			t.Fatalf("missing evidence must not be reported as passed:\n%s", md)
		}
		if !strings.Contains(md, "MISSING") {
			t.Fatalf("expected missing-evidence marker:\n%s", md)
		}
	})
}

// TestCollectConcurrencySummaries: the evidence collector must fail loudly on
// malformed inputs (unlike trend, which tolerates them) and must include
// provenance-less summaries (counted as C=1).
func TestCollectConcurrencySummaries(t *testing.T) {
	writeSummary := func(t *testing.T, dir, sub string, summary SummaryReport) {
		t.Helper()
		full := filepath.Join(dir, sub)
		if err := os.MkdirAll(full, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		data, err := json.Marshal(summary)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if err := os.WriteFile(filepath.Join(full, "benchmark-summary.json"), data, 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	t.Run("includes provenance-less summaries", func(t *testing.T) {
		dir := t.TempDir()
		writeSummary(t, dir, "legacy", SummaryReport{Metadata: ArtifactMetadata{BenchmarkID: "legacy"}})
		writeSummary(t, dir, "c2", concurrencySummaryFixture(2, 40*time.Millisecond))

		summaries, err := CollectConcurrencySummaries(dir)
		if err != nil {
			t.Fatalf("collect: %v", err)
		}
		if len(summaries) != 2 {
			t.Fatalf("expected both summaries collected, got %d", len(summaries))
		}
	})

	t.Run("fails loudly on malformed summary", func(t *testing.T) {
		dir := t.TempDir()
		sub := filepath.Join(dir, "broken")
		if err := os.MkdirAll(sub, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(sub, "benchmark-summary.json"), []byte("{not json"), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
		if _, err := CollectConcurrencySummaries(dir); err == nil {
			t.Fatalf("expected malformed summary to fail collection")
		}
	})
}

func TestFormatConcurrencyMarkdown(t *testing.T) {
	report, err := BuildConcurrencyReport([]SummaryReport{
		concurrencySummaryFixture(1, 20*time.Millisecond),
		concurrencySummaryFixture(2, 40*time.Millisecond),
		concurrencySummaryFixture(4, 80*time.Millisecond),
	})
	if err != nil {
		t.Fatalf("build concurrency report: %v", err)
	}

	md := FormatConcurrencyMarkdown(report)
	for _, want := range []string{
		"# Concurrency Benchmark Report",
		"| Concurrency | P50 | P95 | P99 | P50Δ | P95Δ | P99Δ | QPS |",
		"## baseline-page-1",
		"concurrent-run-route-engine-stable",
		"n/a",
		"+60ms",
	} {
		if !strings.Contains(md, want) {
			t.Fatalf("expected markdown to contain %q, got:\n%s", want, md)
		}
	}
}
