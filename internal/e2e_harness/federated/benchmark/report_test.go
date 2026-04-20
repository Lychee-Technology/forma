package benchmark

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWriteReports(t *testing.T) {
	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "report.json")
	mdPath := filepath.Join(dir, "report.md")
	result := &RunResult{
		Passed:    true,
		Generator: GeneratorConfig{Scale: ScaleSmall, Distribution: DistributionUniform},
		Executions: []WorkloadRunResult{{
			Name:         "baseline-page-1",
			Passed:       true,
			ResultCount:  20,
			TotalRecords: 100,
			Duration:     10 * time.Millisecond,
		}},
	}
	if err := WriteJSONReport(jsonPath, result); err != nil {
		t.Fatalf("WriteJSONReport failed: %v", err)
	}
	if err := WriteMarkdownReport(mdPath, result); err != nil {
		t.Fatalf("WriteMarkdownReport failed: %v", err)
	}
	if _, err := os.Stat(jsonPath); err != nil {
		t.Fatalf("expected JSON report to exist: %v", err)
	}
	if _, err := os.Stat(mdPath); err != nil {
		t.Fatalf("expected markdown report to exist: %v", err)
	}
	summary := SummarizeRunResult(result)
	if summary.Metadata.BenchmarkID == "" {
		t.Fatalf("expected summary to carry metadata")
	}
}

func TestWriteBaselineCaptureAndSummary(t *testing.T) {
	dir := t.TempDir()
	result := &RunResult{
		Passed:       false,
		FailureCount: 2,
		Generator:    GeneratorConfig{Scale: ScaleSmall, Distribution: DistributionUniform},
		Workloads: []WorkloadDefinition{
			{Name: "q1", Category: WorkloadCategoryPagination, TargetSchema: "trade", OracleMode: OracleModeLoadedState},
			{Name: "q2", Category: WorkloadCategoryFilter, TargetSchema: "trade", OracleMode: OracleModeTruthPass, PreferHot: true},
		},
		OracleModes: map[string]string{"q1": string(OracleModeLoadedState), "q2": string(OracleModeTruthPass)},
		Executions: []WorkloadRunResult{
			{Name: "q1", Passed: true, OracleMode: string(OracleModeLoadedState), Duration: 10 * time.Millisecond, Assertions: []AssertionResult{{Name: "a", Passed: true}}},
			{Name: "q2", Passed: false, PreferHot: true, OracleMode: string(OracleModeTruthPass), FailureKind: FailureKindCorrectness, FailureCount: 1, Duration: 30 * time.Millisecond, Assertions: []AssertionResult{{Name: "a", Passed: false}, {Name: "repeated-run-total-records-stable", Passed: false}}},
		},
	}
	if err := WriteBaselineCapture(dir, result); err != nil {
		t.Fatalf("WriteBaselineCapture failed: %v", err)
	}
	for _, name := range []string{"benchmark-result.json", "benchmark-result.md", "benchmark-summary.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("expected %s to exist: %v", name, err)
		}
	}
	summary := SummarizeRunResult(result)
	if summary.ExecutionCount != 2 {
		t.Fatalf("unexpected execution count: %d", summary.ExecutionCount)
	}
	if summary.Avg <= 0 || summary.QPS <= 0 {
		t.Fatalf("expected avg and qps to be populated: %+v", summary)
	}
	if summary.AssertionStats["a"].Passed != 1 || summary.AssertionStats["a"].Failed != 1 {
		t.Fatalf("unexpected assertion stats: %+v", summary.AssertionStats["a"])
	}
	if summary.Passed {
		t.Fatalf("expected summary to be marked failed")
	}
	if summary.FailureCount != 2 {
		t.Fatalf("expected summary failure count 2, got %d", summary.FailureCount)
	}
	if summary.CorrectnessFailures != 1 {
		t.Fatalf("expected one correctness failure, got %d", summary.CorrectnessFailures)
	}
	if summary.Metadata.BenchmarkID == "" {
		t.Fatalf("expected summary metadata to include benchmark ID")
	}
	if summary.OracleModes["q2"] != string(OracleModeTruthPass) {
		t.Fatalf("expected summary to expose oracle modes, got %+v", summary.OracleModes)
	}
	if !summary.Stability.Enabled {
		t.Fatalf("expected summary to expose repeated-run stability state")
	}
	if summary.Stability.WorkloadsChecked != 1 {
		t.Fatalf("expected one workload with repeated-run checks, got %+v", summary.Stability)
	}
	if summary.Stability.TotalRecordsFailures != 1 {
		t.Fatalf("expected one repeated-run total-record failure, got %+v", summary.Stability)
	}
	if len(summary.Stability.UnstableWorkloads) != 1 || summary.Stability.UnstableWorkloads[0] != "q2" {
		t.Fatalf("expected q2 to be marked unstable, got %+v", summary.Stability)
	}
	if len(summary.OracleProvenance) != 2 {
		t.Fatalf("expected grouped oracle provenance, got %+v", summary.OracleProvenance)
	}
	if !summary.Workloads[1].PreferHot {
		t.Fatalf("expected workload summary to expose prefer_hot intent, got %+v", summary.Workloads[1])
	}
	if len(summary.Workloads) != 2 {
		t.Fatalf("expected two workload summaries, got %d", len(summary.Workloads))
	}
}

func TestFormatConsoleSummary(t *testing.T) {
	result := &RunResult{
		Passed:       false,
		FailureCount: 1,
		Generator:    GeneratorConfig{Scale: ScaleSmall, Distribution: DistributionUniform},
		Executions: []WorkloadRunResult{{
			Name:         "q1",
			Passed:       false,
			OracleMode:   string(OracleModeTruthPass),
			FailureKind:  FailureKindCorrectness,
			FailureCount: 1,
			Duration:     10 * time.Millisecond,
			Assertions:   []AssertionResult{{Name: "a", Passed: true}},
		}},
	}
	formatted := FormatConsoleSummary(result)
	if formatted == "" {
		t.Fatalf("expected non-empty console summary")
	}
	if !strings.Contains(formatted, "Benchmark Summary") {
		t.Fatalf("expected benchmark header in summary: %s", formatted)
	}
	if !strings.Contains(formatted, "passed=false") {
		t.Fatalf("expected pass state in summary: %s", formatted)
	}
	if !strings.Contains(formatted, "benchmark_id=") {
		t.Fatalf("expected benchmark id in summary: %s", formatted)
	}
	if !strings.Contains(formatted, "correctness_failures=1") {
		t.Fatalf("expected correctness failures in summary: %s", formatted)
	}
	if !strings.Contains(formatted, "stability enabled=") {
		t.Fatalf("expected stability line in summary: %s", formatted)
	}
	if !strings.Contains(formatted, "oracle_provenance") {
		t.Fatalf("expected oracle provenance line in summary: %s", formatted)
	}
}

func TestSummarizeWorkloadsCarriesOracleMode(t *testing.T) {
	result := &RunResult{Executions: []WorkloadRunResult{{Name: "q1", PreferHot: true, OracleMode: string(OracleModeTruthPass), Duration: 10 * time.Millisecond, Passed: true}}}
	workloads := summarizeWorkloads(result)
	if len(workloads) != 1 || workloads[0].OracleMode != string(OracleModeTruthPass) {
		t.Fatalf("expected workload summary to carry oracle mode, got %+v", workloads)
	}
	if !workloads[0].PreferHot {
		t.Fatalf("expected workload summary to carry prefer_hot, got %+v", workloads)
	}
}

func TestSummarizeRunResultBuildsOracleProvenanceAndStability(t *testing.T) {
	result := &RunResult{
		Workloads: []WorkloadDefinition{
			{Name: "baseline-page-1", Category: WorkloadCategoryPagination, TargetSchema: "trade", OracleMode: OracleModeLoadedState},
			{Name: "hot-selective-page", Category: WorkloadCategoryFilter, TargetSchema: "trade", OracleMode: OracleModeTruthPass},
		},
		Executions: []WorkloadRunResult{
			{
				Name:       "baseline-page-1",
				Passed:     true,
				Duration:   10 * time.Millisecond,
				OracleMode: string(OracleModeLoadedState),
				Assertions: []AssertionResult{{Name: "repeated-run-failure-kind-stable", Passed: true}},
			},
			{
				Name:       "hot-selective-page",
				Passed:     false,
				Duration:   15 * time.Millisecond,
				OracleMode: string(OracleModeTruthPass),
				Assertions: []AssertionResult{{Name: "repeated-run-page-row-ids-stable", Passed: false}},
			},
		},
	}
	summary := SummarizeRunResult(result)
	if len(summary.OracleProvenance) != 2 {
		t.Fatalf("expected two oracle provenance groups, got %+v", summary.OracleProvenance)
	}
	if !summary.Stability.Enabled || summary.Stability.WorkloadsChecked != 2 {
		t.Fatalf("expected stability to be enabled for both workloads, got %+v", summary.Stability)
	}
	if len(summary.Stability.UnstableWorkloads) != 1 || summary.Stability.UnstableWorkloads[0] != "hot-selective-page" {
		t.Fatalf("expected unstable workload to be tracked, got %+v", summary.Stability)
	}
	if summary.Stability.PageRowIDFailures != 1 {
		t.Fatalf("expected one page-row-id stability failure, got %+v", summary.Stability)
	}
}

func TestCompareSummaryReports(t *testing.T) {
	baseline := SummaryReport{
		Metadata:            ArtifactMetadata{BenchmarkID: "bench-a"},
		Passed:              true,
		ExecutionCount:      2,
		CorrectnessFailures: 0,
		QPS:                 10,
		Avg:                 10 * time.Millisecond,
		P95:                 20 * time.Millisecond,
		Workloads: []WorkloadSummary{{
			Name:                "q1",
			TargetSchema:        "trade",
			Passed:              true,
			ExecutionCount:      2,
			CorrectnessFailures: 0,
			QPS:                 5,
			Avg:                 10 * time.Millisecond,
			P95:                 20 * time.Millisecond,
			AvgResultCount:      20,
			AvgTotalRecords:     100,
		}},
	}
	candidate := SummaryReport{
		Metadata:            ArtifactMetadata{BenchmarkID: "bench-b"},
		Passed:              false,
		FailureCount:        1,
		ExecutionCount:      2,
		CorrectnessFailures: 1,
		QPS:                 8,
		Avg:                 12 * time.Millisecond,
		P95:                 25 * time.Millisecond,
		Workloads: []WorkloadSummary{{
			Name:                "q1",
			TargetSchema:        "trade",
			Passed:              false,
			FailureCount:        1,
			ExecutionCount:      2,
			CorrectnessFailures: 1,
			QPS:                 4,
			Avg:                 12 * time.Millisecond,
			P95:                 25 * time.Millisecond,
			AvgResultCount:      18,
			AvgTotalRecords:     95,
		}},
	}
	diff := CompareSummaryReports(baseline, candidate)
	if !diff.Summary.PassedChanged {
		t.Fatalf("expected passed state change in diff")
	}
	if len(diff.Workloads) != 1 {
		t.Fatalf("expected one workload diff, got %d", len(diff.Workloads))
	}
	if diff.Workloads[0].AvgResultCountDelta >= 0 {
		t.Fatalf("expected avg result count delta to be negative: %+v", diff.Workloads[0])
	}
	if diff.Summary.CorrectnessFailuresDelta != 1 {
		t.Fatalf("expected correctness failure delta of 1, got %+v", diff.Summary)
	}
	formatted := FormatDiffSummary(diff)
	if !strings.Contains(formatted, "Benchmark Diff Summary") {
		t.Fatalf("expected diff summary header: %s", formatted)
	}
}

func TestWriteAndReadDiffReport(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "diff.json")
	summaryPath := filepath.Join(dir, "summary.json")
	diff := &DiffReport{
		BaselineMetadata:  ArtifactMetadata{BenchmarkID: "bench-a"},
		CandidateMetadata: ArtifactMetadata{BenchmarkID: "bench-b"},
		Summary:           SummaryDiff{QPSDelta: -2},
	}
	if err := WriteDiffReport(path, diff); err != nil {
		t.Fatalf("WriteDiffReport failed: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected diff report to exist: %v", err)
	}
	summary := SummaryReport{Metadata: ArtifactMetadata{BenchmarkID: "bench-summary"}, Passed: true}
	data, err := json.Marshal(summary)
	if err != nil {
		t.Fatalf("marshal summary fixture failed: %v", err)
	}
	if err := os.WriteFile(summaryPath, data, 0o644); err != nil {
		t.Fatalf("write summary fixture failed: %v", err)
	}
	readSummary, err := ReadSummaryReport(summaryPath)
	if err != nil {
		t.Fatalf("ReadSummaryReport failed: %v", err)
	}
	if readSummary.Metadata.BenchmarkID != summary.Metadata.BenchmarkID {
		t.Fatalf("unexpected summary metadata: %+v", readSummary.Metadata)
	}
}
