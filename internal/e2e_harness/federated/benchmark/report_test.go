package benchmark

import (
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
}

func TestWriteBaselineCaptureAndSummary(t *testing.T) {
	dir := t.TempDir()
	result := &RunResult{
		Passed:       false,
		FailureCount: 2,
		Generator:    GeneratorConfig{Scale: ScaleSmall, Distribution: DistributionUniform},
		Executions: []WorkloadRunResult{
			{Name: "q1", Passed: true, Duration: 10 * time.Millisecond, Assertions: []AssertionResult{{Name: "a", Passed: true}}},
			{Name: "q2", Passed: false, FailureCount: 1, Duration: 30 * time.Millisecond, Assertions: []AssertionResult{{Name: "a", Passed: false}}},
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
}

func TestFormatConsoleSummary(t *testing.T) {
	result := &RunResult{
		Passed:       false,
		FailureCount: 1,
		Generator:    GeneratorConfig{Scale: ScaleSmall, Distribution: DistributionUniform},
		Executions: []WorkloadRunResult{{
			Name:         "q1",
			Passed:       false,
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
}
