package benchmark

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriteReports(t *testing.T) {
	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "report.json")
	mdPath := filepath.Join(dir, "report.md")
	result := &RunResult{
		Generator: GeneratorConfig{Scale: ScaleSmall, Distribution: DistributionUniform},
		Executions: []WorkloadRunResult{{
			Name:         "baseline-page-1",
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
		Generator: GeneratorConfig{Scale: ScaleSmall, Distribution: DistributionUniform},
		Executions: []WorkloadRunResult{
			{Name: "q1", Duration: 10 * time.Millisecond, Assertions: []AssertionResult{{Name: "a", Passed: true}}},
			{Name: "q2", Duration: 30 * time.Millisecond, Assertions: []AssertionResult{{Name: "a", Passed: false}}},
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
	if summary.AssertionStats["a"].Passed != 1 || summary.AssertionStats["a"].Failed != 1 {
		t.Fatalf("unexpected assertion stats: %+v", summary.AssertionStats["a"])
	}
}
