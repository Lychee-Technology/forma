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
