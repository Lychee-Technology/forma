package benchmark

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAnalyzeTrend(t *testing.T) {
	dir := t.TempDir()
	makeSummary := func(id string, ts time.Time, p95 time.Duration, qps float64, correctnessFailures int, passed bool) {
		subDir := filepath.Join(dir, "run-"+id)
		_ = os.MkdirAll(subDir, 0o755)
		summary := SummaryReport{
			Metadata: ArtifactMetadata{BenchmarkID: id},
			Provenance: &RunProvenance{
				StartedAt:    ts,
				CompletedAt:  ts.Add(5 * time.Second),
				Channel:      "ci",
				Mode:         string(ExecutionModeLive),
				Scale:        string(ScaleSmall),
				Distribution: string(DistributionUniform),
				TierProfile:  DefaultTierMixProfile().Name,
			},
			Passed: passed,
			Workloads: []WorkloadSummary{
				{
					Name:                "baseline-page-1",
					TargetSchema:        "trade",
					P95:                 p95,
					QPS:                 qps,
					CorrectnessFailures: correctnessFailures,
					Passed:              passed,
				},
			},
		}
		data, _ := json.MarshalIndent(summary, "", "  ")
		_ = os.WriteFile(filepath.Join(subDir, "benchmark-summary.json"), data, 0o644)
	}
	ts1 := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	ts2 := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	ts3 := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	ts4 := time.Date(2026, 5, 4, 10, 0, 0, 0, time.UTC)
	ts5 := time.Date(2026, 5, 5, 10, 0, 0, 0, time.UTC)
	ts6 := time.Date(2026, 5, 6, 10, 0, 0, 0, time.UTC)
	makeSummary("bench-1", ts1, 100*time.Millisecond, 10, 0, true)
	makeSummary("bench-2", ts2, 105*time.Millisecond, 9.8, 0, true)
	makeSummary("bench-3", ts3, 102*time.Millisecond, 10.1, 0, true)
	makeSummary("bench-4", ts4, 108*time.Millisecond, 9.9, 0, true)
	makeSummary("bench-5", ts5, 104*time.Millisecond, 10.0, 0, true)
	makeSummary("bench-6", ts6, 100*time.Millisecond, 10, 0, true)
	protected := []string{"baseline-page-1"}
	report, err := AnalyzeTrend(dir, "", 5, 0, protected)
	if err != nil {
		t.Fatalf("AnalyzeTrend failed: %v", err)
	}
	if report.Status != TrendStatusPass {
		t.Fatalf("expected pass status, got %q", report.Status)
	}
	if len(report.BaselineWindow) != 5 {
		t.Fatalf("expected 5 baseline runs, got %d", len(report.BaselineWindow))
	}
	if len(report.WorkloadTrends) != 1 {
		t.Fatalf("expected 1 workload trend, got %d", len(report.WorkloadTrends))
	}
}

func TestAnalyzeTrendWithCorrectnessRegression(t *testing.T) {
	dir := t.TempDir()
	makeSummary := func(id string, ts time.Time, correctnessFailures int, passed bool) {
		subDir := filepath.Join(dir, "run-"+id)
		_ = os.MkdirAll(subDir, 0o755)
		summary := SummaryReport{
			Metadata: ArtifactMetadata{BenchmarkID: id},
			Provenance: &RunProvenance{
				StartedAt:    ts,
				CompletedAt:  ts.Add(5 * time.Second),
				Mode:         string(ExecutionModeLive),
				Scale:        string(ScaleSmall),
				Distribution: string(DistributionUniform),
				TierProfile:  DefaultTierMixProfile().Name,
			},
			Passed: passed,
			Workloads: []WorkloadSummary{
				{
					Name:                "baseline-page-1",
					TargetSchema:        "trade",
					P95:                 100 * time.Millisecond,
					QPS:                 10,
					CorrectnessFailures: correctnessFailures,
					Passed:              passed,
				},
			},
		}
		data, _ := json.MarshalIndent(summary, "", "  ")
		_ = os.WriteFile(filepath.Join(subDir, "benchmark-summary.json"), data, 0o644)
	}
	ts1 := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	ts2 := time.Date(2026, 5, 6, 10, 0, 0, 0, time.UTC)
	makeSummary("bench-1", ts1, 0, true)
	makeSummary("bench-2", ts2, 1, false)
	protected := []string{"baseline-page-1"}
	report, err := AnalyzeTrend(dir, "", 5, 0, protected)
	if err != nil {
		t.Fatalf("AnalyzeTrend failed: %v", err)
	}
	if report.Status != TrendStatusHardStopRegression {
		t.Fatalf("expected hard stop status, got %q", report.Status)
	}
	if len(report.Regressions) < 1 {
		t.Fatalf("expected at least 1 regression")
	}
}

func TestAnalyzeTrendWithMethodologyChange(t *testing.T) {
	dir := t.TempDir()
	makeSummary := func(id string, ts time.Time, oracleMode string) {
		subDir := filepath.Join(dir, "run-"+id)
		_ = os.MkdirAll(subDir, 0o755)
		summary := SummaryReport{
			Metadata:    ArtifactMetadata{BenchmarkID: id},
			OracleModes: map[string]string{"q1": oracleMode},
			Provenance: &RunProvenance{
				StartedAt:    ts,
				CompletedAt:  ts.Add(5 * time.Second),
				Mode:         string(ExecutionModeLive),
				Scale:        string(ScaleSmall),
				Distribution: string(DistributionUniform),
				TierProfile:  DefaultTierMixProfile().Name,
			},
			Passed: true,
			Workloads: []WorkloadSummary{
				{Name: "q1", P95: 100 * time.Millisecond, QPS: 10, Passed: true},
			},
		}
		data, _ := json.MarshalIndent(summary, "", "  ")
		_ = os.WriteFile(filepath.Join(subDir, "benchmark-summary.json"), data, 0o644)
	}
	ts1 := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	ts2 := time.Date(2026, 5, 6, 10, 0, 0, 0, time.UTC)
	makeSummary("bench-1", ts1, string(OracleModeLoadedState))
	makeSummary("bench-2", ts2, string(OracleModeTruthPass))
	protected := []string{"q1"}
	report, err := AnalyzeTrend(dir, "", 5, 0, protected)
	if err != nil {
		t.Fatalf("AnalyzeTrend failed: %v", err)
	}
	if report.Status != TrendStatusMethodology {
		t.Fatalf("expected methodology status, got %q", report.Status)
	}
	if len(report.MethodologyChanges) == 0 {
		t.Fatalf("expected methodology changes")
	}
}

func TestFormatTrendSummary(t *testing.T) {
	report := TrendReport{
		Candidate: TrendRun{
			Summary: SummaryReport{
				Metadata:   ArtifactMetadata{BenchmarkID: "bench-cand"},
				Provenance: &RunProvenance{Mode: string(ExecutionModeLive), Scale: string(ScaleSmall)},
			},
		},
		BaselineWindow:     make([]TrendRun, 5),
		ComparabilityKey:   "live|small|uniform|balanced|q1",
		Status:             TrendStatusPass,
		ProtectedWorkloads: []string{"q1"},
		WorkloadTrends: []WorkloadTrend{
			{
				Name:           "q1",
				TargetSchema:   "trade",
				BaselineP95:    100 * time.Millisecond,
				CandidateP95:   105 * time.Millisecond,
				P95Delta:       5 * time.Millisecond,
				Classification: TrendClassStable,
			},
		},
	}
	formatted := FormatTrendSummary(report)
	if !strings.Contains(formatted, "Benchmark Trend Analysis") {
		t.Fatalf("expected trend header: %s", formatted)
	}
	if !strings.Contains(formatted, "status=pass") {
		t.Fatalf("expected pass status: %s", formatted)
	}
	if !strings.Contains(formatted, "q1") {
		t.Fatalf("expected workload name: %s", formatted)
	}
}

func TestWriteAndReadTrendReport(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "trend.json")
	report := &TrendReport{
		Candidate: TrendRun{
			Summary: SummaryReport{
				Metadata: ArtifactMetadata{BenchmarkID: "bench-cand"},
			},
		},
		BaselineWindow:     make([]TrendRun, 3),
		ComparabilityKey:   "test-key",
		Status:             TrendStatusPass,
		ProtectedWorkloads: []string{"q1"},
	}
	if err := WriteTrendReport(path, report); err != nil {
		t.Fatalf("WriteTrendReport failed: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected trend report to exist: %v", err)
	}
}

func TestWriteTrendReportNil(t *testing.T) {
	err := WriteTrendReport("/dev/null", nil)
	if err == nil {
		t.Fatalf("expected error for nil report")
	}
}

func TestIsWorkloadInstabilityChanged(t *testing.T) {
	base := WorkloadSummary{
		AssertionStats: map[string]AssertionStat{
			"repeated-run-failure-kind-stable": {Passed: 0, Failed: 1},
		},
	}
	cand := WorkloadSummary{
		AssertionStats: map[string]AssertionStat{
			"repeated-run-failure-kind-stable": {Passed: 1, Failed: 0},
		},
	}
	if !isWorkloadInstabilityChanged(base, cand) {
		t.Fatalf("expected instability change to be detected")
	}
}

func TestMedianP95FromTrendRuns(t *testing.T) {
	runs := []TrendRun{
		{Summary: SummaryReport{Workloads: []WorkloadSummary{{P95: 100 * time.Millisecond}, {P95: 200 * time.Millisecond}}}},
		{Summary: SummaryReport{Workloads: []WorkloadSummary{{P95: 300 * time.Millisecond}}}},
	}
	med := medianP95FromTrendRuns(runs)
	if med != 200*time.Millisecond {
		t.Fatalf("expected median 200ms, got %s", med)
	}
	med2 := medianP95FromTrendRuns(nil)
	if med2 != 0 {
		t.Fatalf("expected 0 for nil, got %s", med2)
	}
}

func TestReadTrendHistorySkipsInvalid(t *testing.T) {
	dir := t.TempDir()
	_ = os.WriteFile(filepath.Join(dir, "benchmark-summary.json"), []byte("not json"), 0o644)
	runs, err := ReadTrendHistory(dir)
	if err != nil {
		t.Fatalf("ReadTrendHistory should not error on invalid files: %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("expected 0 runs for invalid json, got %d", len(runs))
	}
}

func TestReadTrendHistorySkipsNoTimestamp(t *testing.T) {
	dir := t.TempDir()
	summary := SummaryReport{
		Metadata:   ArtifactMetadata{BenchmarkID: "bench"},
		Provenance: &RunProvenance{Mode: string(ExecutionModeLive)},
		Passed:     true,
	}
	data, _ := json.MarshalIndent(summary, "", "  ")
	_ = os.WriteFile(filepath.Join(dir, "benchmark-summary.json"), data, 0o644)
	runs, err := ReadTrendHistory(dir)
	if err != nil {
		t.Fatalf("ReadTrendHistory failed: %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("expected 0 runs for missing timestamp, got %d", len(runs))
	}
}
