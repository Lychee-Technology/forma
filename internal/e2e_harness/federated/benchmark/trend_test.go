package benchmark

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReadTrendHistory(t *testing.T) {
	dir := t.TempDir()
	makeSummary := func(id string, ts time.Time, preset string, mode, scale string, workloads []string) {
		subDir := filepath.Join(dir, "run-"+id)
		_ = os.MkdirAll(subDir, 0o755)
		workloadSummaries := make([]WorkloadSummary, 0, len(workloads))
		for _, name := range workloads {
			workloadSummaries = append(workloadSummaries, WorkloadSummary{
				Name:   name,
				P95:    100 * time.Millisecond,
				QPS:    10,
				Passed: true,
			})
		}
		summary := SummaryReport{
			Metadata: ArtifactMetadata{BenchmarkID: id},
			Provenance: &RunProvenance{
				StartedAt:    ts,
				CompletedAt:  ts.Add(5 * time.Second),
				Preset:       preset,
				Channel:      "ci",
				Mode:         mode,
				Scale:        scale,
				Distribution: string(DistributionUniform),
				TierProfile:  DefaultTierMixProfile().Name,
			},
			Passed:    true,
			Workloads: workloadSummaries,
		}
		data, _ := json.MarshalIndent(summary, "", "  ")
		_ = os.WriteFile(filepath.Join(subDir, "benchmark-summary.json"), data, 0o644)
	}
	ts1 := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	ts2 := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	ts3 := time.Date(2026, 5, 5, 10, 0, 0, 0, time.UTC)
	makeSummary("bench-aaa", ts1, "small-live", string(ExecutionModeLive), string(ScaleSmall), []string{"q1", "q2"})
	makeSummary("bench-bbb", ts2, "small-live", string(ExecutionModeLive), string(ScaleSmall), []string{"q1", "q2"})
	makeSummary("bench-ccc", ts3, "small-live", string(ExecutionModeLive), string(ScaleSmall), []string{"q1", "q2"})
	runs, err := ReadTrendHistory(dir)
	if err != nil {
		t.Fatalf("ReadTrendHistory failed: %v", err)
	}
	if len(runs) != 3 {
		t.Fatalf("expected 3 runs, got %d", len(runs))
	}
	if !runs[0].StartedAt.Before(runs[1].StartedAt) {
		t.Fatalf("expected chronological ordering")
	}
	if runs[0].Summary.Metadata.BenchmarkID != "bench-aaa" {
		t.Fatalf("expected oldest run first, got %s", runs[0].Summary.Metadata.BenchmarkID)
	}
	if runs[2].Summary.Metadata.BenchmarkID != "bench-ccc" {
		t.Fatalf("expected newest run last, got %s", runs[2].Summary.Metadata.BenchmarkID)
	}
}

func TestProvenanceCarriesConcurrency(t *testing.T) {
	baseTime := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)
	result := &RunResult{
		Passed:      true,
		StartedAt:   baseTime,
		CompletedAt: baseTime.Add(10 * time.Second),
		Config:      Config{Mode: ExecutionModeLive, Scale: ScaleSmall, Distribution: DistributionUniform, TierProfile: DefaultTierMixProfile().Name, Concurrency: 4},
		Executions: []WorkloadRunResult{{
			Name:     "q1",
			Passed:   true,
			Duration: 10 * time.Millisecond,
		}},
	}

	summary := SummarizeRunResult(result)
	if summary.Provenance == nil || summary.Provenance.Concurrency != 4 {
		t.Fatalf("expected fallback provenance to carry concurrency 4, got %+v", summary.Provenance)
	}

	result.Provenance = &RunProvenance{Channel: "manual"}
	summary = SummarizeRunResult(result)
	if summary.Provenance == nil || summary.Provenance.Concurrency != 4 {
		t.Fatalf("expected explicit provenance to be backfilled with concurrency 4, got %+v", summary.Provenance)
	}
}

// TestBuildComparabilityIdentity_ConcurrencySegment: runs at concurrency > 1
// must not share a trend baseline window with sequential runs, while C<=1
// keeps the legacy identity so historical artifacts stay comparable.
func TestBuildComparabilityIdentity_ConcurrencySegment(t *testing.T) {
	mk := func(concurrency int) SummaryReport {
		return SummaryReport{
			Metadata: ArtifactMetadata{BenchmarkID: "bench"},
			Provenance: &RunProvenance{
				Mode:         string(ExecutionModeLive),
				Scale:        string(ScaleSmall),
				Distribution: string(DistributionUniform),
				TierProfile:  DefaultTierMixProfile().Name,
				Concurrency:  concurrency,
			},
			Workloads: []WorkloadSummary{{Name: "q1"}},
		}
	}

	legacy := mk(0)
	sequential := mk(1)
	concurrent := mk(4)

	if buildComparabilityIdentity(legacy) != buildComparabilityIdentity(sequential) {
		t.Fatalf("expected C<=1 to keep the legacy comparability key")
	}
	if buildComparabilityIdentity(concurrent) == buildComparabilityIdentity(legacy) {
		t.Fatalf("expected concurrent runs to form their own comparability group")
	}
	if !strings.HasSuffix(buildComparabilityIdentity(concurrent), "|c4") {
		t.Fatalf("expected concurrency segment suffix, got %q", buildComparabilityIdentity(concurrent))
	}
}

// TestReadTrendHistorySkipsNilProvenance: manual/legacy artifacts without
// provenance must be skipped (trend needs timestamps), not panic.
func TestReadTrendHistorySkipsNilProvenance(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "legacy")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	legacy := SummaryReport{Metadata: ArtifactMetadata{BenchmarkID: "legacy"}}
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sub, "benchmark-summary.json"), data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	runs, err := ReadTrendHistory(dir)
	if err != nil {
		t.Fatalf("read trend history: %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("expected provenance-less summary to be skipped, got %d runs", len(runs))
	}
}

func TestBuildComparabilityIdentity(t *testing.T) {
	a := SummaryReport{
		Metadata: ArtifactMetadata{BenchmarkID: "bench-a"},
		Provenance: &RunProvenance{
			Mode:         string(ExecutionModeLive),
			Scale:        string(ScaleSmall),
			Distribution: string(DistributionUniform),
			TierProfile:  DefaultTierMixProfile().Name,
		},
		OracleModes: map[string]string{"q1": string(OracleModeLoadedState)},
		Workloads: []WorkloadSummary{
			{Name: "q1"}, {Name: "q2"},
		},
	}
	b := SummaryReport{
		Metadata: ArtifactMetadata{BenchmarkID: "bench-b"},
		Provenance: &RunProvenance{
			Mode:         string(ExecutionModeLive),
			Scale:        string(ScaleSmall),
			Distribution: string(DistributionUniform),
			TierProfile:  DefaultTierMixProfile().Name,
		},
		OracleModes: map[string]string{"q1": string(OracleModeTruthPass)},
		Workloads: []WorkloadSummary{
			{Name: "q2"}, {Name: "q1"},
		},
	}
	if buildComparabilityIdentity(a) != buildComparabilityIdentity(b) {
		t.Fatalf("expected same comparability key regardless of workload order and oracle mode")
	}
	c := SummaryReport{
		Metadata: ArtifactMetadata{BenchmarkID: "bench-c"},
		Provenance: &RunProvenance{
			Mode:         string(ExecutionModeLive),
			Scale:        string(ScaleMedium),
			Distribution: string(DistributionUniform),
			TierProfile:  DefaultTierMixProfile().Name,
		},
		Workloads: []WorkloadSummary{
			{Name: "q1"}, {Name: "q2"},
		},
	}
	if buildComparabilityIdentity(a) == buildComparabilityIdentity(c) {
		t.Fatalf("expected different comparability keys for different scales")
	}
	d := SummaryReport{
		Metadata:   ArtifactMetadata{BenchmarkID: "bench-d"},
		Provenance: nil,
		Workloads:  []WorkloadSummary{{Name: "q1"}},
	}
	keyD := buildComparabilityIdentity(d)
	if keyD != "bench-d" {
		t.Fatalf("expected benchmark ID as fallback for nil provenance, got %q", keyD)
	}
}

func TestDetectMethodologyChanges(t *testing.T) {
	base := SummaryReport{
		OracleModes: map[string]string{"q1": string(OracleModeLoadedState)},
		Workloads: []WorkloadSummary{
			{Name: "q1"},
		},
	}
	cand := SummaryReport{
		OracleModes: map[string]string{"q1": string(OracleModeTruthPass)},
		Workloads: []WorkloadSummary{
			{Name: "q1"},
		},
	}
	changes := detectMethodologyChanges(base, cand)
	if len(changes) == 0 {
		t.Fatalf("expected oracle mode change to be detected")
	}
	base2 := SummaryReport{
		OracleModes: map[string]string{"q1": string(OracleModeLoadedState)},
		Workloads: []WorkloadSummary{
			{Name: "q1"},
		},
	}
	cand2 := SummaryReport{
		OracleModes: map[string]string{"q1": string(OracleModeLoadedState)},
		Workloads: []WorkloadSummary{
			{Name: "q1"}, {Name: "q2"},
		},
	}
	changes2 := detectMethodologyChanges(base2, cand2)
	if len(changes2) == 0 {
		t.Fatalf("expected new workload addition to be detected")
	}
}

func TestMedianCalculations(t *testing.T) {
	workloads := []WorkloadSummary{
		{P95: 100 * time.Millisecond},
		{P95: 200 * time.Millisecond},
		{P95: 300 * time.Millisecond},
	}
	med := medianP95(workloads)
	if med != 200*time.Millisecond {
		t.Fatalf("expected median 200ms, got %s", med)
	}
	emptyMed := medianP95(nil)
	if emptyMed != 0 {
		t.Fatalf("expected 0 for empty slice, got %s", emptyMed)
	}
}

func TestDetectRegressions(t *testing.T) {
	candidate := SummaryReport{
		Workloads: []WorkloadSummary{
			{Name: "q1", TargetSchema: "trade", CorrectnessFailures: 1, Passed: false},
		},
	}
	trends := []WorkloadTrend{
		{
			Name:               "q1",
			TargetSchema:       "trade",
			CorrectnessChanged: true,
			CorrectnessDelta:   1,
			Classification:     TrendClassRegression,
		},
	}
	regressions := detectRegressions(trends, candidate)
	if len(regressions) != 1 {
		t.Fatalf("expected 1 correctness regression, got %d", len(regressions))
	}
	if regressions[0].Severity != TrendSeverityHardStop {
		t.Fatalf("expected hard stop for correctness regression, got %q", regressions[0].Severity)
	}
	if regressions[0].Kind != TrendKindCorrectness {
		t.Fatalf("expected correctness kind, got %q", regressions[0].Kind)
	}
}

func TestDetectRegressionsInstability(t *testing.T) {
	candidate := SummaryReport{
		Workloads: []WorkloadSummary{
			{Name: "q1", TargetSchema: "trade", CorrectnessFailures: 0},
		},
	}
	trends := []WorkloadTrend{
		{
			Name:               "q1",
			TargetSchema:       "trade",
			InstabilityChanged: true,
			Classification:     TrendClassRegression,
		},
	}
	regressions := detectRegressions(trends, candidate)
	if len(regressions) != 1 {
		t.Fatalf("expected 1 instability regression, got %d", len(regressions))
	}
	if regressions[0].Kind != TrendKindInstability {
		t.Fatalf("expected instability kind, got %q", regressions[0].Kind)
	}
}

func TestDetectRegressionsP95Latency(t *testing.T) {
	candidate := SummaryReport{
		Workloads: []WorkloadSummary{
			{Name: "q1", TargetSchema: "trade", P95: 150 * time.Millisecond},
		},
	}
	trends := []WorkloadTrend{
		{
			Name:            "q1",
			TargetSchema:    "trade",
			BaselineP95:     100 * time.Millisecond,
			CandidateP95:    150 * time.Millisecond,
			P95Delta:        50 * time.Millisecond,
			P95DeltaPercent: 50,
			Classification:  TrendClassRegression,
		},
	}
	regressions := detectRegressions(trends, candidate)
	if len(regressions) != 1 {
		t.Fatalf("expected 1 p95 latency regression, got %d", len(regressions))
	}
	if regressions[0].Kind != TrendKindP95Latency {
		t.Fatalf("expected p95 kind, got %q", regressions[0].Kind)
	}
	if regressions[0].Severity != TrendSeverityReview {
		t.Fatalf("expected review severity for latency, got %q", regressions[0].Severity)
	}
}

func TestDetectBaselineDrift(t *testing.T) {
	base := []TrendRun{
		{Summary: SummaryReport{Workloads: []WorkloadSummary{{P95: 120 * time.Millisecond}}}},
	}
	drift := []TrendRun{
		{Summary: SummaryReport{Workloads: []WorkloadSummary{{P95: 100 * time.Millisecond}}}},
	}
	sig := detectBaselineDrift(base, drift)
	if sig == nil {
		t.Fatalf("expected baseline drift to be detected (20%% increase)")
	}
	if !sig.Detected {
		t.Fatalf("expected drift to be detected")
	}
	base2 := []TrendRun{
		{Summary: SummaryReport{Workloads: []WorkloadSummary{{P95: 102 * time.Millisecond}}}},
	}
	drift2 := []TrendRun{
		{Summary: SummaryReport{Workloads: []WorkloadSummary{{P95: 100 * time.Millisecond}}}},
	}
	sig2 := detectBaselineDrift(base2, drift2)
	if sig2 != nil && sig2.Detected {
		t.Fatalf("expected no drift for 2%% increase")
	}
	sig3 := detectBaselineDrift(nil, drift)
	if sig3 != nil {
		t.Fatalf("expected nil drift with empty baseline")
	}
}

func TestFindCandidateRun(t *testing.T) {
	ts1 := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	ts2 := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	makeRun := func(path string, ts time.Time) TrendRun {
		return TrendRun{
			Path:      path,
			StartedAt: ts,
			Summary: SummaryReport{
				Provenance: &RunProvenance{
					Mode:         string(ExecutionModeLive),
					Scale:        string(ScaleSmall),
					Distribution: string(DistributionUniform),
					TierProfile:  DefaultTierMixProfile().Name,
				},
				OracleModes: map[string]string{"q1": string(OracleModeLoadedState), "q2": string(OracleModeLoadedState)},
				Workloads:   []WorkloadSummary{{Name: "q1"}, {Name: "q2"}},
			},
		}
	}
	runs := []TrendRun{
		makeRun("/a/summary.json", ts1),
		makeRun("/b/summary.json", ts2),
	}
	key := buildComparabilityIdentity(runs[0].Summary)
	candidate, _, err := findCandidateRun(runs, "", key)
	if err != nil {
		t.Fatalf("findCandidateRun failed: %v", err)
	}
	if candidate.Path != "/b/summary.json" {
		t.Fatalf("expected newest run as candidate, got %s", candidate.Path)
	}
	candidate2, _, err := findCandidateRun(runs, "/a/summary.json", key)
	if err != nil {
		t.Fatalf("findCandidateRun with explicit path failed: %v", err)
	}
	if candidate2.Path != "/a/summary.json" {
		t.Fatalf("expected specified candidate, got %s", candidate2.Path)
	}
}
