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

func TestDefaultProtectedWorkloads(t *testing.T) {
	protected := DefaultProtectedWorkloads()
	if len(protected) != 9 {
		t.Fatalf("expected 9 protected workloads, got %d", len(protected))
	}
	workloads := map[string]bool{}
	for _, name := range protected {
		workloads[name] = true
	}
	for _, required := range []string{"baseline-page-1", "hot-selective-page", "deep-page-1000"} {
		if !workloads[required] {
			t.Fatalf("expected %q in protected workloads", required)
		}
	}
}

func TestWriteBaselineCaptureIncludesProvenance(t *testing.T) {
	dir := t.TempDir()
	baseTime := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)
	result := &RunResult{
		Passed:      true,
		StartedAt:   baseTime,
		CompletedAt: baseTime.Add(10 * time.Second),
		Generator:   GeneratorConfig{Scale: ScaleSmall, Distribution: DistributionUniform},
		Config:      Config{Mode: ExecutionModeLive, Scale: ScaleSmall, Distribution: DistributionUniform, TierProfile: DefaultTierMixProfile().Name},
		Provenance: &RunProvenance{
			Channel:      "ci",
			GitSHA:       "abc123",
			GitRef:       "refs/heads/main",
			Label:        "test-run",
			Mode:         string(ExecutionModeLive),
			Scale:        string(ScaleSmall),
			Distribution: string(DistributionUniform),
			TierProfile:  DefaultTierMixProfile().Name,
		},
		Workloads: []WorkloadDefinition{
			{Name: "q1", Category: WorkloadCategoryPagination, TargetSchema: "trade", OracleMode: OracleModeLoadedState},
		},
		Executions: []WorkloadRunResult{{
			Name:         "q1",
			Passed:       true,
			Duration:     10 * time.Millisecond,
			OracleMode:   string(OracleModeLoadedState),
			ResultCount:  20,
			TotalRecords: 100,
		}},
	}
	if err := WriteBaselineCapture(dir, result); err != nil {
		t.Fatalf("WriteBaselineCapture failed: %v", err)
	}
	summaryPath := filepath.Join(dir, "benchmark-summary.json")
	summary, err := ReadSummaryReport(summaryPath)
	if err != nil {
		t.Fatalf("ReadSummaryReport failed: %v", err)
	}
	if summary.Provenance == nil {
		t.Fatalf("expected provenance in summary")
	}
	if summary.Provenance.Channel != "ci" {
		t.Fatalf("expected channel ci, got %q", summary.Provenance.Channel)
	}
	if summary.Provenance.GitSHA != "abc123" {
		t.Fatalf("expected git SHA abc123, got %q", summary.Provenance.GitSHA)
	}
	if summary.Provenance.StartedAt.IsZero() {
		t.Fatalf("expected started_at to be populated")
	}
	if summary.Provenance.CompletedAt.IsZero() {
		t.Fatalf("expected completed_at to be populated")
	}
}

func TestProvenancePopulatedFromResultWithoutExplicitProvenance(t *testing.T) {
	baseTime := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)
	result := &RunResult{
		Passed:      true,
		StartedAt:   baseTime,
		CompletedAt: baseTime.Add(10 * time.Second),
		Config:      Config{Mode: ExecutionModeSmoke, Scale: ScaleSmall, Distribution: DistributionUniform, TierProfile: DefaultTierMixProfile().Name},
		Generator:   GeneratorConfig{Scale: ScaleSmall, Distribution: DistributionUniform},
		Workloads: []WorkloadDefinition{
			{Name: "q1", Category: WorkloadCategoryPagination, TargetSchema: "trade", OracleMode: OracleModeLoadedState},
		},
		Executions: []WorkloadRunResult{{
			Name:         "q1",
			Passed:       true,
			Duration:     10 * time.Millisecond,
			OracleMode:   string(OracleModeLoadedState),
			ResultCount:  20,
			TotalRecords: 100,
		}},
	}
	summary := SummarizeRunResult(result)
	if summary.Provenance == nil {
		t.Fatalf("expected provenance to be populated from result even without explicit provenance")
	}
	if summary.Provenance.Mode != string(ExecutionModeSmoke) {
		t.Fatalf("expected mode smoke in provenance, got %q", summary.Provenance.Mode)
	}
}

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
