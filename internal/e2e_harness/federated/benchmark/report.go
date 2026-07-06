package benchmark

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

const benchmarkArtifactFormatVersion = "v1"

// ArtifactMetadata captures stable identifiers and environment details for a benchmark artifact.
type ArtifactMetadata struct {
	FormatVersion string              `json:"format_version"`
	BenchmarkID   string              `json:"benchmark_id"`
	WorkloadNames []string            `json:"workload_names,omitempty"`
	Environment   EnvironmentMetadata `json:"environment"`
}

// EnvironmentMetadata records machine metadata relevant to benchmark artifacts.
type EnvironmentMetadata struct {
	GoVersion string `json:"go_version"`
	GOOS      string `json:"goos"`
	GOARCH    string `json:"goarch"`
	NumCPU    int    `json:"num_cpu"`
	Hostname  string `json:"hostname,omitempty"`
}

// RunProvenance captures runtime context needed for longitudinal trend analysis.
type RunProvenance struct {
	StartedAt    time.Time `json:"started_at,omitempty"`
	CompletedAt  time.Time `json:"completed_at,omitempty"`
	Preset       string    `json:"preset,omitempty"`
	Channel      string    `json:"channel,omitempty"`
	GitSHA       string    `json:"git_sha,omitempty"`
	GitRef       string    `json:"git_ref,omitempty"`
	Label        string    `json:"label,omitempty"`
	Mode         string    `json:"mode,omitempty"`
	Scale        string    `json:"scale,omitempty"`
	Distribution string    `json:"distribution,omitempty"`
	TierProfile  string    `json:"tier_profile,omitempty"`
	Concurrency  int       `json:"concurrency,omitempty"`
}

// DefaultProtectedWorkloads returns the standard set of workloads that trigger
// hard-stop regression checks during trend analysis.
func DefaultProtectedWorkloads() []string {
	return []string{
		"baseline-page-1",
		"hot-selective-page",
		"hot-low-selectivity-page",
		"eav-selective-page",
		"mixed-hot-eav-page",
		"mixed-tier-window",
		"hot-only-window",
		"cold-only-window",
		"deep-page-1000",
	}
}

// SummaryReport captures aggregated execution metrics.
type SummaryReport struct {
	Metadata            ArtifactMetadata         `json:"metadata"`
	Provenance          *RunProvenance           `json:"provenance,omitempty"`
	OracleModes         map[string]string        `json:"oracle_modes,omitempty"`
	OracleProvenance    []OracleProvenance       `json:"oracle_provenance,omitempty"`
	Stability           StabilitySummary         `json:"stability"`
	ExecutionCount      int                      `json:"execution_count"`
	Passed              bool                     `json:"passed"`
	FailureCount        int                      `json:"failure_count,omitempty"`
	CorrectnessFailures int                      `json:"correctness_failures,omitempty"`
	InfraFailures       int                      `json:"infra_failures,omitempty"`
	Min                 time.Duration            `json:"min"`
	P50                 time.Duration            `json:"p50"`
	P95                 time.Duration            `json:"p95"`
	P99                 time.Duration            `json:"p99"`
	Max                 time.Duration            `json:"max"`
	Avg                 time.Duration            `json:"avg"`
	TotalDuration       time.Duration            `json:"total_duration"`
	QPS                 float64                  `json:"qps"`
	AssertionStats      map[string]AssertionStat `json:"assertion_stats"`
	Workloads           []WorkloadSummary        `json:"workloads,omitempty"`
}

// AssertionStat captures aggregate assertion pass/fail counts.
type AssertionStat struct {
	Passed int `json:"passed"`
	Failed int `json:"failed"`
}

// OracleProvenance groups workloads by expected-result oracle mode.
type OracleProvenance struct {
	Mode      string   `json:"mode"`
	Workloads []string `json:"workloads,omitempty"`
}

// StabilitySummary captures repeated-run stability signals for same-seed runs.
type StabilitySummary struct {
	Enabled              bool     `json:"enabled"`
	WorkloadsChecked     int      `json:"workloads_checked,omitempty"`
	UnstableWorkloads    []string `json:"unstable_workloads,omitempty"`
	FailureKindFailures  int      `json:"failure_kind_failures,omitempty"`
	TotalRecordsFailures int      `json:"total_records_failures,omitempty"`
	PageRowIDFailures    int      `json:"page_row_id_failures,omitempty"`
}

// WorkloadSummary captures stable workload-level metrics and metadata.
type WorkloadSummary struct {
	Name                string                   `json:"name"`
	Category            string                   `json:"category"`
	TargetSchema        string                   `json:"target_schema,omitempty"`
	ExecutionSource     string                   `json:"execution_source,omitempty"`
	OracleMode          string                   `json:"oracle_mode,omitempty"`
	PreferHot           bool                     `json:"prefer_hot,omitempty"`
	Distribution        Distribution             `json:"distribution"`
	ExecutionCount      int                      `json:"execution_count"`
	Passed              bool                     `json:"passed"`
	FailureCount        int                      `json:"failure_count,omitempty"`
	CorrectnessFailures int                      `json:"correctness_failures,omitempty"`
	InfraFailures       int                      `json:"infra_failures,omitempty"`
	PageSize            int                      `json:"page_size,omitempty"`
	MaxOffset           int                      `json:"max_offset,omitempty"`
	AvgResultCount      float64                  `json:"avg_result_count,omitempty"`
	AvgTotalRecords     float64                  `json:"avg_total_records,omitempty"`
	Min                 time.Duration            `json:"min"`
	P50                 time.Duration            `json:"p50"`
	P95                 time.Duration            `json:"p95"`
	P99                 time.Duration            `json:"p99"`
	Max                 time.Duration            `json:"max"`
	Avg                 time.Duration            `json:"avg"`
	TotalDuration       time.Duration            `json:"total_duration"`
	QPS                 float64                  `json:"qps"`
	AssertionStats      map[string]AssertionStat `json:"assertion_stats,omitempty"`
	RouteEngineCounts   map[string]int           `json:"route_engine_counts,omitempty"`
	RouteReasonCounts   map[string]int           `json:"route_reason_counts,omitempty"`
}

// DiffReport captures machine-readable baseline deltas.
type DiffReport struct {
	BaselineMetadata  ArtifactMetadata `json:"baseline_metadata"`
	CandidateMetadata ArtifactMetadata `json:"candidate_metadata"`
	Summary           SummaryDiff      `json:"summary"`
	Workloads         []WorkloadDiff   `json:"workloads,omitempty"`
}

// SummaryDiff captures aggregate benchmark deltas.
type SummaryDiff struct {
	PassedChanged            bool          `json:"passed_changed"`
	FailureCountDelta        int           `json:"failure_count_delta"`
	CorrectnessFailuresDelta int           `json:"correctness_failures_delta"`
	InfraFailuresDelta       int           `json:"infra_failures_delta"`
	ExecutionCountDelta      int           `json:"execution_count_delta"`
	QPSDelta                 float64       `json:"qps_delta"`
	AvgLatencyDelta          time.Duration `json:"avg_latency_delta"`
	P95LatencyDelta          time.Duration `json:"p95_latency_delta"`
	P99LatencyDelta          time.Duration `json:"p99_latency_delta"`
	TotalDurationDelta       time.Duration `json:"total_duration_delta"`
}

// WorkloadDiff captures workload-level metric deltas.
type WorkloadDiff struct {
	Name                     string        `json:"name"`
	TargetSchema             string        `json:"target_schema,omitempty"`
	MissingInBaseline        bool          `json:"missing_in_baseline,omitempty"`
	MissingInCandidate       bool          `json:"missing_in_candidate,omitempty"`
	PassedChanged            bool          `json:"passed_changed"`
	FailureCountDelta        int           `json:"failure_count_delta"`
	CorrectnessFailuresDelta int           `json:"correctness_failures_delta"`
	InfraFailuresDelta       int           `json:"infra_failures_delta"`
	ExecutionCountDelta      int           `json:"execution_count_delta"`
	QPSDelta                 float64       `json:"qps_delta"`
	AvgLatencyDelta          time.Duration `json:"avg_latency_delta"`
	P95LatencyDelta          time.Duration `json:"p95_latency_delta"`
	P99LatencyDelta          time.Duration `json:"p99_latency_delta"`
	AvgResultCountDelta      float64       `json:"avg_result_count_delta"`
	AvgTotalRecordsDelta     float64       `json:"avg_total_records_delta"`
	RouteEngineChanged       bool          `json:"route_engine_changed,omitempty"`
}

// TrendRun represents a single historical benchmark summary loaded from disk.
type TrendRun struct {
	Path      string        `json:"path"`
	Summary   SummaryReport `json:"summary"`
	StartedAt time.Time     `json:"started_at"`
}

// TrendReport is a longitudinal analysis artifact that surfaces regressions,
// baseline drift, and methodology changes across historical benchmark runs.
type TrendReport struct {
	Candidate          TrendRun              `json:"candidate"`
	BaselineWindow     []TrendRun            `json:"baseline_window"`
	DriftWindow        []TrendRun            `json:"drift_window,omitempty"`
	ComparabilityKey   string                `json:"comparability_key"`
	MethodologyChanges []string              `json:"methodology_changes,omitempty"`
	Regressions        []RegressionCandidate `json:"regressions,omitempty"`
	Drift              *DriftSignal          `json:"drift,omitempty"`
	WorkloadTrends     []WorkloadTrend       `json:"workload_trends"`
	ProtectedWorkloads []string              `json:"protected_workloads"`
	WindowsToStable    bool                  `json:"windows_too_stable"`
	Status             string                `json:"status"`
}

// RegressionCandidate records a single regression signal detected during trend analysis.
type RegressionCandidate struct {
	WorkloadName   string  `json:"workload"`
	TargetSchema   string  `json:"target_schema,omitempty"`
	Kind           string  `json:"kind"`
	BaselineValue  float64 `json:"baseline_value"`
	CandidateValue float64 `json:"candidate_value"`
	Delta          float64 `json:"delta"`
	Threshold      float64 `json:"threshold,omitempty"`
	Severity       string  `json:"severity"`
	Description    string  `json:"description,omitempty"`
}

// DriftSignal captures whether the baseline window itself has drifted from an
// older reference window, independent of the candidate run.
type DriftSignal struct {
	Detected    bool     `json:"detected"`
	Description string   `json:"description"`
	Evidence    []string `json:"evidence,omitempty"`
}

// WorkloadTrend represents a single workload's metrics across the baseline window
// and candidate run.
type WorkloadTrend struct {
	Name               string        `json:"name"`
	TargetSchema       string        `json:"target_schema,omitempty"`
	BaselineP95        time.Duration `json:"baseline_p95"`
	CandidateP95       time.Duration `json:"candidate_p95"`
	P95Delta           time.Duration `json:"p95_delta"`
	P95DeltaPercent    float64       `json:"p95_delta_percent"`
	BaselineQPS        float64       `json:"baseline_qps"`
	CandidateQPS       float64       `json:"candidate_qps"`
	QPSDelta           float64       `json:"qps_delta"`
	QPSDeltaPercent    float64       `json:"qps_delta_percent"`
	CorrectnessChanged bool          `json:"correctness_changed"`
	CorrectnessDelta   int           `json:"correctness_delta"`
	InstabilityChanged bool          `json:"instability_changed"`
	RouteChanged       bool          `json:"route_engine_changed,omitempty"`
	Classification     string        `json:"classification"`
}

// WriteJSONReport writes a benchmark run result to JSON.
func WriteJSONReport(path string, result *RunResult) error {
	if result == nil {
		return fmt.Errorf("run result cannot be nil")
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal JSON report: %w", err)
	}
	return os.WriteFile(path, data, 0o644)
}

// WriteMarkdownReport writes a lightweight markdown summary for a benchmark run.
func WriteMarkdownReport(path string, result *RunResult) error {
	if result == nil {
		return fmt.Errorf("run result cannot be nil")
	}
	summary := SummarizeRunResult(result)
	var b strings.Builder
	b.WriteString("# Federated Query Benchmark Report\n\n")
	b.WriteString(fmt.Sprintf("- Benchmark ID: `%s`\n", summary.Metadata.BenchmarkID))
	b.WriteString(fmt.Sprintf("- Format version: `%s`\n", summary.Metadata.FormatVersion))
	b.WriteString(fmt.Sprintf("- Validation only: %t\n", result.ValidationOnly))
	b.WriteString(fmt.Sprintf("- Passed: %t\n", result.Passed))
	b.WriteString(fmt.Sprintf("- Failure count: %d\n", result.FailureCount))
	b.WriteString(fmt.Sprintf("- Correctness failures: %d\n", summary.CorrectnessFailures))
	b.WriteString(fmt.Sprintf("- Distribution: %s\n", result.Generator.Distribution))
	b.WriteString(fmt.Sprintf("- Scale: %s\n", result.Generator.Scale))
	b.WriteString(fmt.Sprintf("- Executions: %d\n", len(result.Executions)))
	b.WriteString(fmt.Sprintf("- Total duration: %s\n", summary.TotalDuration))
	b.WriteString(fmt.Sprintf("- Min: %s\n", summary.Min))
	b.WriteString(fmt.Sprintf("- P50: %s\n", summary.P50))
	b.WriteString(fmt.Sprintf("- P95: %s\n", summary.P95))
	b.WriteString(fmt.Sprintf("- P99: %s\n", summary.P99))
	b.WriteString(fmt.Sprintf("- Max: %s\n", summary.Max))
	b.WriteString(fmt.Sprintf("- Repeated-run checks enabled: %t\n", summary.Stability.Enabled))
	if summary.Stability.Enabled {
		b.WriteString(fmt.Sprintf("- Repeated-run workloads checked: %d\n", summary.Stability.WorkloadsChecked))
		b.WriteString(fmt.Sprintf("- Repeated-run failure-kind failures: %d\n", summary.Stability.FailureKindFailures))
		b.WriteString(fmt.Sprintf("- Repeated-run total-record failures: %d\n", summary.Stability.TotalRecordsFailures))
		b.WriteString(fmt.Sprintf("- Repeated-run page-row-id failures: %d\n", summary.Stability.PageRowIDFailures))
	}
	if len(summary.Stability.UnstableWorkloads) > 0 {
		b.WriteString(fmt.Sprintf("- Unstable workloads: `%s`\n", strings.Join(summary.Stability.UnstableWorkloads, "`, `")))
	}
	if len(result.Executions) > 0 {
		b.WriteString("\n## Executions\n\n")
		for _, execution := range result.Executions {
			b.WriteString(fmt.Sprintf("- `%s`: passed=%t failure_kind=%s oracle_mode=%s prefer_hot=%t failures=%d count=%d total=%d duration=%s offset=%d\n", execution.Name, execution.Passed, execution.FailureKind, execution.OracleMode, execution.PreferHot, execution.FailureCount, execution.ResultCount, execution.TotalRecords, execution.Duration, execution.Offset))
			if execution.InfraError != "" {
				b.WriteString(fmt.Sprintf("  infra_error=%s\n", execution.InfraError))
			}
		}
	}
	if len(summary.AssertionStats) > 0 {
		b.WriteString("\n## Assertions\n\n")
		keys := make([]string, 0, len(summary.AssertionStats))
		for key := range summary.AssertionStats {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			stat := summary.AssertionStats[key]
			b.WriteString(fmt.Sprintf("- `%s`: passed=%d failed=%d\n", key, stat.Passed, stat.Failed))
		}
	}
	if len(summary.Workloads) > 0 {
		b.WriteString("\n## Workload Summaries\n\n")
		for _, workload := range summary.Workloads {
			routeInfo := ""
			if len(workload.RouteEngineCounts) > 0 {
				parts := make([]string, 0)
				for engine, count := range workload.RouteEngineCounts {
					parts = append(parts, fmt.Sprintf("%s=%d", engine, count))
				}
				sort.Strings(parts)
				routeInfo = " route=[" + strings.Join(parts, ", ") + "]"
			}
			b.WriteString(fmt.Sprintf("- `%s`: schema=%s oracle_mode=%s prefer_hot=%t executions=%d passed=%t qps=%.2f p95=%s avg=%s avg_result_count=%.2f avg_total_records=%.2f%s\n", workload.Name, workload.TargetSchema, workload.OracleMode, workload.PreferHot, workload.ExecutionCount, workload.Passed, workload.QPS, workload.P95, workload.Avg, workload.AvgResultCount, workload.AvgTotalRecords, routeInfo))
		}
	}
	if len(summary.OracleProvenance) > 0 {
		b.WriteString("\n## Oracle Provenance\n\n")
		for _, provenance := range summary.OracleProvenance {
			b.WriteString(fmt.Sprintf("- `%s`: `%s`\n", provenance.Mode, strings.Join(provenance.Workloads, "`, `")))
		}
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

// WriteBaselineCapture writes JSON and markdown reports into a directory.
func WriteBaselineCapture(dir string, result *RunResult) error {
	if result == nil {
		return fmt.Errorf("run result cannot be nil")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create baseline dir: %w", err)
	}
	if err := WriteJSONReport(filepath.Join(dir, "benchmark-result.json"), result); err != nil {
		return err
	}
	if err := WriteMarkdownReport(filepath.Join(dir, "benchmark-result.md"), result); err != nil {
		return err
	}
	summary := SummarizeRunResult(result)
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal baseline summary: %w", err)
	}
	return os.WriteFile(filepath.Join(dir, "benchmark-summary.json"), data, 0o644)
}

// WriteDiffReport writes a diff report to JSON.
func WriteDiffReport(path string, diff *DiffReport) error {
	if diff == nil {
		return fmt.Errorf("diff report cannot be nil")
	}
	data, err := json.MarshalIndent(diff, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal diff report: %w", err)
	}
	return os.WriteFile(path, data, 0o644)
}

// SummarizeRunResult aggregates durations and assertion outcomes.
func SummarizeRunResult(result *RunResult) SummaryReport {
	summary := SummaryReport{AssertionStats: make(map[string]AssertionStat)}
	if result == nil {
		return summary
	}
	summary.Metadata = metadataForResult(result)
	summary.OracleModes = cloneStringMap(result.OracleModes)
	summary.OracleProvenance = summarizeOracleProvenance(result)
	if result.Provenance != nil {
		pc := *result.Provenance
		if pc.StartedAt.IsZero() {
			pc.StartedAt = result.StartedAt
		}
		if pc.CompletedAt.IsZero() {
			pc.CompletedAt = result.CompletedAt
		}
		if pc.Concurrency == 0 {
			pc.Concurrency = result.Config.Concurrency
		}
		summary.Provenance = &pc
	} else if !result.StartedAt.IsZero() {
		summary.Provenance = &RunProvenance{
			StartedAt:    result.StartedAt,
			CompletedAt:  result.CompletedAt,
			Mode:         string(result.Config.Mode),
			Scale:        string(result.Config.Scale),
			Distribution: string(result.Config.Distribution),
			TierProfile:  result.Config.TierProfile,
			Concurrency:  result.Config.Concurrency,
		}
	}
	if len(result.Executions) == 0 {
		summary.Passed = result != nil && result.Passed
		summary.FailureCount = resultFailureCount(result)
		summary.Workloads = summarizeWorkloads(result)
		summary.Stability = summarizeStability(summary.Workloads)
		return summary
	}
	summary.Passed = result.Passed
	summary.FailureCount = resultFailureCount(result)
	summary.Workloads = summarizeWorkloads(result)
	summary.Stability = summarizeStability(summary.Workloads)
	durations := make([]time.Duration, 0, len(result.Executions))
	for _, execution := range result.Executions {
		durations = append(durations, execution.Duration)
		if execution.InfraError != "" {
			summary.InfraFailures++
		}
		if execution.FailureKind == FailureKindCorrectness {
			summary.CorrectnessFailures++
		}
		for _, assertion := range execution.Assertions {
			stat := summary.AssertionStats[assertion.Name]
			if assertion.Passed {
				stat.Passed++
			} else {
				stat.Failed++
			}
			summary.AssertionStats[assertion.Name] = stat
		}
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	summary.ExecutionCount = len(durations)
	summary.Min = durations[0]
	summary.P50 = percentileDuration(durations, 0.50)
	summary.P95 = percentileDuration(durations, 0.95)
	summary.P99 = percentileDuration(durations, 0.99)
	summary.Max = durations[len(durations)-1]
	var total time.Duration
	for _, duration := range durations {
		total += duration
	}
	summary.TotalDuration = total
	summary.Avg = total / time.Duration(len(durations))
	if total > 0 {
		summary.QPS = float64(len(durations)) / total.Seconds()
	}
	return summary
}

func percentileDuration(values []time.Duration, percentile float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	idx := int(math.Ceil(percentile*float64(len(values)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

// FormatConsoleSummary returns a stable text summary for terminal output.
func FormatConsoleSummary(result *RunResult) string {
	if result == nil {
		return "benchmark result: <nil>"
	}
	summary := SummarizeRunResult(result)
	var b strings.Builder
	b.WriteString("Benchmark Summary\n")
	b.WriteString(fmt.Sprintf("benchmark_id=%s scale=%s distribution=%s executions=%d passed=%t failures=%d correctness_failures=%d infra_failures=%d\n", summary.Metadata.BenchmarkID, result.Generator.Scale, result.Generator.Distribution, summary.ExecutionCount, summary.Passed, summary.FailureCount, summary.CorrectnessFailures, summary.InfraFailures))
	b.WriteString(fmt.Sprintf("latency min=%s p50=%s p95=%s p99=%s max=%s avg=%s total=%s qps=%.2f\n", summary.Min, summary.P50, summary.P95, summary.P99, summary.Max, summary.Avg, summary.TotalDuration, summary.QPS))
	b.WriteString(fmt.Sprintf("stability enabled=%t workloads_checked=%d unstable_workloads=%d failure_kind_failures=%d total_record_failures=%d page_row_id_failures=%d\n", summary.Stability.Enabled, summary.Stability.WorkloadsChecked, len(summary.Stability.UnstableWorkloads), summary.Stability.FailureKindFailures, summary.Stability.TotalRecordsFailures, summary.Stability.PageRowIDFailures))
	if len(summary.OracleProvenance) > 0 {
		parts := make([]string, 0, len(summary.OracleProvenance))
		for _, provenance := range summary.OracleProvenance {
			parts = append(parts, fmt.Sprintf("%s=%s", provenance.Mode, strings.Join(provenance.Workloads, ",")))
		}
		b.WriteString(fmt.Sprintf("oracle_provenance %s\n", strings.Join(parts, " ")))
	}
	if len(summary.AssertionStats) > 0 {
		keys := make([]string, 0, len(summary.AssertionStats))
		for key := range summary.AssertionStats {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			stat := summary.AssertionStats[key]
			b.WriteString(fmt.Sprintf("assertion %s passed=%d failed=%d\n", key, stat.Passed, stat.Failed))
		}
	}
	if len(summary.Workloads) > 0 {
		for _, workload := range summary.Workloads {
			b.WriteString(fmt.Sprintf("workload %s schema=%s executions=%d passed=%t qps=%.2f p95=%s avg=%s avg_result_count=%.2f avg_total_records=%.2f\n", workload.Name, workload.TargetSchema, workload.ExecutionCount, workload.Passed, workload.QPS, workload.P95, workload.Avg, workload.AvgResultCount, workload.AvgTotalRecords))
		}
	}
	return b.String()
}

func resultFailureCount(result *RunResult) int {
	if result == nil {
		return 0
	}
	if result.FailureCount > 0 {
		return result.FailureCount
	}
	failed := 0
	for _, execution := range result.Executions {
		failed += maxInt(execution.FailureCount, countFailedAssertions(execution.Assertions))
	}
	return failed
}

// BuildArtifactMetadata returns stable metadata for benchmark artifacts.
func BuildArtifactMetadata(cfg Config, genCfg GeneratorConfig, workloads []WorkloadDefinition) ArtifactMetadata {
	hostname, _ := os.Hostname()
	names := make([]string, 0, len(workloads))
	for _, workload := range workloads {
		names = append(names, workload.Name)
	}
	payload := struct {
		Config    Config          `json:"config"`
		Generator GeneratorConfig `json:"generator"`
		Workloads []string        `json:"workloads"`
	}{Config: cfg, Generator: genCfg, Workloads: names}
	encoded, _ := json.Marshal(payload)
	hash := fnv.New64a()
	_, _ = hash.Write(encoded)
	return ArtifactMetadata{
		FormatVersion: benchmarkArtifactFormatVersion,
		BenchmarkID:   fmt.Sprintf("bench-%x", hash.Sum64()),
		WorkloadNames: names,
		Environment: EnvironmentMetadata{
			GoVersion: runtime.Version(),
			GOOS:      runtime.GOOS,
			GOARCH:    runtime.GOARCH,
			NumCPU:    runtime.NumCPU(),
			Hostname:  hostname,
		},
	}
}

func metadataForResult(result *RunResult) ArtifactMetadata {
	if result == nil {
		return ArtifactMetadata{}
	}
	if result.Metadata.BenchmarkID != "" {
		return result.Metadata
	}
	return BuildArtifactMetadata(result.Config, result.Generator, result.Workloads)
}

// CompareSummaryReports builds a machine-readable diff between two summaries.
func CompareSummaryReports(baseline, candidate SummaryReport) DiffReport {
	return DiffReport{
		BaselineMetadata:  baseline.Metadata,
		CandidateMetadata: candidate.Metadata,
		Summary: SummaryDiff{
			PassedChanged:            baseline.Passed != candidate.Passed,
			FailureCountDelta:        candidate.FailureCount - baseline.FailureCount,
			CorrectnessFailuresDelta: candidate.CorrectnessFailures - baseline.CorrectnessFailures,
			InfraFailuresDelta:       candidate.InfraFailures - baseline.InfraFailures,
			ExecutionCountDelta:      candidate.ExecutionCount - baseline.ExecutionCount,
			QPSDelta:                 candidate.QPS - baseline.QPS,
			AvgLatencyDelta:          candidate.Avg - baseline.Avg,
			P95LatencyDelta:          candidate.P95 - baseline.P95,
			P99LatencyDelta:          candidate.P99 - baseline.P99,
			TotalDurationDelta:       candidate.TotalDuration - baseline.TotalDuration,
		},
		Workloads: compareWorkloadSummaries(baseline.Workloads, candidate.Workloads),
	}
}

// CompareRunResults summarizes and compares two run results.
func CompareRunResults(baseline, candidate *RunResult) DiffReport {
	return CompareSummaryReports(SummarizeRunResult(baseline), SummarizeRunResult(candidate))
}

// ReadSummaryReport reads a summary report JSON file.
func ReadSummaryReport(path string) (SummaryReport, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return SummaryReport{}, err
	}
	var summary SummaryReport
	if err := json.Unmarshal(data, &summary); err != nil {
		return SummaryReport{}, fmt.Errorf("decode summary report: %w", err)
	}
	return summary, nil
}

// FormatDiffSummary returns a stable console diff summary.
func FormatDiffSummary(diff DiffReport) string {
	var b strings.Builder
	b.WriteString("Benchmark Diff Summary\n")
	b.WriteString(fmt.Sprintf("baseline=%s candidate=%s passed_changed=%t failure_delta=%d correctness_delta=%d infra_delta=%d qps_delta=%.2f avg_latency_delta=%s p95_latency_delta=%s\n",
		diff.BaselineMetadata.BenchmarkID,
		diff.CandidateMetadata.BenchmarkID,
		diff.Summary.PassedChanged,
		diff.Summary.FailureCountDelta,
		diff.Summary.CorrectnessFailuresDelta,
		diff.Summary.InfraFailuresDelta,
		diff.Summary.QPSDelta,
		diff.Summary.AvgLatencyDelta,
		diff.Summary.P95LatencyDelta,
	))
	for _, workload := range diff.Workloads {
		b.WriteString(fmt.Sprintf("workload %s schema=%s missing_baseline=%t missing_candidate=%t passed_changed=%t correctness_delta=%d qps_delta=%.2f avg_latency_delta=%s p95_latency_delta=%s p99_latency_delta=%s avg_result_delta=%.2f avg_total_delta=%.2f route_changed=%t\n",
			workload.Name,
			workload.TargetSchema,
			workload.MissingInBaseline,
			workload.MissingInCandidate,
			workload.PassedChanged,
			workload.CorrectnessFailuresDelta,
			workload.QPSDelta,
			workload.AvgLatencyDelta,
			workload.P95LatencyDelta,
			workload.P99LatencyDelta,
			workload.AvgResultCountDelta,
			workload.AvgTotalRecordsDelta,
			workload.RouteEngineChanged,
		))
	}
	return b.String()
}

func summarizeWorkloads(result *RunResult) []WorkloadSummary {
	if result == nil || len(result.Executions) == 0 {
		return nil
	}
	workloadDefs := make(map[string]WorkloadDefinition, len(result.Workloads))
	for _, workload := range result.Workloads {
		workloadDefs[workload.Name] = workload
	}
	grouped := make(map[string][]WorkloadRunResult)
	for _, execution := range result.Executions {
		grouped[execution.Name] = append(grouped[execution.Name], execution)
	}
	names := make([]string, 0, len(grouped))
	for name := range grouped {
		names = append(names, name)
	}
	sort.Strings(names)
	workloads := make([]WorkloadSummary, 0, len(names))
	for _, name := range names {
		runs := grouped[name]
		workload := WorkloadSummary{Name: name, Passed: true, AssertionStats: make(map[string]AssertionStat)}
		if def, ok := workloadDefs[name]; ok {
			workload.Category = string(def.Category)
			workload.TargetSchema = def.TargetSchema
			workload.ExecutionSource = def.ExecutionSource
			workload.OracleMode = string(def.ResolvedOracleMode())
			workload.PreferHot = def.PreferHot
			workload.Distribution = runs[0].Distribution
			workload.PageSize = def.PageSize
		}
		if mode, ok := result.OracleModes[name]; ok && mode != "" {
			workload.OracleMode = mode
		}
		durations := make([]time.Duration, 0, len(runs))
		var totalDuration time.Duration
		var totalResultCount int
		var totalRecords int64
		for _, run := range runs {
			durations = append(durations, run.Duration)
			totalDuration += run.Duration
			totalResultCount += run.ResultCount
			totalRecords += run.TotalRecords
			workload.ExecutionCount++
			workload.FailureCount += maxInt(run.FailureCount, countFailedAssertions(run.Assertions))
			if run.InfraError != "" {
				workload.InfraFailures++
			}
			if run.FailureKind == FailureKindCorrectness {
				workload.CorrectnessFailures++
			}
			if !run.Passed {
				workload.Passed = false
			}
			if run.PageSize > 0 && workload.PageSize == 0 {
				workload.PageSize = run.PageSize
			}
			if run.Offset > workload.MaxOffset {
				workload.MaxOffset = run.Offset
			}
			if workload.OracleMode == "" && run.OracleMode != "" {
				workload.OracleMode = run.OracleMode
			}
			if run.PreferHot {
				workload.PreferHot = true
			}
			if run.RouteEngine != "" {
				if workload.RouteEngineCounts == nil {
					workload.RouteEngineCounts = make(map[string]int)
					workload.RouteReasonCounts = make(map[string]int)
				}
				workload.RouteEngineCounts[run.RouteEngine]++
				if run.RouteReason != "" {
					workload.RouteReasonCounts[run.RouteReason]++
				}
			}
			for _, assertion := range run.Assertions {
				stat := workload.AssertionStats[assertion.Name]
				if assertion.Passed {
					stat.Passed++
				} else {
					stat.Failed++
				}
				workload.AssertionStats[assertion.Name] = stat
			}
		}
		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
		workload.Min = durations[0]
		workload.P50 = percentileDuration(durations, 0.50)
		workload.P95 = percentileDuration(durations, 0.95)
		workload.P99 = percentileDuration(durations, 0.99)
		workload.Max = durations[len(durations)-1]
		workload.TotalDuration = totalDuration
		workload.Avg = totalDuration / time.Duration(len(durations))
		if totalDuration > 0 {
			workload.QPS = float64(len(durations)) / totalDuration.Seconds()
		}
		workload.AvgResultCount = float64(totalResultCount) / float64(len(runs))
		workload.AvgTotalRecords = float64(totalRecords) / float64(len(runs))
		workloads = append(workloads, workload)
	}
	return workloads
}

func summarizeOracleProvenance(result *RunResult) []OracleProvenance {
	if result == nil {
		return nil
	}
	byMode := make(map[string][]string)
	if len(result.Workloads) > 0 {
		for _, workload := range result.Workloads {
			mode := string(workload.ResolvedOracleMode())
			if sampled, ok := result.OracleModes[workload.Name]; ok && sampled != "" {
				mode = sampled
			}
			byMode[mode] = append(byMode[mode], workload.Name)
		}
	} else {
		for _, execution := range result.Executions {
			if execution.OracleMode == "" {
				continue
			}
			byMode[execution.OracleMode] = append(byMode[execution.OracleMode], execution.Name)
		}
	}
	if len(byMode) == 0 {
		return nil
	}
	modes := make([]string, 0, len(byMode))
	for mode := range byMode {
		modes = append(modes, mode)
	}
	sort.Strings(modes)
	provenance := make([]OracleProvenance, 0, len(modes))
	for _, mode := range modes {
		workloads := append([]string(nil), byMode[mode]...)
		sort.Strings(workloads)
		provenance = append(provenance, OracleProvenance{Mode: mode, Workloads: workloads})
	}
	return provenance
}

func summarizeStability(workloads []WorkloadSummary) StabilitySummary {
	var summary StabilitySummary
	seenUnstable := make(map[string]struct{})
	for _, workload := range workloads {
		failureKind := workload.AssertionStats["repeated-run-failure-kind-stable"]
		totalRecords := workload.AssertionStats["repeated-run-total-records-stable"]
		pageRowIDs := workload.AssertionStats["repeated-run-page-row-ids-stable"]
		checked := failureKind.Passed+failureKind.Failed > 0 || totalRecords.Passed+totalRecords.Failed > 0 || pageRowIDs.Passed+pageRowIDs.Failed > 0
		if !checked {
			continue
		}
		summary.Enabled = true
		summary.WorkloadsChecked++
		summary.FailureKindFailures += failureKind.Failed
		summary.TotalRecordsFailures += totalRecords.Failed
		summary.PageRowIDFailures += pageRowIDs.Failed
		if failureKind.Failed > 0 || totalRecords.Failed > 0 || pageRowIDs.Failed > 0 {
			if _, ok := seenUnstable[workload.Name]; !ok {
				seenUnstable[workload.Name] = struct{}{}
				summary.UnstableWorkloads = append(summary.UnstableWorkloads, workload.Name)
			}
		}
	}
	sort.Strings(summary.UnstableWorkloads)
	return summary
}

const (
	TrendSeverityHardStop = "hard_stop"
	TrendSeverityReview   = "review"

	TrendKindCorrectness   = "correctness_regression"
	TrendKindInstability   = "instability_regression"
	TrendKindP95Latency    = "p95_latency_regression"
	TrendKindQPSRegression = "qps_regression"

	TrendClassStable      = "stable"
	TrendClassRegression  = "regression"
	TrendClassImprovement = "improvement"
	TrendClassDrift       = "drift"
	TrendClassMethodology = "methodology_change"

	TrendStatusPass               = "pass"
	TrendStatusRegression         = "regression"
	TrendStatusHardStopRegression = "hard_stop_regression"
	TrendStatusDrift              = "baseline_drift"
	TrendStatusMethodology        = "methodology_change"
)

// ReadTrendHistory scans a directory tree for benchmark-summary.json files
// and returns them sorted by timestamp, oldest first.
func ReadTrendHistory(historyDir string) ([]TrendRun, error) {
	var runs []TrendRun
	err := filepath.Walk(historyDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if info.Name() != "benchmark-summary.json" {
			return nil
		}
		summary, readErr := ReadSummaryReport(path)
		if readErr != nil {
			return nil
		}
		// Trend needs timestamps: summaries without provenance (manual or
		// legacy artifacts) are skipped, never dereferenced.
		if summary.Provenance == nil {
			return nil
		}
		startedAt := summary.Provenance.StartedAt
		if startedAt.IsZero() {
			startedAt = summary.Provenance.CompletedAt
		}
		if startedAt.IsZero() {
			return nil
		}
		runs = append(runs, TrendRun{
			Path:      path,
			Summary:   summary,
			StartedAt: startedAt,
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk history directory: %w", err)
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].StartedAt.Before(runs[j].StartedAt) })
	return runs, nil
}

// buildComparabilityIdentity builds a string key that groups comparable benchmark runs.
// The identity combines mode, scale, distribution, tier profile, and workload set.
// Oracle modes are intentionally excluded so that oracle-mode changes are surfaced
// as methodology changes rather than splitting runs into separate comparability groups.
func buildComparabilityIdentity(summary SummaryReport) string {
	if summary.Provenance == nil {
		return summary.Metadata.BenchmarkID
	}
	names := make([]string, 0, len(summary.Workloads))
	for _, w := range summary.Workloads {
		names = append(names, w.Name)
	}
	sort.Strings(names)
	identity := fmt.Sprintf("%s|%s|%s|%s|%s",
		summary.Provenance.Mode,
		summary.Provenance.Scale,
		summary.Provenance.Distribution,
		summary.Provenance.TierProfile,
		strings.Join(names, ","))
	// Concurrent runs form their own comparability group so they never mix
	// into the sequential trend baseline; C<=1 keeps the legacy identity so
	// historical artifacts stay comparable.
	if summary.Provenance.Concurrency > 1 {
		identity = fmt.Sprintf("%s|c%d", identity, summary.Provenance.Concurrency)
	}
	return identity
}

// findCandidateRun returns the specified candidate path or the newest comparable run.
func findCandidateRun(runs []TrendRun, candidatePath string, comparabilityKey string) (TrendRun, []TrendRun, error) {
	var comparable []TrendRun
	for _, r := range runs {
		if buildComparabilityIdentity(r.Summary) == comparabilityKey {
			comparable = append(comparable, r)
		}
	}
	if len(comparable) == 0 {
		return TrendRun{}, nil, fmt.Errorf("no comparable runs found for identity %s", comparabilityKey)
	}
	if candidatePath != "" {
		for _, r := range comparable {
			if r.Path == candidatePath {
				return r, comparable, nil
			}
		}
		return TrendRun{}, nil, fmt.Errorf("candidate path %s not found in comparable runs", candidatePath)
	}
	return comparable[len(comparable)-1], comparable, nil
}

// selectBaseAndDriftWindows selects baseline and drift windows from comparable runs,
// excluding the candidate.
func selectBaseAndDriftWindows(comparable []TrendRun, candidate TrendRun, baselineWindow, driftWindow int) (baseline, drift []TrendRun) {
	var before []TrendRun
	for _, r := range comparable {
		if r.StartedAt.Before(candidate.StartedAt) {
			before = append(before, r)
		}
	}
	sort.Slice(before, func(i, j int) bool { return before[i].StartedAt.After(before[j].StartedAt) })
	if len(before) > baselineWindow {
		before = before[:baselineWindow]
	}
	sort.Slice(before, func(i, j int) bool { return before[i].StartedAt.Before(before[j].StartedAt) })
	baseline = before
	if driftWindow <= 0 || len(baseline) < baselineWindow {
		return baseline, nil
	}
	older := make([]TrendRun, 0)
	for _, r := range comparable {
		if r.StartedAt.Before(candidate.StartedAt) && !containsRun(baseline, r) {
			older = append(older, r)
		}
	}
	sort.Slice(older, func(i, j int) bool { return older[i].StartedAt.After(older[j].StartedAt) })
	if len(older) > driftWindow {
		older = older[:driftWindow]
	}
	sort.Slice(older, func(i, j int) bool { return older[i].StartedAt.Before(older[j].StartedAt) })
	return baseline, older
}

func containsRun(runs []TrendRun, target TrendRun) bool {
	for _, r := range runs {
		if r.Path == target.Path || r.StartedAt.Equal(target.StartedAt) {
			return true
		}
	}
	return false
}

// detectMethodologyChanges checks for oracle mode or workload set changes.
func detectMethodologyChanges(baseline, candidate SummaryReport) []string {
	var changes []string
	if len(baseline.OracleModes) != len(candidate.OracleModes) {
		changes = append(changes, "oracle mode count changed")
		return changes
	}
	for k, v := range baseline.OracleModes {
		if cv, ok := candidate.OracleModes[k]; !ok || cv != v {
			changes = append(changes, fmt.Sprintf("oracle mode for workload %s changed", k))
		}
	}
	baseNames := make(map[string]bool)
	for _, w := range baseline.Workloads {
		baseNames[w.Name] = true
	}
	for _, w := range candidate.Workloads {
		if !baseNames[w.Name] {
			changes = append(changes, fmt.Sprintf("new workload %s not present in baseline", w.Name))
		}
	}
	candNames := make(map[string]bool)
	for _, w := range candidate.Workloads {
		candNames[w.Name] = true
	}
	for _, w := range baseline.Workloads {
		if !candNames[w.Name] {
			changes = append(changes, fmt.Sprintf("workload %s removed in candidate", w.Name))
		}
	}
	return changes
}

// isWorkloadInstabilityChanged returns whether a workload's instability status changed
// between baseline and candidate. It checks changes in CorrectnessFailures (>0) as
// a proxy for hard-stop instability regressions.
func isWorkloadInstabilityChanged(base, cand WorkloadSummary) bool {
	baseUnstable := containsString(base.AssertionStats, func(s AssertionStat) bool { return s.Failed > 0 })
	candUnstable := containsString(cand.AssertionStats, func(s AssertionStat) bool { return s.Failed > 0 })
	return baseUnstable != candUnstable
}

func containsString(stats map[string]AssertionStat, predicate func(AssertionStat) bool) bool {
	for _, s := range stats {
		if predicate(s) {
			return true
		}
	}
	return false
}

// analyzeWorkloadTrends computes per-workload trends across baseline and candidate.
func analyzeWorkloadTrends(baselineRuns []TrendRun, candidate SummaryReport, protected map[string]bool) []WorkloadTrend {
	if len(baselineRuns) == 0 {
		return nil
	}
	baselineByWorkload := make(map[string][]WorkloadSummary)
	for _, r := range baselineRuns {
		for _, w := range r.Summary.Workloads {
			baselineByWorkload[w.Name] = append(baselineByWorkload[w.Name], w)
		}
	}
	var trends []WorkloadTrend
	for _, cw := range candidate.Workloads {
		bws, ok := baselineByWorkload[cw.Name]
		if !ok || !protected[cw.Name] && len(bws) == 0 {
			continue
		}
		if !protected[cw.Name] {
			continue
		}
		avgP95 := medianP95(bws)
		avgQPS := medianQPS(bws)
		p95Delta := cw.P95 - avgP95
		qpsDelta := cw.QPS - avgQPS
		var p95DeltaPct, qpsDeltaPct float64
		if avgP95 > 0 {
			p95DeltaPct = float64(p95Delta) / float64(avgP95) * 100
		}
		if avgQPS > 0 {
			qpsDeltaPct = qpsDelta / avgQPS * 100
		}
		instabilityChanged := false
		if len(bws) > 0 {
			instabilityChanged = isWorkloadInstabilityChanged(bws[0], cw)
		}
		correctnessDelta := 0
		correctnessChanged := false
		if len(bws) > 0 {
			correctnessDelta = cw.CorrectnessFailures - bws[0].CorrectnessFailures
			correctnessChanged = correctnessDelta != 0
		}
		classification := TrendClassStable
		if correctnessChanged && correctnessDelta > 0 {
			classification = TrendClassRegression
		} else if instabilityChanged {
			classification = TrendClassRegression
		} else if p95DeltaPct >= 10 {
			classification = TrendClassRegression
		} else if qpsDeltaPct <= -10 {
			classification = TrendClassRegression
		} else if p95DeltaPct <= -10 {
			classification = TrendClassImprovement
		}
		trends = append(trends, WorkloadTrend{
			Name:               cw.Name,
			TargetSchema:       cw.TargetSchema,
			BaselineP95:        avgP95,
			CandidateP95:       cw.P95,
			P95Delta:           p95Delta,
			P95DeltaPercent:    p95DeltaPct,
			BaselineQPS:        avgQPS,
			CandidateQPS:       cw.QPS,
			QPSDelta:           qpsDelta,
			QPSDeltaPercent:    qpsDeltaPct,
			CorrectnessChanged: correctnessChanged,
			CorrectnessDelta:   correctnessDelta,
			InstabilityChanged: instabilityChanged,
			Classification:     classification,
		})
	}
	sort.Slice(trends, func(i, j int) bool { return trends[i].Name < trends[j].Name })
	return trends
}

func medianP95(workloads []WorkloadSummary) time.Duration {
	if len(workloads) == 0 {
		return 0
	}
	durs := make([]time.Duration, len(workloads))
	for i, w := range workloads {
		durs[i] = w.P95
	}
	sort.Slice(durs, func(i, j int) bool { return durs[i] < durs[j] })
	mid := len(durs) / 2
	if len(durs)%2 == 0 {
		return (durs[mid-1] + durs[mid]) / 2
	}
	return durs[mid]
}

func medianQPS(workloads []WorkloadSummary) float64 {
	if len(workloads) == 0 {
		return 0
	}
	qpsVals := make([]float64, len(workloads))
	for i, w := range workloads {
		qpsVals[i] = w.QPS
	}
	sort.Float64s(qpsVals)
	mid := len(qpsVals) / 2
	if len(qpsVals)%2 == 0 {
		return (qpsVals[mid-1] + qpsVals[mid]) / 2
	}
	return qpsVals[mid]
}

func medianP95FromTrendRuns(runs []TrendRun) time.Duration {
	var all []time.Duration
	for _, r := range runs {
		for _, w := range r.Summary.Workloads {
			all = append(all, w.P95)
		}
	}
	if len(all) == 0 {
		return 0
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	mid := len(all) / 2
	if len(all)%2 == 0 {
		return (all[mid-1] + all[mid]) / 2
	}
	return all[mid]
}

// detectRegressions analyzes workload trends and flags regression candidates.
func detectRegressions(trends []WorkloadTrend, candidate SummaryReport) []RegressionCandidate {
	var regressions []RegressionCandidate
	candByName := make(map[string]WorkloadSummary, len(candidate.Workloads))
	for _, w := range candidate.Workloads {
		candByName[w.Name] = w
	}
	for _, t := range trends {
		cw := candByName[t.Name]
		if t.CorrectnessChanged && t.CorrectnessDelta > 0 {
			regressions = append(regressions, RegressionCandidate{
				WorkloadName:   t.Name,
				TargetSchema:   t.TargetSchema,
				Kind:           TrendKindCorrectness,
				BaselineValue:  float64(cw.CorrectnessFailures - t.CorrectnessDelta),
				CandidateValue: float64(cw.CorrectnessFailures),
				Delta:          float64(t.CorrectnessDelta),
				Severity:       TrendSeverityHardStop,
				Description:    fmt.Sprintf("correctness failures increased from %d to %d", cw.CorrectnessFailures-t.CorrectnessDelta, cw.CorrectnessFailures),
			})
		}
		if t.InstabilityChanged {
			regressions = append(regressions, RegressionCandidate{
				WorkloadName: t.Name,
				TargetSchema: t.TargetSchema,
				Kind:         TrendKindInstability,
				Severity:     TrendSeverityHardStop,
				Description:  "workload repeated-run stability status changed",
			})
		}
		if t.P95DeltaPercent >= 10 && t.Classification == TrendClassRegression {
			regressions = append(regressions, RegressionCandidate{
				WorkloadName:   t.Name,
				TargetSchema:   t.TargetSchema,
				Kind:           TrendKindP95Latency,
				BaselineValue:  float64(t.BaselineP95),
				CandidateValue: float64(t.CandidateP95),
				Delta:          float64(t.P95Delta),
				Threshold:      10,
				Severity:       TrendSeverityReview,
				Description:    fmt.Sprintf("p95 increased by %.1f%% from %s to %s", t.P95DeltaPercent, t.BaselineP95, t.CandidateP95),
			})
		}
		if t.QPSDeltaPercent <= -10 && t.Classification == TrendClassRegression {
			regressions = append(regressions, RegressionCandidate{
				WorkloadName:   t.Name,
				TargetSchema:   t.TargetSchema,
				Kind:           TrendKindQPSRegression,
				BaselineValue:  t.BaselineQPS,
				CandidateValue: t.CandidateQPS,
				Delta:          t.QPSDelta,
				Threshold:      -10,
				Severity:       TrendSeverityReview,
				Description:    fmt.Sprintf("qps dropped by %.1f%% from %.2f to %.2f", -t.QPSDeltaPercent, t.BaselineQPS, t.CandidateQPS),
			})
		}
	}
	return regressions
}

// detectBaselineDrift checks whether the baseline window has drifted from the
// drift window.
func detectBaselineDrift(baseline, drift []TrendRun) *DriftSignal {
	if len(drift) == 0 || len(baseline) == 0 {
		return nil
	}
	baselineP95 := medianP95FromTrendRuns(baseline)
	driftP95 := medianP95FromTrendRuns(drift)
	if baselineP95 <= 0 || driftP95 <= 0 {
		return nil
	}
	deltaPct := float64(baselineP95-driftP95) / float64(driftP95) * 100
	if deltaPct >= 10 {
		return &DriftSignal{
			Detected:    true,
			Description: fmt.Sprintf("baseline p95 is %.1f%% slower than drift window (%s vs %s)", deltaPct, baselineP95, driftP95),
			Evidence:    []string{fmt.Sprintf("baseline_median_p95=%s drift_median_p95=%s delta_pct=%.1f", baselineP95, driftP95, deltaPct)},
		}
	}
	return nil
}

// AnalyzeTrend is the main entry point for longitudinal trend analysis.
func AnalyzeTrend(historyDir, candidatePath string, baselineWindow, driftWindow int, protectedWorkloads []string) (TrendReport, error) {
	runs, err := ReadTrendHistory(historyDir)
	if err != nil {
		return TrendReport{}, err
	}
	if len(runs) == 0 {
		return TrendReport{}, fmt.Errorf("no historical runs found in %s", historyDir)
	}
	protectedSet := make(map[string]bool, len(protectedWorkloads))
	for _, name := range protectedWorkloads {
		protectedSet[name] = true
	}
	if candidatePath == "" {
		candidatePath = runs[len(runs)-1].Path
	}
	candidateSummary := runs[len(runs)-1].Summary
	if candidatePath != runs[len(runs)-1].Path {
		for _, r := range runs {
			if r.Path == candidatePath {
				candidateSummary = r.Summary
				break
			}
		}
	}
	compKey := buildComparabilityIdentity(candidateSummary)
	candidate, comparable, err := findCandidateRun(runs, candidatePath, compKey)
	if err != nil {
		return TrendReport{}, err
	}
	baseline, drift := selectBaseAndDriftWindows(comparable, candidate, baselineWindow, driftWindow)
	methodologyChanges := make([]string, 0)
	if len(baseline) > 0 {
		methodologyChanges = detectMethodologyChanges(baseline[0].Summary, candidate.Summary)
	}
	trends := analyzeWorkloadTrends(baseline, candidate.Summary, protectedSet)
	regressions := detectRegressions(trends, candidate.Summary)
	driftSignal := detectBaselineDrift(baseline, drift)
	status := TrendStatusPass
	hasHardStop := false
	for _, reg := range regressions {
		if reg.Severity == TrendSeverityHardStop {
			hasHardStop = true
			break
		}
	}
	if hasHardStop {
		status = TrendStatusHardStopRegression
	} else if len(methodologyChanges) > 0 {
		status = TrendStatusMethodology
	} else if driftSignal != nil && driftSignal.Detected {
		status = TrendStatusDrift
	} else if len(regressions) > 0 {
		status = TrendStatusRegression
	}
	stableWindows := len(baseline) == baselineWindow && driftWindow > 0 && len(drift) < driftWindow
	return TrendReport{
		Candidate:          candidate,
		BaselineWindow:     baseline,
		DriftWindow:        drift,
		ComparabilityKey:   compKey,
		MethodologyChanges: methodologyChanges,
		Regressions:        regressions,
		Drift:              driftSignal,
		WorkloadTrends:     trends,
		ProtectedWorkloads: protectedWorkloads,
		WindowsToStable:    stableWindows,
		Status:             status,
	}, nil
}

// FormatTrendSummary returns a stable console summary for trend analysis.
func FormatTrendSummary(report TrendReport) string {
	var b strings.Builder
	b.WriteString("Benchmark Trend Analysis\n")
	b.WriteString(fmt.Sprintf("candidate=%s comparability=%s status=%s baseline_window=%d drift_window=%d\n",
		report.Candidate.Summary.Metadata.BenchmarkID,
		report.ComparabilityKey,
		report.Status,
		len(report.BaselineWindow),
		len(report.DriftWindow),
	))
	if len(report.MethodologyChanges) > 0 {
		b.WriteString(fmt.Sprintf("methodology_changes=%s\n", strings.Join(report.MethodologyChanges, "; ")))
	}
	if report.Drift != nil && report.Drift.Detected {
		b.WriteString(fmt.Sprintf("baseline_drift=%s\n", report.Drift.Description))
	}
	if len(report.Regressions) > 0 {
		b.WriteString("regressions:\n")
		for _, reg := range report.Regressions {
			b.WriteString(fmt.Sprintf("  workload=%s kind=%s severity=%s baseline=%v candidate=%v delta=%v description=%s\n",
				reg.WorkloadName, reg.Kind, reg.Severity, reg.BaselineValue, reg.CandidateValue, reg.Delta, reg.Description))
		}
	}
	b.WriteString("workload trends:\n")
	for _, t := range report.WorkloadTrends {
		b.WriteString(fmt.Sprintf("  %s schema=%s classification=%s p95_baseline=%s p95_candidate=%s p95_delta=%s (%.1f%%) qps_baseline=%.2f qps_candidate=%.2f qps_delta=%.2f (%.1f%%) correctness_changed=%t instability_changed=%t\n",
			t.Name, t.TargetSchema, t.Classification, t.BaselineP95, t.CandidateP95, t.P95Delta, t.P95DeltaPercent, t.BaselineQPS, t.CandidateQPS, t.QPSDelta, t.QPSDeltaPercent, t.CorrectnessChanged, t.InstabilityChanged))
	}
	return b.String()
}

// WriteTrendReport writes a trend report to JSON.
func WriteTrendReport(path string, report *TrendReport) error {
	if report == nil {
		return fmt.Errorf("trend report cannot be nil")
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal trend report: %w", err)
	}
	return os.WriteFile(path, data, 0o644)
}

func compareWorkloadSummaries(baseline, candidate []WorkloadSummary) []WorkloadDiff {
	baselineByName := make(map[string]WorkloadSummary, len(baseline))
	candidateByName := make(map[string]WorkloadSummary, len(candidate))
	nameSet := make(map[string]struct{}, len(baseline)+len(candidate))
	for _, workload := range baseline {
		baselineByName[workload.Name] = workload
		nameSet[workload.Name] = struct{}{}
	}
	for _, workload := range candidate {
		candidateByName[workload.Name] = workload
		nameSet[workload.Name] = struct{}{}
	}
	names := make([]string, 0, len(nameSet))
	for name := range nameSet {
		names = append(names, name)
	}
	sort.Strings(names)
	diffs := make([]WorkloadDiff, 0, len(names))
	for _, name := range names {
		base, baseOK := baselineByName[name]
		cand, candOK := candidateByName[name]
		targetSchema := base.TargetSchema
		if targetSchema == "" {
			targetSchema = cand.TargetSchema
		}
		routeEngineChanged := false
		if len(base.RouteEngineCounts) > 0 && len(cand.RouteEngineCounts) > 0 {
			for engine := range base.RouteEngineCounts {
				if _, ok := cand.RouteEngineCounts[engine]; !ok {
					routeEngineChanged = true
					break
				}
			}
			if !routeEngineChanged {
				for engine := range cand.RouteEngineCounts {
					if _, ok := base.RouteEngineCounts[engine]; !ok {
						routeEngineChanged = true
						break
					}
				}
			}
		} else if len(base.RouteEngineCounts) != len(cand.RouteEngineCounts) {
			routeEngineChanged = true
		}
		diffs = append(diffs, WorkloadDiff{
			Name:                     name,
			TargetSchema:             targetSchema,
			MissingInBaseline:        !baseOK,
			MissingInCandidate:       !candOK,
			PassedChanged:            base.Passed != cand.Passed,
			FailureCountDelta:        cand.FailureCount - base.FailureCount,
			CorrectnessFailuresDelta: cand.CorrectnessFailures - base.CorrectnessFailures,
			InfraFailuresDelta:       cand.InfraFailures - base.InfraFailures,
			ExecutionCountDelta:      cand.ExecutionCount - base.ExecutionCount,
			QPSDelta:                 cand.QPS - base.QPS,
			AvgLatencyDelta:          cand.Avg - base.Avg,
			P95LatencyDelta:          cand.P95 - base.P95,
			P99LatencyDelta:          cand.P99 - base.P99,
			AvgResultCountDelta:      cand.AvgResultCount - base.AvgResultCount,
			AvgTotalRecordsDelta:     cand.AvgTotalRecords - base.AvgTotalRecords,
			RouteEngineChanged:       routeEngineChanged,
		})
	}
	return diffs
}
