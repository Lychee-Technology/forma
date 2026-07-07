package benchmark

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

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
