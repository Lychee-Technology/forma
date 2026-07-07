package benchmark

import "time"

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
