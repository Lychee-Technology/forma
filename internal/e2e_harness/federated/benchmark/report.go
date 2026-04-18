package benchmark

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// SummaryReport captures aggregated execution metrics.
type SummaryReport struct {
	ExecutionCount int                      `json:"execution_count"`
	P50            time.Duration            `json:"p50"`
	P95            time.Duration            `json:"p95"`
	P99            time.Duration            `json:"p99"`
	Max            time.Duration            `json:"max"`
	AssertionStats map[string]AssertionStat `json:"assertion_stats"`
}

// AssertionStat captures aggregate assertion pass/fail counts.
type AssertionStat struct {
	Passed int `json:"passed"`
	Failed int `json:"failed"`
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
	b.WriteString(fmt.Sprintf("- Validation only: %t\n", result.ValidationOnly))
	b.WriteString(fmt.Sprintf("- Distribution: %s\n", result.Generator.Distribution))
	b.WriteString(fmt.Sprintf("- Scale: %s\n", result.Generator.Scale))
	b.WriteString(fmt.Sprintf("- Executions: %d\n", len(result.Executions)))
	b.WriteString(fmt.Sprintf("- P50: %s\n", summary.P50))
	b.WriteString(fmt.Sprintf("- P95: %s\n", summary.P95))
	b.WriteString(fmt.Sprintf("- P99: %s\n", summary.P99))
	b.WriteString(fmt.Sprintf("- Max: %s\n", summary.Max))
	if len(result.Executions) > 0 {
		b.WriteString("\n## Executions\n\n")
		for _, execution := range result.Executions {
			b.WriteString(fmt.Sprintf("- `%s`: count=%d total=%d duration=%s offset=%d\n", execution.Name, execution.ResultCount, execution.TotalRecords, execution.Duration, execution.Offset))
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

// SummarizeRunResult aggregates durations and assertion outcomes.
func SummarizeRunResult(result *RunResult) SummaryReport {
	summary := SummaryReport{AssertionStats: make(map[string]AssertionStat)}
	if result == nil || len(result.Executions) == 0 {
		return summary
	}
	durations := make([]time.Duration, 0, len(result.Executions))
	for _, execution := range result.Executions {
		durations = append(durations, execution.Duration)
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
	summary.P50 = percentileDuration(durations, 0.50)
	summary.P95 = percentileDuration(durations, 0.95)
	summary.P99 = percentileDuration(durations, 0.99)
	summary.Max = durations[len(durations)-1]
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
