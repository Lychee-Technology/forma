package benchmark

import (
	"encoding/json"
	"fmt"
	"os"
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
	var b strings.Builder
	b.WriteString("# Federated Query Benchmark Report\n\n")
	b.WriteString(fmt.Sprintf("- Validation only: %t\n", result.ValidationOnly))
	b.WriteString(fmt.Sprintf("- Distribution: %s\n", result.Generator.Distribution))
	b.WriteString(fmt.Sprintf("- Scale: %s\n", result.Generator.Scale))
	b.WriteString(fmt.Sprintf("- Executions: %d\n", len(result.Executions)))
	if len(result.Executions) > 0 {
		b.WriteString("\n## Executions\n\n")
		for _, execution := range result.Executions {
			b.WriteString(fmt.Sprintf("- `%s`: count=%d total=%d duration=%s offset=%d\n", execution.Name, execution.ResultCount, execution.TotalRecords, execution.Duration, execution.Offset))
		}
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}
