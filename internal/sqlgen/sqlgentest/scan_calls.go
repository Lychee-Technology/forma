// Package sqlgentest provides shared helpers for tests that pin the
// postgres_scan contract on both sides: the runtime template
// (internal/sqlgen) and the executable §5 sketch in
// docs/federated-query/design.md (internal/federated).
package sqlgentest

import "strings"

// FindPostgresScanCalls returns the raw argument text of every postgres_scan
// invocation in sqlText, matching parentheses so multi-line and nested forms
// are captured whole.
func FindPostgresScanCalls(sqlText string) []string {
	const marker = "postgres_scan("
	var calls []string
	for i := 0; ; {
		idx := strings.Index(sqlText[i:], marker)
		if idx < 0 {
			break
		}
		start := i + idx + len(marker)
		depth := 1
		j := start
		for ; j < len(sqlText) && depth > 0; j++ {
			switch sqlText[j] {
			case '(':
				depth++
			case ')':
				depth--
			}
		}
		calls = append(calls, strings.TrimSpace(sqlText[start:j-1]))
		i = j
	}
	return calls
}
