package sqlutil

import "strings"

// EscapeLiteral doubles single quotes so a value can be embedded inside a
// single-quoted SQL string literal. PostgreSQL and DuckDB share the doubling
// rule, and both treat backslashes in a plain '…' literal as ordinary
// characters, so doubling quotes is the entire rule. It does not add the
// surrounding quotes, and it is only correct for string-literal context —
// use SanitizeIdentifier for identifiers.
//
// This is the single home of the rule; #307 showed it is load-bearing (an
// unescaped credential in postgres_scan('…') put deployment-configured text
// into SQL structure), and #310 consolidated the copies that previously
// lived in internal/cdc, internal/sqlgen, and the federated e2e harness.
func EscapeLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
