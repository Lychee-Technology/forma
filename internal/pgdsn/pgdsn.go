// Package pgdsn builds libpq keyword/value connection strings.
//
// It exists so the quoting rule has one implementation. Two subsystems hand a
// DSN to DuckDB's postgres scanner — internal/cdc (ATTACH, for CDC export) and
// internal/federated (postgres_scan, for the federated read path) — and an
// unquoted value is two bugs at once: a password containing a space produces a
// DSN libpq cannot parse, and the credential scrubber in internal/redact cannot
// tell where an unquoted value ends, so it truncates at the first separator and
// leaks the tail (#301).
package pgdsn

import (
	"fmt"
	"strings"
)

// Params are the inputs for a libpq keyword/value connection string.
type Params struct {
	Host     string
	Port     int
	User     string
	Password string
	DB       string
	SSLMode  string
}

// Build renders a libpq keyword/value DSN with every string value quoted, so
// values containing spaces or quotes survive parsing by pgx and by DuckDB's
// postgres scanner.
func Build(p Params) string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		Quote(p.Host), p.Port, Quote(p.User),
		Quote(p.Password), Quote(p.DB), Quote(p.SSLMode))
}

// Quote wraps a value in single quotes, escaping backslashes and single quotes
// with a backslash as libpq requires.
//
// The escaping order matters: backslashes first, then quotes, so the backslash
// introduced for an escaped quote is not itself doubled.
func Quote(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `'`, `\'`)
	return "'" + v + "'"
}
