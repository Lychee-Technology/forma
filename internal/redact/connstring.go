// Package redact removes credential material from strings before they leave the
// process — into a log sink, or into an HTTP response body.
//
// It exists because the same connection-string password has to be scrubbed in two
// unrelated places, and the matcher is subtle enough that two copies would drift:
//
//   - internal/cdc logs DuckDB ATTACH statements and DSNs (#290).
//   - internal/httpapi logs error chains, and DuckDB quotes the whole postgres_scan
//     connection string back inside its own attach-failure prose, so the password
//     arrives as driver-authored text no Forma wrap site can intercept (#301).
//
// A weaker matcher is not a smaller version of this one, it is a leak: a naive
// `'[^']*'` branch mistakes libpq's escaped `\'` for the closing quote and emits
// the password tail past the placeholder. That was a real bug, fixed in #290, and
// the regression tests for it live in internal/cdc/redact_test.go.
package redact

import "regexp"

// Placeholder replaces every matched credential value.
const Placeholder = "***REDACTED***"

// connPassword matches the password= key-value pair in a libpq-style connection
// string, including the quoted forms used inside DuckDB ATTACH statements. The
// alternation is ordered from most- to least-quoted so each form is consumed whole
// and no password material survives past the placeholder. libpq quoting escapes
// both ' and \ with a backslash (quotePGConnValue), and escapeLiteral additionally
// doubles every ' when the DSN is embedded in a DuckDB SQL literal — so the
// matcher must understand backslash escapes, not just balanced quotes.
//
//   - Branch 1 — password=''…''  the escapeLiteral'd doubled form (tried first).
//     Content atoms are `\''` (a libpq escaped quote whose ' was doubled by
//     escapeLiteral), `\[^']` (any other backslash escape, incl. `\\`), and
//     `[^'\]` (a plain char). A bare `'` never matches an atom, so the closing `''`
//     is unreachable from inside the content: greedy matching is safe and the old
//     non-greedy early-close leak is structurally gone.
//   - Branch 2 — password='…'  the raw quoted BuildPGDSN output. Standard libpq
//     quoted value `'(?:\.|[^'\])*'` consumes `\'` and `\\` as escapes instead of
//     terminating on the escaped quote (the old '[^']*' branch mistook `\'` for the
//     closing quote).
//   - Branch 3 — password=value  the legacy unquoted form. This is the shape
//     internal/federated/engine.go builds for postgres_scan and the one DuckDB
//     echoes back on an attach failure.
//
// Known limitation, deliberate: whitespace around the `=` is not matched, so
// `password = secret` passes through. No producer in this repo emits that form —
// BuildPGDSN and DuckDBPostgresConnStringFromPool both write `password=` with no
// spaces, and DuckDB echoes what it was given. The pattern is kept byte-identical
// to the one #290 hardened rather than widened on speculation; widen it here, in
// one place, if a producer ever appears.
var connPassword = regexp.MustCompile(`(?i)(password=)(''(?:\\''|\\[^']|[^'\\])*''|'(?:\\.|[^'\\])*'|[^' \t\r\n;,)]*)`)

// ConnStringPassword replaces any password value in a connection string, or in an
// SQL string or error message that embeds one, with Placeholder. It is safe to
// call on any string, whether or not it contains a password.
//
// Only the credential is removed. Everything else — hosts, database names, object
// keys, driver prose — survives, because the redacted output is usually an
// operator's only remaining copy of the diagnosis.
func ConnStringPassword(s string) string {
	return connPassword.ReplaceAllString(s, "${1}"+Placeholder)
}
