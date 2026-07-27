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
//   - internal/federated scrubs driver errors at the source (#306): the engine
//     wraps duck.Query failures through redact.Error before the text enters an
//     error chain or the internal execution plan, so embedders that log the
//     engine's errors never capture the credential.
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
// string, including the quoted forms used inside DuckDB ATTACH statements and
// postgres_scan literals. The alternation is ordered from most- to least-quoted so
// each form is consumed whole and no password material survives past the
// placeholder.
//
// libpq quoting escapes both a single quote and a backslash with a backslash
// (pgdsn.Quote), and escapeLiteral additionally doubles every single quote when the
// DSN is embedded in a DuckDB SQL literal — so the matcher must understand
// backslash escapes, not just balanced quotes.
//
// The three branches, written as an indented block on purpose: gofmt rewrites a
// bare pair of single quotes in comment prose into a typographic quote, which
// would silently corrupt the very sequences being described.
//
//	Branch 1  password=''VALUE''   the escapeLiteral'd doubled form, tried first.
//	          content atoms:  \''  a libpq escaped quote whose ' was doubled
//	                          \[^'] any other backslash escape, incl. \\
//	                          [^'\] a plain character
//	          A bare ' never matches an atom, so the closing pair is unreachable
//	          from inside the content: greedy matching is safe and the old
//	          non-greedy early-close leak is structurally gone.
//
//	Branch 2  password='VALUE'     the raw quoted pgdsn.Quote output. The body
//	          '(?:\.|[^'\])*' consumes \' and \\ as escapes instead of
//	          terminating on the escaped quote (the old '[^']*' branch mistook
//	          \' for the closing quote — the #290 bug).
//
//	Branch 3  password=VALUE       the unquoted form. It terminates only on a
//	          quote or whitespace, and deliberately NOT on ; , or ) — those are
//	          not separators in a libpq keyword/value DSN, and stopping on them
//	          left the tail of any password containing one exposed. Over-consuming
//	          here is harmless; under-consuming is a leak.
//
// Known limitation, deliberate: whitespace around the = is not matched, so
// "password = secret" passes through. No producer in this repo emits that form —
// pgdsn.Build and DuckDBPostgresConnStringFromPool both write password= with no
// spaces, and DuckDB echoes back what it was given.
//
// Residual: an *unquoted* value containing a space cannot be fully matched by any
// pattern, because nothing marks where such a value ends. The fix for that belongs
// at the producer, and #301 applied it — DuckDBPostgresConnStringFromPool now
// quotes its values, as internal/cdc's builder already did. Both DSN builders in
// this repo therefore land on branch 1 or 2; branch 3 remains for third-party or
// legacy text.
var connPassword = regexp.MustCompile(`(?i)(password=)(''(?:\\''|\\[^']|[^'\\])*''|'(?:\\.|[^'\\])*'|[^'\s]*)`)

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
