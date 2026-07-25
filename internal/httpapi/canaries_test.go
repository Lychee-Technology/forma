package httpapi

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
)

// This file holds the leak fixtures shared by error_response_test.go and
// error_leak_test.go: the canary strings, the predicate that decides whether a
// string leaked one, and the two error chains shaped like what the federated
// engine really returns.
//
// They live together because the canaries are only as good as their construction,
// and that reasoning is easier to keep honest in one place than spread across two
// suites that both depend on it.

// leakCanaries are the strings that must never appear in a response body.
// The password one is the real shape: DuckDB's postgres_scan attach failure puts
// the connection string — password included — into its own prose, verified
// against duckdb-go v2.5.6. The key one is ParquetSetInconsistentError.MissingKeys.
//
// The secret canary is built to exhibit both failure modes this PR fixed, because
// asserting the wrong string makes the whole suite decorative:
//
//   - canaryPassword carries the `password=` prefix. Asserting only its absence
//     passes a regression that strips the prefix and leaves the secret, so
//     canarySecret — the bare value — is asserted too.
//   - canarySecret contains a `;`, which the scrubber's bare-value branch used to
//     treat as a terminator. That is what makes a *truncation* regression visible
//     here: revert the branch and the match stops at the semicolon, so
//     canarySecretTail survives in the log while canarySecret (the whole value)
//     does not. Assert the tail as well, or the truncation class this PR is about
//     goes ungated in this package.
const (
	canarySecretHead = "SUPERSECRET"
	canarySecretTail = "CANARY-TAIL"
	canarySecret     = canarySecretHead + ";" + canarySecretTail
	canaryPassword   = "password=" + canarySecret
	canaryKey        = "base/schema_22/CANARY-KEY.parquet"
)

// leaksCanarySecret reports whether s carries any part of the credential canary:
// the whole assignment, the whole value, or the tail left behind by a matcher
// that stops at the semicolon. All three are leaks, and each corresponds to a
// distinct regression shape.
func leaksCanarySecret(s string) bool {
	return strings.Contains(s, canaryPassword) ||
		strings.Contains(s, canarySecret) ||
		strings.Contains(s, canarySecretHead) ||
		strings.Contains(s, canarySecretTail)
}

// operatorDetailError builds an error chain shaped like the one the federated
// engine actually returns: a typed read-path carrier wrapping raw driver text.
func operatorDetailError() error {
	return fmt.Errorf("execute duckdb query: %w: %w",
		&forma.ParquetSetInconsistentError{SchemaID: 22, MissingKeys: []string{canaryKey}},
		fmt.Errorf(`IO Error: Unable to connect to Postgres at "host=h port=5432 user=u %s dbname=d"`, canaryPassword))
}

// driverNotFoundTextError reproduces the #301 leak that status-based gating
// missed. DuckDB reports a missing S3 object with the literal words "404 (Not
// Found)", which the old substring heuristic read as a client 404 — routing a
// read-path failure, S3 URL and connection string included, at the verbatim
// branch, and answering a storage failure with the HTTP status for "the resource
// does not exist".
func driverNotFoundTextError() error {
	return fmt.Errorf("execute duckdb query: %w: %w",
		&forma.ParquetSetInconsistentError{SchemaID: 22, MissingKeys: []string{canaryKey}},
		fmt.Errorf(`HTTP Error: Unable to connect to URL "https://b.s3.amazonaws.com/%s": 404 (Not Found). `+
			`Also failed to attach postgres_scan with "host=h user=u %s"`, canaryKey, canaryPassword))
}
