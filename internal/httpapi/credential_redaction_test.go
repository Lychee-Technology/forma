package httpapi

import (
	"strings"
	"testing"
)

// TestRedactCredentialsRemovesPasswordValues pins the scrubber against the
// conninfo shapes this repo actually produces. The first case is the literal one
// DuckDB emits when postgres_scan cannot attach — it quotes the whole connection
// string back inside its own prose, password included.
//
// The matcher is internal/redact's, shared with the CDC logger, so these cases
// document the HTTP boundary's use of it rather than re-testing the pattern; the
// #290 hostile-DSN regressions live in internal/cdc/redact_test.go.
func TestRedactCredentialsRemovesPasswordValues(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "duckdb attach failure quotes the whole conninfo",
			in:   `IO Error: Unable to connect to Postgres at "host=h port=5432 user=u password=s3cr3t dbname=d"`,
			want: `IO Error: Unable to connect to Postgres at "host=h port=5432 user=u password=***REDACTED*** dbname=d"`,
		},
		{
			name: "value at end of string",
			in:   "host=h user=u password=s3cr3t",
			want: "host=h user=u password=***REDACTED***",
		},
		{
			name: "single-quoted value with spaces",
			in:   "host=h password='two words' dbname=d",
			want: "host=h password=***REDACTED*** dbname=d",
		},
		{
			// The escaped quote must not be mistaken for the closing one. A naive
			// `'[^']*'` branch stops at the `\'` and emits the tail — the #290 bug,
			// and the reason this package borrows the CDC matcher instead of
			// carrying its own.
			name: "libpq-escaped quote inside the value",
			in:   `host=h password='p\'w\\d' dbname=forma`,
			want: "host=h password=***REDACTED*** dbname=forma",
		},
		{
			// sqlutil.EscapeLiteral doubles every quote when a DSN is embedded in a DuckDB
			// SQL literal.
			name: "EscapeLiteral doubled-quote form",
			in:   `ATTACH 'host=h password=''s3cr3t'' dbname=forma'`,
			want: `ATTACH 'host=h password=***REDACTED*** dbname=forma'`,
		},
		{
			name: "uppercase key keeps its case, value still goes",
			in:   "PASSWORD=s3cr3t dbname=d",
			want: "PASSWORD=***REDACTED*** dbname=d",
		},
		{
			name: "two occurrences both go",
			in:   "a password=one b password=two c",
			want: "a password=***REDACTED*** b password=***REDACTED*** c",
		},
		{
			name: "empty value leaves the following keys intact",
			in:   "password= dbname=d",
			want: "password=***REDACTED*** dbname=d",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := redactCredentials(tt.in); got != tt.want {
				t.Fatalf("redactCredentials(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestRedactCredentialsKnownGaps records the shapes the matcher does NOT cover,
// so the boundary of the control is written down rather than discovered.
//
// An earlier version of this test named double-quoting as the residual. That was
// wrong on both halves: `password="SECRET"` is fully redacted (the bare branch
// consumes the quotes along with the value), and the shape that actually survives
// is an *unquoted value containing a space* — which no pattern can match, because
// nothing marks where such a value ends.
//
// That residual is closed at the producer rather than here. Both DSN builders in
// this repo now quote their values — internal/cdc's since #290,
// federated.DuckDBPostgresConnStringFromPool since #301 — so every password this
// repo generates lands on a quoted branch and is matched whole. What remains
// uncovered is third-party or legacy unquoted text.
func TestRedactCredentialsKnownGaps(t *testing.T) {
	t.Run("whitespace around the equals is not matched", func(t *testing.T) {
		in := "password = s3cr3t dbname=d"
		if got := redactCredentials(in); got != in {
			t.Fatalf("gap closed — update this test and the redact package docs: %q", got)
		}
	})

	t.Run("an unquoted value containing a space keeps its tail", func(t *testing.T) {
		got := redactCredentials("host=h password=my secret dbname=d")
		if !strings.Contains(got, "secret") {
			t.Fatalf("residual closed — update this test and the redact package docs: %q", got)
		}
	})

	t.Run("quoting at the producer is what closes it", func(t *testing.T) {
		got := redactCredentials(`host='h' password='my secret' dbname='d'`)
		if strings.Contains(got, "secret") {
			t.Fatalf("a quoted value must be redacted whole, got %q", got)
		}
	})

	t.Run("double-quoted values are in fact covered", func(t *testing.T) {
		got := redactCredentials(`host=h password="SECRET" dbname=d`)
		if strings.Contains(got, "SECRET") {
			t.Fatalf("expected the bare branch to consume the quotes too, got %q", got)
		}
	})
}

// TestRedactCredentialsKeepsOperatorDetail is the other half of the contract and
// the reason the pattern is narrow rather than a blanket scrub. A redacted
// response — or, since #361, a withheld-detail disclosed 4xx — leaves the
// operator with an error_id and a log line; if that line lost the object key,
// the schema id, the endpoint, or the driver's own diagnosis, there would be
// nothing to correlate the id against.
func TestRedactCredentialsKeepsOperatorDetail(t *testing.T) {
	in := `execute duckdb query: parquet set inconsistent for schema 22: manifest lists ` +
		`base/schema_22/CANARY-KEY.parquet: HTTP Error: Unable to connect to URL ` +
		`"https://bucket.s3.amazonaws.com/base/schema_22/CANARY-KEY.parquet": 404 (Not Found)`

	got := redactCredentials(in)
	if got != in {
		t.Fatalf("scrubber altered an error with no credential in it:\n got: %s\nwant: %s", got, in)
	}
	for _, required := range []string{
		"base/schema_22/CANARY-KEY.parquet", "schema 22", "404 (Not Found)",
		"https://bucket.s3.amazonaws.com", "HTTP Error",
	} {
		if !strings.Contains(got, required) {
			t.Fatalf("operator detail %q was scrubbed; got: %s", required, got)
		}
	}
}

// TestRedactCredentialsLeavesUnrelatedWordsAlone guards against the pattern
// widening into prose that merely mentions passwords.
func TestRedactCredentialsLeavesUnrelatedWordsAlone(t *testing.T) {
	for _, in := range []string{
		`password authentication failed for user "u"`,
		"the password_hash column is not populated",
		"missing attribute 'password' in schema 'account'",
	} {
		if got := redactCredentials(in); got != in {
			t.Fatalf("redactCredentials(%q) = %q, want it unchanged", in, got)
		}
	}
}
