package federated

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma/internal/redact"
)

// newTestPool builds a pgxpool from a DSN without contacting Postgres. MinConns
// defaults to 0, so no connection is established and Config() is available
// immediately — enough to exercise the conn-string derivation.
func newTestPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse pool config: %v", err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// TestDuckDBPostgresConnStringQuotesValues pins the #301 producer fix. The DSN
// this function builds is interpolated into postgres_scan and is the string DuckDB
// quotes back on an attach failure, so it has to be both parseable and
// redactable — an unquoted value is neither.
func TestDuckDBPostgresConnStringQuotesValues(t *testing.T) {
	pool := newTestPool(t, `host=127.0.0.1 port=5432 user='u u' password='p\'w\\d' dbname='d b'`)

	got := DuckDBPostgresConnStringFromPool(pool)
	want := `host='127.0.0.1' port=5432 user='u u' password='p\'w\\d' dbname='d b'`
	if got != want {
		t.Fatalf("conn string =\n %s\nwant\n %s", got, want)
	}
}

// TestDuckDBPostgresConnStringIsFullyRedactable is the property that actually
// matters for #301, asserted end to end rather than by inspecting the format.
//
// Before the producer quoted its values, a password containing a space or one of
// `, ; )` truncated under redaction and its tail was logged at ERROR — the exact
// exposure #301 exists to close. Quoting is what makes the value's extent
// unambiguous to the matcher.
func TestDuckDBPostgresConnStringIsFullyRedactable(t *testing.T) {
	secrets := map[string]string{
		"space":            "my secret",
		"comma":            "ZQX,VKW",
		"semicolon":        "ZQX;VKW",
		"close paren":      "ZQX)VKW",
		"quote":            `ZQX'VKW`,
		"backslash":        `ZQX\VKW`,
		"quote and space":  `ZQX' VKW`,
		"all of the above": `ZQX,' ;)VKW`,
	}

	for name, secret := range secrets {
		t.Run(name, func(t *testing.T) {
			pool := newTestPool(t, "host=127.0.0.1 port=5432 user=u dbname=d password="+quoteForTestDSN(secret))

			connStr := DuckDBPostgresConnStringFromPool(pool)
			if !strings.Contains(connStr, "password=") {
				t.Fatalf("precondition failed: no password in %q", connStr)
			}

			redacted := redact.ConnStringPassword(connStr)
			for _, frag := range strings.FieldsFunc(secret, func(r rune) bool {
				return strings.ContainsRune(`,;)' \`, r)
			}) {
				if strings.Contains(redacted, frag) {
					t.Fatalf("fragment %q of password %q survived redaction: %q", frag, secret, redacted)
				}
			}
			// The rest of the DSN must survive — it is the operator's diagnosis.
			if !strings.Contains(redacted, "dbname='d'") {
				t.Fatalf("redaction consumed trailing keys: %q", redacted)
			}
		})
	}
}

// quoteForTestDSN renders a value for a libpq DSN we hand to ParseConfig, so the
// test can feed hostile passwords in without the parser mangling them.
func quoteForTestDSN(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `'`, `\'`)
	return "'" + v + "'"
}

// TestDuckDBPostgresConnStringNilSafety pins the documented "" returns.
func TestDuckDBPostgresConnStringNilSafety(t *testing.T) {
	if got := DuckDBPostgresConnStringFromPool(nil); got != "" {
		t.Fatalf("nil pool = %q, want empty", got)
	}
}
