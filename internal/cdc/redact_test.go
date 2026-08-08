package cdc

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/sqlutil"
	"github.com/stretchr/testify/require"
)

const redactSecret = "secret123"

// redactHostileSecret is a password carrying both an embedded single quote and a
// backslash (Go literal for p'w\d). Once quoted by BuildPGDSN it becomes
// password='p\'w\\d', whose escaped quote \' fools a naive '[^']*' matcher into
// treating the escape as the closing quote and leaking the tail (#290).
const redactHostileSecret = "p'w\\d"

// redactHostileFragment is the leaking password tail as it appears *after*
// quoting: pgdsn.Quote doubles the backslash, so the tail that a naive
// matcher leaks is w\\d (Go literal for w, backslash, backslash, d). This is
// the exact material the buggy '[^']*' branch surfaced past ***REDACTED***; it
// must never survive redaction in any DSN form.
const redactHostileFragment = "w\\\\d"

func redactTestParams() PGDSNParams {
	return PGDSNParams{
		Host:     "db.internal",
		Port:     5432,
		User:     "flusher",
		Password: redactSecret,
		DB:       "forma",
		SSLMode:  "require",
	}
}

func redactHostileParams() PGDSNParams {
	p := redactTestParams()
	p.Password = redactHostileSecret
	return p
}

// TestRedactConnStr_HostilePasswordRawQuoted feeds a password with an embedded
// quote+backslash through BuildPGDSN's raw quoted form. The escaped \' must not
// be mistaken for the closing quote: neither the password nor its w\d tail may
// leak, and the trailing keys must survive un-mangled.
func TestRedactConnStr_HostilePasswordRawQuoted(t *testing.T) {
	dsn := BuildPGDSN(redactHostileParams())
	redacted := redactConnStr(dsn)
	t.Logf("raw dsn:      %q", dsn)
	t.Logf("raw redacted: %q", redacted)

	require.NotContains(t, redacted, redactHostileFragment, "password tail leaked in redacted output: %q", redacted)
	require.NotContains(t, redacted, redactHostileSecret, "full password leaked in redacted output: %q", redacted)
	require.Contains(t, redacted, "***REDACTED***")
	require.Contains(t, redacted, "dbname=")
	require.Contains(t, redacted, "sslmode=")
}

// TestRedactConnStr_HostilePasswordEscapedQuoted feeds the same hostile password
// through the sqlutil.EscapeLiteral'd (doubled-quote) form embedded in a DuckDB literal.
func TestRedactConnStr_HostilePasswordEscapedQuoted(t *testing.T) {
	sqlLiteral := sqlutil.EscapeLiteral(BuildPGDSN(redactHostileParams()))
	redacted := redactConnStr(sqlLiteral)
	t.Logf("escaped literal:  %q", sqlLiteral)
	t.Logf("escaped redacted: %q", redacted)

	require.NotContains(t, redacted, redactHostileFragment, "password tail leaked in redacted output: %q", redacted)
	require.NotContains(t, redacted, redactHostileSecret, "full password leaked in redacted output: %q", redacted)
	require.Contains(t, redacted, "***REDACTED***")
	// In the escaped form trailing keys look like dbname=''forma''.
	require.Contains(t, redacted, "dbname=''")
	require.Contains(t, redacted, "sslmode=''")
}

// TestRedactConnStr_EmptyPasswordRawQuoted pins that an empty password does
// not derail redaction of surrounding keys. The raw-quoted form is (kept in a
// code block because gofmt rewrites doubled single quotes into a typographic
// quote when they sit in comment prose, #276):
//
//	password=''
func TestRedactConnStr_EmptyPasswordRawQuoted(t *testing.T) {
	p := redactTestParams()
	p.Password = ""
	redacted := redactConnStr(BuildPGDSN(p))
	require.Contains(t, redacted, "***REDACTED***")
	require.Contains(t, redacted, "dbname=")
	require.Contains(t, redacted, "sslmode=")
}

// TestRedactConnStr_EmptyPasswordEscapedQuoted pins the doubled-quote empty
// form — the branch-1 fallback must not swallow the trailing keys:
//
//	password=''''
func TestRedactConnStr_EmptyPasswordEscapedQuoted(t *testing.T) {
	p := redactTestParams()
	p.Password = ""
	redacted := redactConnStr(sqlutil.EscapeLiteral(BuildPGDSN(p)))
	require.Contains(t, redacted, "***REDACTED***")
	require.Contains(t, redacted, "dbname=''")
	require.Contains(t, redacted, "sslmode=''")
}

// TestRedactConnStr_EscapedQuotedDSN pins the #290 regression: the flusher/init
// feed BuildPGDSN's quoted DSN through sqlutil.EscapeLiteral (doubling single quotes)
// into a DuckDB ATTACH literal, then log it via redactConnStr. The doubled-quote
// form must still be fully redacted — the old regex matched the empty string
// between the doubled quotes and leaked the password:
//
//	password=''secret''
func TestRedactConnStr_EscapedQuotedDSN(t *testing.T) {
	sqlLiteral := sqlutil.EscapeLiteral(BuildPGDSN(redactTestParams()))
	require.Contains(t, sqlLiteral, "password=''"+redactSecret+"''",
		"precondition: escaped DSN must carry the doubled-quote form")

	redacted := redactConnStr(sqlLiteral)
	require.NotContains(t, redacted, redactSecret, "password leaked in redacted output: %q", redacted)
	require.Contains(t, redacted, "***REDACTED***")
	// The trailing keys after password must survive redaction.
	require.Contains(t, redacted, "dbname=")
}

// TestRedactConnStr_PlainQuotedDSN pins the un-escaped quoted form as produced
// by BuildPGDSN directly (password='secret').
func TestRedactConnStr_PlainQuotedDSN(t *testing.T) {
	redacted := redactConnStr(BuildPGDSN(redactTestParams()))
	require.NotContains(t, redacted, redactSecret, "password leaked: %q", redacted)
	require.Contains(t, redacted, "***REDACTED***")
	require.Contains(t, redacted, "dbname=")
}

// TestRedactConnStr_LegacyUnquotedDSN pins the pre-#290 bare form
// (password=secret) so the fix keeps redacting it.
func TestRedactConnStr_LegacyUnquotedDSN(t *testing.T) {
	legacy := "host=db.internal port=5432 user=flusher password=" + redactSecret + " dbname=forma sslmode=require"
	redacted := redactConnStr(legacy)
	require.NotContains(t, redacted, redactSecret, "password leaked: %q", redacted)
	require.Contains(t, redacted, "***REDACTED***")
	require.Contains(t, redacted, "dbname=forma")
	require.True(t, strings.HasPrefix(redacted, "host=db.internal"))
}
