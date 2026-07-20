package cdc

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const redactSecret = "secret123"

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

// TestRedactConnStr_EscapedQuotedDSN pins the #290 regression: the flusher/init
// feed BuildPGDSN's quoted DSN through escapeLiteral (doubling single quotes)
// into a DuckDB ATTACH literal, then log it via redactConnStr. The doubled-quote
// form (password=''secret'') must still be fully redacted — the old regex
// matched the empty string between the doubled quotes and leaked the password.
func TestRedactConnStr_EscapedQuotedDSN(t *testing.T) {
	sqlLiteral := escapeLiteral(BuildPGDSN(redactTestParams()))
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
