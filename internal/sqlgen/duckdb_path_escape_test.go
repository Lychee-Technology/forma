package sqlgen

import (
	"strings"
	"testing"
)

// TestFormatDuckDBPathListEscapesQuotes pins #456: a parquet path containing a
// single quote is doubled, so it cannot break out of the read_parquet literal.
func TestFormatDuckDBPathListEscapesQuotes(t *testing.T) {
	got := formatDuckDBPathList([]string{"s3://b/x.parquet') UNION ALL SELECT 1 --"})
	// Anchor on the payload prefix: an unescaped path renders "parquet') UNION"
	// (a lone quote closing the literal); a correctly doubled path renders
	// "parquet'') UNION". Anchoring is required because "'') UNION" is a
	// superstring of "') UNION", so a bare `') UNION` check can never pass a
	// correct escape.
	if strings.Contains(got, "parquet') UNION") {
		t.Fatalf("single quote not escaped, SQL breakout possible: %s", got)
	}
	if !strings.Contains(got, "parquet'') UNION") {
		t.Fatalf("expected doubled quote in output, got: %s", got)
	}
}

// TestFormatDuckDBPathListLeavesCleanPathUnchanged pins that escaping is a
// no-op on a normal path (no accidental mangling of existing SQL).
func TestFormatDuckDBPathListLeavesCleanPathUnchanged(t *testing.T) {
	got := formatDuckDBPathList([]string{"s3://b/7/base/a.parquet"})
	if got != "'s3://b/7/base/a.parquet'" {
		t.Fatalf("clean path altered: %s", got)
	}
}
