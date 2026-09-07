package compaction

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/sqlutil"
	"github.com/stretchr/testify/require"
)

// TestBuildMergeSQL_PinsLWWShape pins the exact fold the merge must share
// with the federated read path (#188): any drift in the ORDER BY, the
// tombstone filter, or the deleted_at normalization changes which version
// survives compaction versus merge-on-read.
func TestBuildMergeSQL_PinsLWWShape(t *testing.T) {
	sql, err := buildMergeSQL([]string{"s3://b/p/1/a.parquet", "s3://b/p/1/b.parquet"}, "s3://b/p/1/_tmp/t.parquet", "")
	require.NoError(t, err)

	require.Contains(t, sql, "PARTITION BY row_id")
	require.Contains(t, sql, "ORDER BY changed_at DESC, deleted_at DESC NULLS LAST, row_id ASC")
	require.Contains(t, sql, "WHERE _rn = 1 AND (deleted_at IS NULL OR deleted_at = 0)")
	require.Contains(t, sql, "SELECT * EXCLUDE (_rn) REPLACE (COALESCE(deleted_at, 0) AS deleted_at)")
	require.Contains(t, sql, "read_parquet(['s3://b/p/1/a.parquet', 's3://b/p/1/b.parquet'], union_by_name=true)")
	require.Contains(t, sql, "TO 's3://b/p/1/_tmp/t.parquet'")
	// COPY options default to the CDC exporters' parquet shape.
	require.Contains(t, sql, "FORMAT PARQUET, PARQUET_VERSION V2, COMPRESSION 'ZSTD', COMPRESSION_LEVEL 3")
}

func TestBuildMergeSQL_CustomCopyOptions(t *testing.T) {
	sql, err := buildMergeSQL([]string{"s3://b/a.parquet"}, "s3://b/t.parquet", "FORMAT PARQUET")
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(sql, "(FORMAT PARQUET)"), sql)
}

func TestBuildMergeSQL_Validation(t *testing.T) {
	_, err := buildMergeSQL(nil, "s3://b/t.parquet", "")
	require.ErrorContains(t, err, "at least one source")

	_, err = buildMergeSQL([]string{"s3://b/a.parquet"}, "", "")
	require.ErrorContains(t, err, "tmp target")
	require.ErrorContains(t, err, "empty parquet URI")

	_, err = buildMergeSQL([]string{""}, "s3://b/t.parquet", "")
	require.ErrorContains(t, err, "merge source")
	require.ErrorContains(t, err, "empty parquet URI")
}

// TestMergeSQL_QuoteBearingURIsRenderEscaped pins #546, the writer-side
// twin of internal/federated's TestValidateProbesQuoteBearingPath (#529):
// every compaction render site wraps the URI in sqlutil.EscapeLiteral
// (#478), so an object key that legitimately carries a quote, a double
// quote, or a semicolon is accepted and rendered with the quote doubled,
// never raw. Before #546 validateMergeURI refused such keys outright, so a
// manifest entry the read path served could be neither compacted nor
// reconciled.
func TestMergeSQL_QuoteBearingURIsRenderEscaped(t *testing.T) {
	const quoted = "s3://b/p/1/it's;\"odd\".parquet"
	const quotedTmp = "s3://b/p/1/_tmp/it's;\"odd\".parquet"
	escaped := sqlutil.EscapeLiteral(quoted)
	escapedTmp := sqlutil.EscapeLiteral(quotedTmp)
	require.NotEqual(t, quoted, escaped, "the fixture must actually need escaping")

	merge, err := buildMergeSQL([]string{"s3://b/p/1/plain.parquet", quoted}, quotedTmp, "")
	require.NoError(t, err, "a quote-bearing source and target render safely and must be accepted")
	require.Contains(t, merge, "read_parquet(['s3://b/p/1/plain.parquet', '"+escaped+"'], union_by_name=true)")
	require.NotContains(t, merge, "'"+quoted+"'", "the source quote must never render raw")
	require.Contains(t, merge, "TO '"+escapedTmp+"'")
	require.NotContains(t, merge, "'"+quotedTmp+"'", "the target quote must never render raw")

	rowsIn, err := buildMergeRowsInSQL([]string{quoted})
	require.NoError(t, err)
	require.Equal(t, "SELECT COUNT(*) FROM read_parquet(['"+escaped+"'], union_by_name=true)", rowsIn)

	stats, err := buildMergeStatsSQL(quotedTmp)
	require.NoError(t, err)
	require.Contains(t, stats, "FROM read_parquet('"+escapedTmp+"')")
	require.NotContains(t, stats, "'"+quotedTmp+"'")
}

func TestBuildMergeStatsSQL_CoalescesZeroRowMerge(t *testing.T) {
	sql, err := buildMergeStatsSQL("s3://b/t.parquet")
	require.NoError(t, err)
	// An all-tombstone schema merges to zero rows; the stats must come back
	// as usable zero values, not NULLs, so the entry is still written.
	require.Contains(t, sql, `COALESCE(MIN(CAST(row_id AS VARCHAR)), '')`)
	require.Contains(t, sql, "COALESCE(MIN(changed_at), 0)")

	_, err = buildMergeStatsSQL("")
	require.ErrorContains(t, err, "stats target")
	require.ErrorContains(t, err, "empty parquet URI")
}

func TestBuildMergeRowsInSQL(t *testing.T) {
	sql, err := buildMergeRowsInSQL([]string{"s3://b/a.parquet", "s3://b/b.parquet"})
	require.NoError(t, err)
	require.Equal(t, "SELECT COUNT(*) FROM read_parquet(['s3://b/a.parquet', 's3://b/b.parquet'], union_by_name=true)", sql)

	_, err = buildMergeRowsInSQL(nil)
	require.Error(t, err)
}
