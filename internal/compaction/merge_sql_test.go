package compaction

import (
	"strings"
	"testing"

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

	_, err = buildMergeSQL([]string{"s3://b/a'.parquet"}, "s3://b/t.parquet", "")
	require.ErrorContains(t, err, "quote or semicolon")

	_, err = buildMergeSQL([]string{"s3://b/a.parquet"}, "s3://b/t';drop.parquet", "")
	require.ErrorContains(t, err, "tmp target")

	_, err = buildMergeSQL([]string{""}, "s3://b/t.parquet", "")
	require.ErrorContains(t, err, "empty parquet URI")
}

func TestBuildMergeStatsSQL_CoalescesZeroRowMerge(t *testing.T) {
	sql, err := buildMergeStatsSQL("s3://b/t.parquet")
	require.NoError(t, err)
	// An all-tombstone schema merges to zero rows; the stats must come back
	// as usable zero values, not NULLs, so the entry is still written.
	require.Contains(t, sql, `COALESCE(MIN(CAST(row_id AS VARCHAR)), '')`)
	require.Contains(t, sql, "COALESCE(MIN(changed_at), 0)")

	_, err = buildMergeStatsSQL("bad'uri")
	require.Error(t, err)
}

func TestBuildMergeRowsInSQL(t *testing.T) {
	sql, err := buildMergeRowsInSQL([]string{"s3://b/a.parquet", "s3://b/b.parquet"})
	require.NoError(t, err)
	require.Equal(t, "SELECT COUNT(*) FROM read_parquet(['s3://b/a.parquet', 's3://b/b.parquet'], union_by_name=true)", sql)

	_, err = buildMergeRowsInSQL(nil)
	require.Error(t, err)
}
