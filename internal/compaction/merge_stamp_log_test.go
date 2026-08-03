package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// mergeStampFixture stages one healthy source file and returns (sourcePath,
// tmpPath) for a merge.
func mergeStampFixture(t *testing.T, db *sql.DB) (string, string) {
	t.Helper()
	dir := t.TempDir()
	src := filepath.Join(dir, "base.parquet")
	writeParquetFixture(t, db, src, []mergeFixtureRow{{rowA, 100, "0", "a-v1"}})
	return src, filepath.Join(dir, "merged.parquet")
}

// TestDuckMergerStampsAndStaysSilentOnSuccess: a merge that can describe its
// own output stamps the stats and says nothing. The warning below is only
// meaningful if the quiet path is genuinely quiet.
func TestDuckMergerStampsAndStaysSilentOnSuccess(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	src, tmp := mergeStampFixture(t, db)

	core, logs := observer.New(zap.WarnLevel)
	merger := &DuckMerger{DB: db, Logger: zap.New(core)}

	stats, err := merger.MergeToTmp(context.Background(), []string{src}, tmp)
	require.NoError(t, err)
	require.Contains(t, stats.Columns, "row_id", "a successful merge stamps its own footer (#256)")
	require.Zero(t, logs.Len(), "nothing to report when the describe succeeds")
}

// TestDuckMergerWarnsWhenStampProbeFails: the describe is best-effort, so its
// failure must not fail the merge — but it must not vanish either. An
// unstamped entry silently costs every future read a footer probe, and a
// DESCRIBE failing on bytes DuckDB just wrote is a signal about the store.
// Before this the error was assigned and dropped on the floor.
//
// Driven through the seam because the merge's own connection has by then
// succeeded at reading the sources, writing the output and collecting stats
// from it — there is no way to make only the last step fail from outside.
func TestDuckMergerWarnsWhenStampProbeFails(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	src, tmp := mergeStampFixture(t, db)

	core, logs := observer.New(zap.WarnLevel)
	merger := &DuckMerger{
		DB:     db,
		Logger: zap.New(core),
		describeColumns: func(context.Context, string) (map[string]string, error) {
			return nil, fmt.Errorf("object store timed out")
		},
	}

	stats, err := merger.MergeToTmp(context.Background(), []string{src}, tmp)
	require.NoError(t, err, "a failed stamp probe must never fail the merge")
	require.Nil(t, stats.Columns, "the entry stays unstamped")
	require.Equal(t, int64(1), stats.RowsOut, "everything the merge itself produced still stands")

	entries := logs.All()
	require.Len(t, entries, 1, "the discarded describe error must be reported")
	require.Contains(t, entries[0].Message, "unstamped")
	require.Equal(t, tmp, entries[0].ContextMap()["tmp_uri"])
	require.Contains(t, entries[0].ContextMap()["error"], "object store timed out",
		"the cause must survive into the log, not just the fact of failure")
}

// TestDuckMergerNilLoggerIsSafe: Logger is optional, and the merge path must
// not depend on a caller having set it.
func TestDuckMergerNilLoggerIsSafe(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	src, tmp := mergeStampFixture(t, db)

	merger := &DuckMerger{DB: db}
	stats, err := merger.MergeToTmp(context.Background(), []string{src}, tmp)
	require.NoError(t, err)
	require.Contains(t, stats.Columns, "row_id")

	var nilMerger *DuckMerger
	require.NotNil(t, nilMerger.log(), "the nil-safe accessor must never return nil")
}
