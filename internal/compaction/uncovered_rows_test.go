package compaction

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUncoveredRowIDs_AntiJoinAgainstListedFiles(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	dir := t.TempDir()

	orphan := filepath.Join(dir, "orphan.parquet")
	writeParquetFixture(t, db, orphan, []mergeFixtureRow{
		{rowID: rowA, changedAt: 100, deletedAt: "NULL", title: "a"},
		{rowID: rowB, changedAt: 200, deletedAt: "NULL", title: "b"},
		{rowID: rowC, changedAt: 300, deletedAt: "NULL", title: "c"},
	})
	listed := filepath.Join(dir, "base.parquet")
	writeParquetFixture(t, db, listed, []mergeFixtureRow{
		{rowID: rowB, changedAt: 400, deletedAt: "0", title: "b2"},
	})

	uncovered, err := UncoveredRowIDs(context.Background(), db, orphan, []string{listed})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{rowA, rowC}, uncovered, "rowB is covered by the listed base")
}

func TestUncoveredRowIDs_NoListedFilesEverythingUncovered(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	dir := t.TempDir()

	orphan := filepath.Join(dir, "orphan.parquet")
	writeParquetFixture(t, db, orphan, []mergeFixtureRow{
		{rowID: rowA, changedAt: 100, deletedAt: "NULL", title: "a"},
	})

	uncovered, err := UncoveredRowIDs(context.Background(), db, orphan, nil)
	require.NoError(t, err)
	require.Equal(t, []string{rowA}, uncovered)
}

func TestUncoveredRowIDs_FullyCoveredReturnsEmpty(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	dir := t.TempDir()

	orphan := filepath.Join(dir, "orphan.parquet")
	writeParquetFixture(t, db, orphan, []mergeFixtureRow{
		{rowID: rowA, changedAt: 100, deletedAt: "NULL", title: "a"},
	})
	listed := filepath.Join(dir, "merged.parquet")
	writeParquetFixture(t, db, listed, []mergeFixtureRow{
		{rowID: rowA, changedAt: 100, deletedAt: "0", title: "a"},
	})

	uncovered, err := UncoveredRowIDs(context.Background(), db, orphan, []string{listed})
	require.NoError(t, err)
	require.Empty(t, uncovered)
}

func TestUncoveredRowIDs_RejectsInvalidURI(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = UncoveredRowIDs(context.Background(), db, "bad'uri.parquet", nil)
	require.Error(t, err)
}
