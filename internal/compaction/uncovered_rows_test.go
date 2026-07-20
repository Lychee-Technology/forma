package compaction

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func uncoveredDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db, t.TempDir()
}

func TestUncoveredRows_CoverageIsVersionAware(t *testing.T) {
	db, dir := uncoveredDB(t)

	orphan := filepath.Join(dir, "orphan.parquet")
	writeParquetFixture(t, db, orphan, []mergeFixtureRow{
		{rowID: rowA, changedAt: 500, deletedAt: "NULL", title: "a-newer"}, // lost update: listed only has @100
		{rowID: rowB, changedAt: 200, deletedAt: "NULL", title: "b-old"},   // superseded: listed has @400
		{rowID: rowC, changedAt: 300, deletedAt: "NULL", title: "c"},       // row absent from listed entirely
	})
	listed := filepath.Join(dir, "base.parquet")
	writeParquetFixture(t, db, listed, []mergeFixtureRow{
		{rowID: rowA, changedAt: 100, deletedAt: "0", title: "a-old"},
		{rowID: rowB, changedAt: 400, deletedAt: "0", title: "b-new"},
	})

	uncovered, err := UncoveredRows(context.Background(), db, orphan, []string{listed})
	require.NoError(t, err)
	require.Equal(t, []UncoveredRow{
		{RowID: rowA, Tombstone: false},
		{RowID: rowC, Tombstone: false},
	}, uncovered, "a lost update (newer version than any listed) must count as uncovered; a superseded version must not")
}

func TestUncoveredRows_TombstoneFlagged(t *testing.T) {
	db, dir := uncoveredDB(t)

	orphan := filepath.Join(dir, "orphan.parquet")
	writeParquetFixture(t, db, orphan, []mergeFixtureRow{
		{rowID: rowA, changedAt: 300, deletedAt: "250", title: "NULL"}, // tombstone newer than listed live version
	})
	listed := filepath.Join(dir, "base.parquet")
	writeParquetFixture(t, db, listed, []mergeFixtureRow{
		{rowID: rowA, changedAt: 100, deletedAt: "0", title: "a-live"},
	})

	uncovered, err := UncoveredRows(context.Background(), db, orphan, []string{listed})
	require.NoError(t, err)
	require.Equal(t, []UncoveredRow{{RowID: rowA, Tombstone: true}}, uncovered,
		"an uncovered tombstone must be flagged — re-appending it RESTORES the delete rather than resurrecting data")
}

func TestUncoveredRows_EqualChangedAtIsCovered(t *testing.T) {
	db, dir := uncoveredDB(t)

	orphan := filepath.Join(dir, "orphan.parquet")
	writeParquetFixture(t, db, orphan, []mergeFixtureRow{
		{rowID: rowA, changedAt: 100, deletedAt: "NULL", title: "a"},
	})
	listed := filepath.Join(dir, "base.parquet")
	writeParquetFixture(t, db, listed, []mergeFixtureRow{
		{rowID: rowA, changedAt: 100, deletedAt: "0", title: "a"},
	})

	uncovered, err := UncoveredRows(context.Background(), db, orphan, []string{listed})
	require.NoError(t, err)
	require.Empty(t, uncovered, "equal changed_at ties resolve base-wins in LWW (#183), so the listed version covers the orphan's")
}

func TestUncoveredRows_NoListedFilesEverythingUncovered(t *testing.T) {
	db, dir := uncoveredDB(t)

	orphan := filepath.Join(dir, "orphan.parquet")
	writeParquetFixture(t, db, orphan, []mergeFixtureRow{
		{rowID: rowA, changedAt: 100, deletedAt: "NULL", title: "a"},
		{rowID: rowB, changedAt: 200, deletedAt: "150", title: "NULL"},
	})

	uncovered, err := UncoveredRows(context.Background(), db, orphan, nil)
	require.NoError(t, err)
	require.Equal(t, []UncoveredRow{
		{RowID: rowA, Tombstone: false},
		{RowID: rowB, Tombstone: true},
	}, uncovered)
}

func TestUncoveredRows_RejectsInvalidURI(t *testing.T) {
	db, _ := uncoveredDB(t)

	_, err := UncoveredRows(context.Background(), db, "bad'uri.parquet", nil)
	require.Error(t, err)
	_, err = UncoveredRows(context.Background(), db, "ok.parquet", []string{"bad'listed.parquet"})
	require.Error(t, err)
}
