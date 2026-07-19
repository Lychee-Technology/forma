package compaction

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSingleFileStats_ReadsParquetMetadata(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	path := filepath.Join(t.TempDir(), "delta.parquet")
	writeParquetFixture(t, db, path, []mergeFixtureRow{
		{rowID: rowA, changedAt: 100, deletedAt: "NULL", title: "a"},
		{rowID: rowC, changedAt: 300, deletedAt: "NULL", title: "c"},
		{rowID: rowB, changedAt: 200, deletedAt: "150", title: "NULL"},
	})

	stats, err := SingleFileStats(context.Background(), db, path)
	require.NoError(t, err)
	require.Equal(t, int64(3), stats.RowsOut)
	require.Equal(t, rowA, stats.RowIDMin)
	require.Equal(t, rowC, stats.RowIDMax)
	require.Equal(t, int64(100), stats.CreatedMin)
	require.Equal(t, int64(300), stats.CreatedMax)
}

func TestSingleFileStats_RejectsInvalidURI(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = SingleFileStats(context.Background(), db, "s3://bkt/it's.parquet")
	require.Error(t, err)
}

func TestIsConcurrentModification(t *testing.T) {
	require.True(t, IsConcurrentModification(errRewriteTestConflict))
	require.False(t, IsConcurrentModification(errors.New("network timeout")))
	require.False(t, IsConcurrentModification(nil))
}
