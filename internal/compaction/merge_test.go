package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/lychee-technology/forma/internal/parquetcheck"
	"github.com/stretchr/testify/require"
)

// mergeFixtureRow models one version row in a source parquet fixture.
type mergeFixtureRow struct {
	rowID     string
	changedAt int64
	deletedAt string // SQL literal: "0", "NULL", or a positive ms value
	title     string // SQL literal without quotes; "NULL" for tombstones
}

func writeParquetFixture(t *testing.T, db *sql.DB, path string, rows []mergeFixtureRow) {
	t.Helper()
	selects := make([]string, 0, len(rows))
	for _, r := range rows {
		title := "NULL"
		if r.title != "NULL" {
			title = "'" + r.title + "'"
		}
		selects = append(selects, fmt.Sprintf(
			"SELECT CAST('%s' AS UUID) AS row_id, CAST(%d AS BIGINT) AS changed_at, CAST(%s AS BIGINT) AS deleted_at, %s AS title",
			r.rowID, r.changedAt, r.deletedAt, title))
	}
	q := fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET)", joinSQL(selects), path)
	_, err := db.Exec(q)
	require.NoError(t, err)
}

func joinSQL(selects []string) string {
	out := selects[0]
	for _, s := range selects[1:] {
		out += " UNION ALL " + s
	}
	return out
}

const (
	rowA = "018f05c0-0000-7000-8000-00000000000a"
	rowB = "018f05c0-0000-7000-8000-00000000000b"
	rowC = "018f05c0-0000-7000-8000-00000000000c"
	rowD = "018f05c0-0000-7000-8000-00000000000d"
)

// TestDuckMerger_MergeToTmp runs the real merge SQL on in-memory DuckDB over
// local parquet fixtures shaped like production exports: base rows carry
// deleted_at=0 (init exporter's COALESCE), live delta rows carry NULL, and
// tombstones carry the delete timestamp. Pins the four #188 fold behaviors:
// newest-version wins, tombstones drop, equal-ver_ts ties stay base-wins
// (#183), and survivors are normalized to deleted_at=0 with changed_at
// carried verbatim (#210).
func TestDuckMerger_MergeToTmp(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.parquet")
	deltaPath := filepath.Join(dir, "delta.parquet")
	tmpPath := filepath.Join(dir, "merged.parquet")

	writeParquetFixture(t, db, basePath, []mergeFixtureRow{
		{rowA, 100, "0", "a-v1"},
		{rowB, 100, "0", "b-v1"},
		{rowC, 100, "0", "c-v1"},
		{rowD, 150, "0", "d-base"},
	})
	writeParquetFixture(t, db, deltaPath, []mergeFixtureRow{
		{rowA, 200, "NULL", "a-v2"},    // newer live version wins
		{rowB, 300, "300", "NULL"},     // tombstone: row B must vanish
		{rowD, 150, "NULL", "d-delta"}, // equal-ver_ts tie: base copy must win
	})

	merger := &DuckMerger{DB: db}
	stats, err := merger.MergeToTmp(context.Background(), []string{basePath, deltaPath}, tmpPath)
	require.NoError(t, err)

	require.Equal(t, int64(7), stats.RowsIn)
	require.Equal(t, int64(3), stats.RowsOut)
	require.Equal(t, rowA, stats.RowIDMin)
	require.Equal(t, rowD, stats.RowIDMax)
	require.Equal(t, int64(100), stats.CreatedMin)
	require.Equal(t, int64(200), stats.CreatedMax)

	type mergedRow struct {
		changedAt, deletedAt int64
		title                string
	}
	got := map[string]mergedRow{}
	rows, err := db.Query(fmt.Sprintf(
		"SELECT CAST(row_id AS VARCHAR), changed_at, deleted_at, title FROM read_parquet('%s')", tmpPath))
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var id, title string
		var r mergedRow
		require.NoError(t, rows.Scan(&id, &r.changedAt, &r.deletedAt, &title))
		r.title = title
		got[id] = r
	}
	require.NoError(t, rows.Err())

	require.Equal(t, map[string]mergedRow{
		rowA: {changedAt: 200, deletedAt: 0, title: "a-v2"},   // winner verbatim, NULL normalized to 0
		rowC: {changedAt: 100, deletedAt: 0, title: "c-v1"},   // untouched base row
		rowD: {changedAt: 150, deletedAt: 0, title: "d-base"}, // tie: base-wins preserved
	}, got)

	// Verify stats carry the merged file's stamp satisfying the system-column
	// invariant.
	if err := parquetcheck.Check("merged", stats.Columns); err != nil {
		t.Fatalf("merged stats must carry a stamp satisfying the invariant: %v", err)
	}
}

// TestDuckMerger_AllTombstones pins the schema-empties-out edge: the merge
// writes a zero-row parquet and the stats come back as zeros, so the caller
// still records a (RowCount=0) manifest entry.
func TestDuckMerger_AllTombstones(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.parquet")
	deltaPath := filepath.Join(dir, "delta.parquet")
	tmpPath := filepath.Join(dir, "merged.parquet")

	writeParquetFixture(t, db, basePath, []mergeFixtureRow{{rowA, 100, "0", "a-v1"}})
	writeParquetFixture(t, db, deltaPath, []mergeFixtureRow{{rowA, 200, "200", "NULL"}})

	merger := &DuckMerger{DB: db}
	stats, err := merger.MergeToTmp(context.Background(), []string{basePath, deltaPath}, tmpPath)
	require.NoError(t, err)
	// Verify basic stats; Columns will be populated by DescribeColumns
	require.Equal(t, int64(2), stats.RowsIn)
	require.Equal(t, int64(0), stats.RowsOut)
	require.Equal(t, "", stats.RowIDMin)
	require.Equal(t, "", stats.RowIDMax)
	require.Equal(t, int64(0), stats.CreatedMin)
	require.Equal(t, int64(0), stats.CreatedMax)
	// Columns should be populated even for zero-row merges
	require.NotNil(t, stats.Columns)

	var n int
	require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", tmpPath)).Scan(&n))
	require.Zero(t, n)
}

func TestDuckMerger_RequiresDB(t *testing.T) {
	var m *DuckMerger
	_, err := m.MergeToTmp(context.Background(), []string{"a"}, "b")
	require.ErrorContains(t, err, "no database")
}

// TestDuckMerger_RejectsWrongSchemaSource pins the #189-review P1: the merge
// scan runs union_by_name, which NULL-pads a malformed source's system
// columns — every such row folds into the single NULL row_id partition and
// all but one are silently discarded, and runRewrite deletes the merged
// sources afterwards, making the loss permanent. The pre-merge invariant
// check must abort before any write instead.
func TestDuckMerger_RejectsWrongSchemaSource(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	dir := t.TempDir()
	goodPath := filepath.Join(dir, "good.parquet")
	badPath := filepath.Join(dir, "bad.parquet")
	tmpPath := filepath.Join(dir, "merged.parquet")

	writeParquetFixture(t, db, goodPath, []mergeFixtureRow{{rowA, 100, "0", "a-v1"}})
	_, err = db.Exec(fmt.Sprintf("COPY (SELECT 1 AS wrong_col, 'x' AS other_col) TO '%s' (FORMAT PARQUET)", badPath))
	require.NoError(t, err)

	merger := &DuckMerger{DB: db}
	_, err = merger.MergeToTmp(context.Background(), []string{goodPath, badPath}, tmpPath)
	require.Error(t, err)
	require.ErrorContains(t, err, "row_id")
	require.ErrorContains(t, err, badPath)

	_, statErr := os.Stat(tmpPath)
	require.True(t, os.IsNotExist(statErr), "rejected merge must not stage a tmp object")

	// Positive control: the good source alone merges cleanly.
	_, err = merger.MergeToTmp(context.Background(), []string{goodPath}, tmpPath)
	require.NoError(t, err)
}
