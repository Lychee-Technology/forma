package federated

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// #460 regression: the parquet legs used to alias the LWW version stamp into
// the created_at slot, so a created→updated→flushed row reported its LAST
// UPDATE time as its creation time, and the default sort key
// (created_at DESC, row_id ASC) changed value the instant a row was flushed.
//
// These tests run the federated pass over a real parquet file on an
// in-memory DuckDB (pgx_pass_fixture_test.go). Every row was created at
// 1000+n, and the fixture's parquet copies carry the version stamp 100, so
// a reader that reports the version stamp as created_at collapses the
// default order onto row_id, which no tie-break can hide.
//
//   - rows 1..3 are flushed: they live only in parquet.
//   - rows 4..6 are hot: the snapshot serves them, and their parquet copies
//     are stale versions the dirty-set anti-join discards, mirroring a row
//     that is being re-written after its first flush.
func flushBoundaryRows() []pgxRow {
	var rows []pgxRow
	for n := 1; n <= 6; n++ {
		rows = append(rows, pgxRow{i: n, createdAt: int64(1000 + n), age: ageOf(int64(n)), hot: n > 3, parquet: true})
	}
	return rows
}

// A created→updated→flushed row reports its creation time through the
// federated route, not its version stamp.
func TestFederatedCreatedAt_ReportsCreationTimeNotUpdateTime(t *testing.T) {
	f := newPgxFixture(t, flushBoundaryRows())
	e := f.engine(t, []string{f.dir + "/base.parquet"})
	records, total, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 10, 0, nil, nil)
	require.NoError(t, err)
	require.Equal(t, int64(6), total, "every seeded row must be visible")
	require.Len(t, records, 6)
	for _, r := range records {
		n := int(r.RowID[15])
		require.Equal(t, int64(1000+n), r.CreatedAt,
			"row %s must report its creation time, not its version stamp (#460)", r.RowID)
		if n <= 3 {
			require.NotEqual(t, r.UpdatedAt, r.CreatedAt,
				"row %s must not collapse created_at onto the LWW version stamp (#460)", r.RowID)
		}
	}
}

// The default ORDER BY created_at DESC, row_id ASC ranks flushed
// (parquet-served) and unflushed (hot-served) rows by the same quantity, so a
// page window does not duplicate or skip rows when a flush lands
// mid-pagination.
func TestFederatedCreatedAt_DefaultOrderIsStableAcrossFlushBoundary(t *testing.T) {
	f := newPgxFixture(t, flushBoundaryRows())
	e := f.engine(t, []string{f.dir + "/base.parquet"})
	want := []uuid.UUID{parquetOnlyRowID(6), parquetOnlyRowID(5), parquetOnlyRowID(4),
		parquetOnlyRowID(3), parquetOnlyRowID(2), parquetOnlyRowID(1)}

	records, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 10, 0, nil, nil)
	require.NoError(t, err)
	require.Equal(t, want, recordIDs(records),
		"the default order must rank hot and parquet rows by the same quantity (#460)")

	// The same order, walked one page at a time, must reproduce the full set
	// with no duplicate and no skip.
	var paged []uuid.UUID
	for offset := 0; offset < 6; offset += 2 {
		page, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 2, offset, nil, nil)
		require.NoError(t, err)
		paged = append(paged, recordIDs(page)...)
	}
	require.Equal(t, want, paged,
		"LIMIT/OFFSET windows over the default sort key must tile the result set exactly (#460)")
}
