package internal

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRunOptimizedQueryEmptyPageRecountsTotal pins the #181 fallback: a page
// at or beyond the last match returns zero rows (the window count travels on
// data rows), so the repository must recount at limit 1, offset 0 and report
// the true total instead of 0.
func TestRunOptimizedQueryEmptyPageRecountsTotal(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	repo := NewDBPersistentRecordRepository(mock, nil)
	tables := model.StorageTables{EntityMain: "main_table", EAVData: "eav_table"}

	columns, values := optimizedQueryFixtureColumnsAndValues(
		uuid.MustParse("11111111-1111-1111-1111-111111111111"), int64(25))

	// Deep page: offset past the 25 matches returns no rows.
	mock.ExpectQuery("WITH anchor").
		WithArgs(int16(1), 10, 30).
		WillReturnRows(pgxmock.NewRows(columns))
	// Fallback recount: same shape at limit 1, offset 0 carries the count back.
	mock.ExpectQuery("WITH anchor").
		WithArgs(int16(1), 1, 0).
		WillReturnRows(pgxmock.NewRows(columns).AddRow(values...))

	records, total, err := repo.runOptimizedQuery(ctx, tables, 1, "1=1", nil, 10, 30, nil, true)
	require.NoError(t, err)
	assert.Empty(t, records)
	assert.Equal(t, int64(25), total)

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRunOptimizedQueryEmptyResultAtOffsetZeroSkipsRecount guards the other
// half of the #181 condition: with offset 0 an empty result is a genuine
// total of 0, and the fallback must not burn a second query.
func TestRunOptimizedQueryEmptyResultAtOffsetZeroSkipsRecount(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	repo := NewDBPersistentRecordRepository(mock, nil)
	tables := model.StorageTables{EntityMain: "main_table", EAVData: "eav_table"}

	columns, _ := optimizedQueryFixtureColumnsAndValues(uuid.New(), 0)
	mock.ExpectQuery("WITH anchor").
		WithArgs(int16(1), 10, 0).
		WillReturnRows(pgxmock.NewRows(columns))

	records, total, err := repo.runOptimizedQuery(ctx, tables, 1, "1=1", nil, 10, 0, nil, true)
	require.NoError(t, err)
	assert.Empty(t, records)
	assert.Equal(t, int64(0), total)

	require.NoError(t, mock.ExpectationsWereMet())
}
