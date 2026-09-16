package internal

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/require"
)

// #554 pins for the batch writers' lock discipline: every advisory lock is
// taken in ascending (schemaID, rowID) order BEFORE any row is written, and
// the writes then follow input order so positional results stay positional.

var (
	lockOrderLowRow  = uuid.MustParse("11111111-1111-1111-1111-111111111111")
	lockOrderHighRow = uuid.MustParse("99999999-9999-9999-9999-999999999999")
)

func expectSortedLockPass(mock pgxmock.PgxPoolIface, schemaID int16, rows ...uuid.UUID) {
	for _, row := range rows {
		mock.ExpectExec(`^SELECT pg_advisory_xact_lock`).
			WithArgs(rowVersionLockKey(schemaID, row)).
			WillReturnResult(pgxmock.NewResult("SELECT", 1))
	}
}

func TestBatchInsertLocksInSortedOrderThenWritesInInputOrder(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	repo := NewDBPersistentRecordRepository(mock, nil)
	fixed := time.Date(2024, 6, 7, 8, 9, 10, 0, time.UTC)
	repo.withClock(func() time.Time { return fixed })
	fixedMillis := fixed.UnixMilli()
	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table", ChangeLog: "change_log"}

	// Input order is descending; the lock pass must still be ascending.
	records := []*model.PersistentRecord{
		{SchemaID: 1, RowID: lockOrderHighRow, TextItems: map[string]string{"text_01": "high"}},
		{SchemaID: 1, RowID: lockOrderLowRow, TextItems: map[string]string{"text_01": "low"}},
	}
	insertQueries := make([]string, len(records))
	insertArgs := make([][]any, len(records))
	for i, record := range records {
		expected := *record
		expected.CreatedAt = fixedMillis
		expected.UpdatedAt = fixedMillis
		insertQueries[i], insertArgs[i], err = buildInsertMainStatement(tables.EntityMain, &expected)
		require.NoError(t, err)
	}

	mock.ExpectBegin()
	expectSortedLockPass(mock, 1, lockOrderLowRow, lockOrderHighRow)
	for i, record := range records {
		mock.ExpectQuery(`^SELECT COALESCE\(MAX\(changed_at\), 0\) FROM "change_log"`).
			WithArgs(int16(1), record.RowID).
			WillReturnRows(pgxmock.NewRows([]string{"coalesce"}).AddRow(int64(0)))
		mock.ExpectExec("^" + regexp.QuoteMeta(insertQueries[i]) + "$").
			WithArgs(insertArgs[i]...).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))
		mock.ExpectExec(`^INSERT INTO "change_log"`).
			WithArgs(int16(1), record.RowID, int64(0), fixedMillis, nil).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))
	}
	mock.ExpectCommit()
	mock.ExpectRollback()

	require.NoError(t, repo.BatchInsertPersistentRecords(ctx, tables, records))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBatchDeleteLocksInSortedOrderThenWritesInInputOrder(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	repo := NewDBPersistentRecordRepository(mock, nil)
	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table"}

	keys := []model.PersistentRecordKey{
		{SchemaID: 1, RowID: lockOrderHighRow},
		{SchemaID: 1, RowID: lockOrderLowRow},
	}

	mock.ExpectBegin()
	expectSortedLockPass(mock, 1, lockOrderLowRow, lockOrderHighRow)
	for _, key := range keys {
		mock.ExpectQuery(`^DELETE FROM "entity_main"`).
			WithArgs(int16(1), key.RowID).
			WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at"}).AddRow(int64(100)))
		mock.ExpectExec(`^DELETE FROM "eav_table"`).
			WithArgs(int16(1), key.RowID).
			WillReturnResult(pgxmock.NewResult("DELETE", 1))
	}
	mock.ExpectCommit()
	mock.ExpectRollback()

	require.NoError(t, repo.BatchDeletePersistentRecords(ctx, tables, keys))
	require.NoError(t, mock.ExpectationsWereMet())
}
