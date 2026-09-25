package internal

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/require"
)

var requeueTables = model.StorageTables{EntityMain: "entity_main", EAVData: "eav_data", ChangeLog: "change_log"}

func newRequeueRepo(t *testing.T) (*DBPersistentRecordRepository, pgxmock.PgxPoolIface, time.Time) {
	t.Helper()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	mock.MatchExpectationsInOrder(true)

	repo := NewDBPersistentRecordRepository(mock, schemameta.NewMetadataCache())
	fixed := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	repo.withClock(func() time.Time { return fixed })
	return repo, mock, fixed
}

// TestRequeueForFlushAdvancesVersionAndStampsChangeLog pins #501's repair: it
// takes the per-row version lock, advances ltbase_updated_at by the #274
// GREATEST rule without touching a value column, and stamps slot 0 of
// change_log with exactly the version Postgres returned (#210), carrying the
// row's soft-delete state through unchanged.
func TestRequeueForFlushAdvancesVersionAndStampsChangeLog(t *testing.T) {
	repo, mock, fixed := newRequeueRepo(t)
	rowID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	effective := fixed.UnixMilli() + 7 // a clock-ahead prior version wins GREATEST
	softDeleted := int64(1234)

	mock.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	mock.ExpectExec(`^SELECT pg_advisory_xact_lock`).
		WithArgs(rowVersionLockKey(9, rowID)).
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
	mock.ExpectQuery(regexp.QuoteMeta(buildRequeueMainStatement("entity_main"))).
		WithArgs(fixed.UnixMilli(), int16(9), rowID).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at", "ltbase_deleted_at"}).AddRow(effective, &softDeleted))
	mock.ExpectExec(`INSERT INTO "change_log" \(schema_id, row_id, flushed_at, changed_at, deleted_at\)`).
		WithArgs(int16(9), rowID, int64(0), effective, softDeleted).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	require.NoError(t, repo.RequeueForFlush(context.Background(), requeueTables, 9, rowID))
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRequeueForFlushMissingRowIsNotFound: a row with no entity_main row has
// nothing to re-export; the sweep must be able to tell that apart from a
// storage failure, so it answers the NotFound carrier and writes nothing.
func TestRequeueForFlushMissingRowIsNotFound(t *testing.T) {
	repo, mock, fixed := newRequeueRepo(t)
	rowID := uuid.MustParse("44444444-4444-4444-4444-444444444444")

	mock.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	mock.ExpectExec(`^SELECT pg_advisory_xact_lock`).
		WithArgs(pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
	mock.ExpectQuery(regexp.QuoteMeta(buildRequeueMainStatement("entity_main"))).
		WithArgs(fixed.UnixMilli(), int16(9), rowID).
		WillReturnError(pgx.ErrNoRows)
	mock.ExpectRollback()

	err := repo.RequeueForFlush(context.Background(), requeueTables, 9, rowID)
	require.True(t, errors.Is(err, forma.ErrNotFound), "want NotFound, got %v", err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRequeueForFlushRequiresChangeLog: without a change_log table there is
// no dirty set to join, so a requeue would advance the version and re-export
// nothing. It is refused before any write.
func TestRequeueForFlushRequiresChangeLog(t *testing.T) {
	repo, mock, _ := newRequeueRepo(t)
	tables := requeueTables
	tables.ChangeLog = ""

	err := repo.RequeueForFlush(context.Background(), tables, 9, uuid.New())
	require.ErrorContains(t, err, "change log table name is required")
	require.NoError(t, mock.ExpectationsWereMet())
}
