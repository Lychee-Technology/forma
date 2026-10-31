package internal

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/uniquekey"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #699: a reservation whose value is released and taken again in every round
// runs out of rounds. That is contention, not a conflict with stored state:
// the reservation is undone and the write answers ErrRetryableConflict, as a
// 40001 or 40P01 does. The rounds are forced on a mock, because on a real
// server each one needs another transaction to release the value between this
// write's insert and its lookup.

var (
	reserveInsert = regexp.QuoteMeta(fmt.Sprintf("INSERT INTO %s (ltbase_schema_id, key_id, key_value, ltbase_row_id)",
		sanitizeIdentifier(uniquekey.KeyTable("entity_main"))))
	reserveLookup = regexp.QuoteMeta("SELECT u.n, k.ltbase_row_id FROM unnest(")
	reserveUndo   = regexp.QuoteMeta(fmt.Sprintf("DELETE FROM %s AS k USING unnest(",
		sanitizeIdentifier(uniquekey.KeyTable("entity_main"))))
)

// expectReleasedRound expects one round in which the insert skips entry and
// the lookup finds nobody holding it: its holder released it in between.
func expectReleasedRound(mock pgxmock.PgxPoolIface, rowID uuid.UUID, entry uniqueKeyEntry) {
	mock.ExpectQuery(reserveInsert).
		WithArgs([]int16{1}, []int16{entry.keyID}, []string{entry.value}, []uuid.UUID{rowID}).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_schema_id", "key_id", "key_value"}))
	mock.ExpectQuery(reserveLookup).
		WithArgs([]int16{1}, []int16{entry.keyID}, []string{entry.value}).
		WillReturnRows(pgxmock.NewRows([]string{"n", "ltbase_row_id"}))
}

func reserveTestRepo(t *testing.T) (*DBPersistentRecordRepository, pgxmock.PgxPoolIface) {
	t.Helper()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	mock.MatchExpectationsInOrder(true)
	return NewDBPersistentRecordRepository(mock, schemameta.NewMetadataCache()), mock
}

func requireRoundsExhausted(t *testing.T, err error, keyName string) {
	t.Helper()
	require.ErrorIs(t, err, forma.ErrRetryableConflict)
	assert.True(t, forma.IsRetryable(err), "nothing was written, so the write can run again")
	assert.NotErrorIs(t, err, forma.ErrConflict, "contention is not a conflict with stored state")
	assert.Equal(t, batchCodeRetryableConflict, batchErrorCode(err, "STORAGE"))
	_, published := forma.ResolvePublicMessage(err)
	assert.False(t, published, "a retryable conflict publishes nothing")
	assert.Contains(t, err.Error(), fmt.Sprintf("unique key %s: its value was released and taken again %d times", keyName, maxReserveRounds),
		"the operator copy names the key and the rounds")
}

func TestReserveRoundsExhaustedIsARetryableConflict(t *testing.T) {
	ctx := context.Background()
	repo, mock := reserveTestRepo(t)
	tables := model.StorageTables{EntityMain: "entity_main"}
	rowID := uuid.New()
	email := uniqueKeyEntry{keyID: 5, keyName: "tenant_email", value: "a@example.com"}

	mock.ExpectBegin()
	for range maxReserveRounds {
		expectReleasedRound(mock, rowID, email)
	}
	tx, err := mock.Begin(ctx)
	require.NoError(t, err)

	reservation, err := repo.reserveUniqueKeys(ctx, tx, tables, &uniqueKeyClaim{schemaID: 1, rowID: rowID, take: []uniqueKeyEntry{email}})
	assert.Nil(t, reservation)
	requireRoundsExhausted(t, err, "tenant_email")
	// The callers wrap it on the way to the transaction boundary, whose
	// classifier must keep it as it is.
	wrapped := classifyStatementError(fmt.Errorf("reserve unique keys of %s: %w", rowID, err))
	requireRoundsExhausted(t, wrapped, "tenant_email")
	require.NoError(t, mock.ExpectationsWereMet(), "exactly %d rounds, and no undo of an empty reservation", maxReserveRounds)
}

// The entries the earlier rounds inserted are removed before the answer, so
// the write leaves nothing behind in its transaction either.
func TestReserveRoundsExhaustedUndoesTheEntriesItInserted(t *testing.T) {
	ctx := context.Background()
	repo, mock := reserveTestRepo(t)
	tables := model.StorageTables{EntityMain: "entity_main"}
	rowID := uuid.New()
	code := uniqueKeyEntry{keyID: 3, keyName: "tenant_code", value: "c-1"}
	email := uniqueKeyEntry{keyID: 5, keyName: "tenant_email", value: "a@example.com"}

	mock.ExpectBegin()
	// Round 1 inserts code and skips email, whose holder is gone at the lookup.
	mock.ExpectQuery(reserveInsert).
		WithArgs([]int16{1, 1}, []int16{3, 5}, []string{"c-1", "a@example.com"}, []uuid.UUID{rowID, rowID}).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_schema_id", "key_id", "key_value"}).AddRow(int16(1), int16(3), "c-1"))
	mock.ExpectQuery(reserveLookup).
		WithArgs([]int16{1}, []int16{5}, []string{"a@example.com"}).
		WillReturnRows(pgxmock.NewRows([]string{"n", "ltbase_row_id"}))
	for range maxReserveRounds - 1 {
		expectReleasedRound(mock, rowID, email)
	}
	mock.ExpectExec(reserveUndo).
		WithArgs([]int16{1}, []int16{3}, []string{"c-1"}, []uuid.UUID{rowID}).
		WillReturnResult(pgxmock.NewResult("DELETE", 1))
	tx, err := mock.Begin(ctx)
	require.NoError(t, err)

	_, err = repo.reserveUniqueKeys(ctx, tx, tables, &uniqueKeyClaim{schemaID: 1, rowID: rowID, take: []uniqueKeyEntry{email, code}})
	requireRoundsExhausted(t, err, "tenant_email")
	require.NoError(t, mock.ExpectationsWereMet())
}

// An undo that fails aborts the transaction with a failed statement, so the
// answer is that failure, never a retryable conflict claiming the Tx usable.
func TestReserveRoundsExhaustedWithAFailedUndoIsNotRetryable(t *testing.T) {
	ctx := context.Background()
	repo, mock := reserveTestRepo(t)
	tables := model.StorageTables{EntityMain: "entity_main"}
	rowID := uuid.New()
	code := uniqueKeyEntry{keyID: 3, keyName: "tenant_code", value: "c-1"}
	email := uniqueKeyEntry{keyID: 5, keyName: "tenant_email", value: "a@example.com"}

	mock.ExpectBegin()
	mock.ExpectQuery(reserveInsert).
		WithArgs([]int16{1, 1}, []int16{3, 5}, []string{"c-1", "a@example.com"}, []uuid.UUID{rowID, rowID}).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_schema_id", "key_id", "key_value"}).AddRow(int16(1), int16(3), "c-1"))
	mock.ExpectQuery(reserveLookup).
		WithArgs([]int16{1}, []int16{5}, []string{"a@example.com"}).
		WillReturnRows(pgxmock.NewRows([]string{"n", "ltbase_row_id"}))
	for range maxReserveRounds - 1 {
		expectReleasedRound(mock, rowID, email)
	}
	undoFailed := errors.New("connection reset")
	mock.ExpectExec(reserveUndo).WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnError(undoFailed)
	tx, err := mock.Begin(ctx)
	require.NoError(t, err)

	_, err = repo.reserveUniqueKeys(ctx, tx, tables, &uniqueKeyClaim{schemaID: 1, rowID: rowID, take: []uniqueKeyEntry{email, code}})
	require.ErrorIs(t, err, undoFailed)
	assert.NotErrorIs(t, err, forma.ErrRetryableConflict)
	assert.False(t, forma.IsRetryable(err))
	assert.Contains(t, err.Error(), "released and taken again", "the cause stays as text")
	require.NoError(t, mock.ExpectationsWereMet())
}
