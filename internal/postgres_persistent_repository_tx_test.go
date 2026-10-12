package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The transaction runner is where #645's classification happens: these pin
// that a body error and a COMMIT error each go through their classifier, and
// that every write path runs through the runner.

func TestInWriteTxClassifiesBodyAndCommitErrors(t *testing.T) {
	tests := []struct {
		name      string
		bodyErr   error
		commitErr error
		check     func(t *testing.T, err error)
	}{
		{
			name:    "deadlock in the body is retryable",
			bodyErr: fmt.Errorf("upsert change log for r: %w", pgError("40P01", "deadlock detected")),
			check: func(t *testing.T, err error) {
				require.ErrorIs(t, err, forma.ErrRetryableConflict)
				assert.True(t, forma.IsRetryable(err))
			},
		},
		{
			name:      "40001 at COMMIT is retryable, never unknown",
			commitErr: pgError("40001", "could not serialize access"),
			check: func(t *testing.T, err error) {
				require.ErrorIs(t, err, forma.ErrRetryableConflict)
				assert.NotErrorIs(t, err, forma.ErrCommitOutcomeUnknown)
			},
		},
		{
			name:      "COMMIT sent and never answered is unknown",
			commitErr: io.ErrUnexpectedEOF,
			check: func(t *testing.T, err error) {
				require.ErrorIs(t, err, forma.ErrCommitOutcomeUnknown)
				assert.False(t, forma.IsRetryable(err))
				assert.NotErrorIs(t, err, forma.ErrConflict)
			},
		},
		{
			name:      "context cancelled while COMMIT is in flight is unknown",
			commitErr: context.Canceled,
			check: func(t *testing.T, err error) {
				require.ErrorIs(t, err, forma.ErrCommitOutcomeUnknown)
				assert.ErrorIs(t, err, context.Canceled, "the cause stays in the chain")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock, err := pgxmock.NewPool()
			require.NoError(t, err)
			defer mock.Close()

			mock.ExpectBeginTx(pgx.TxOptions{})
			if tt.bodyErr != nil {
				mock.ExpectRollback()
			} else {
				mock.ExpectCommit().WillReturnError(tt.commitErr)
			}

			repo := NewDBPersistentRecordRepository(mock, nil)
			err = repo.inWriteTx(context.Background(), pgx.TxOptions{}, func(pgx.Tx) error { return tt.bodyErr })
			require.Error(t, err)
			tt.check(t, err)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestInWriteTxReportsABeginFailure(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBeginTx(pgx.TxOptions{}).WillReturnError(errors.New("pool closed"))
	repo := NewDBPersistentRecordRepository(mock, nil)
	err = repo.inWriteTx(context.Background(), pgx.TxOptions{}, func(pgx.Tx) error {
		t.Fatal("the body must not run without a transaction")
		return nil
	})
	require.ErrorContains(t, err, "begin transaction: pool closed")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestEveryWritePathClassifiesItsLockStatement is invariant 1 of #645 path by
// path: the first statement of every write transaction is its advisory lock,
// and a lock_timeout there (55P03) must reach the caller as a retryable
// conflict. Before #645 none of these paths classified that statement, so the
// raw driver error answered an opaque 500.
func TestEveryWritePathClassifiesItsLockStatement(t *testing.T) {
	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table", ChangeLog: "change_log"}
	rowID := uuid.MustParse("44444444-4444-4444-4444-444444444444")
	key := model.PersistentRecordKey{SchemaID: 1, RowID: rowID}
	record := func() *model.PersistentRecord { return &model.PersistentRecord{SchemaID: 1, RowID: rowID} }
	mergeOne := func(context.Context, *model.PersistentRecord) (*model.PersistentRecord, error) { return record(), nil }
	mergeBatch := func(context.Context, int, *model.PersistentRecord) (*model.PersistentRecord, error) {
		return record(), nil
	}
	readCommitted := pgx.TxOptions{IsoLevel: pgx.ReadCommitted}

	paths := []struct {
		name string
		opts pgx.TxOptions
		run  func(r *DBPersistentRecordRepository) error
	}{
		{"insert", pgx.TxOptions{}, func(r *DBPersistentRecordRepository) error {
			return r.InsertPersistentRecord(context.Background(), tables, record())
		}},
		{"delete", pgx.TxOptions{}, func(r *DBPersistentRecordRepository) error {
			return r.DeletePersistentRecord(context.Background(), tables, 1, rowID)
		}},
		{"merge", readCommitted, func(r *DBPersistentRecordRepository) error {
			_, err := r.MergePersistentRecord(context.Background(), tables, 1, rowID, mergeOne)
			return err
		}},
		{"batch insert", pgx.TxOptions{}, func(r *DBPersistentRecordRepository) error {
			return r.BatchInsertPersistentRecords(context.Background(), tables, []*model.PersistentRecord{record()})
		}},
		{"batch delete", pgx.TxOptions{}, func(r *DBPersistentRecordRepository) error {
			return r.BatchDeletePersistentRecords(context.Background(), tables, []model.PersistentRecordKey{key})
		}},
		{"batch merge", readCommitted, func(r *DBPersistentRecordRepository) error {
			_, err := r.BatchMergePersistentRecords(context.Background(), tables, []model.PersistentRecordKey{key}, mergeBatch)
			return err
		}},
		{"requeue", readCommitted, func(r *DBPersistentRecordRepository) error {
			return r.RequeueForFlush(context.Background(), tables, 1, rowID)
		}},
	}
	for _, path := range paths {
		t.Run(path.name, func(t *testing.T) {
			mock, err := pgxmock.NewPool()
			require.NoError(t, err)
			defer mock.Close()
			mock.MatchExpectationsInOrder(true)

			mock.ExpectBeginTx(path.opts)
			mock.ExpectExec(`^SELECT pg_advisory_xact_lock`).WithArgs(rowVersionLockKey(1, rowID)).
				WillReturnError(pgError("55P03", "canceling statement due to lock timeout"))
			mock.ExpectRollback()

			err = path.run(NewDBPersistentRecordRepository(mock, schemameta.NewMetadataCache()))
			require.ErrorIs(t, err, forma.ErrRetryableConflict)
			assert.True(t, forma.IsRetryable(err))
			assert.NotErrorIs(t, err, forma.ErrConflict)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
