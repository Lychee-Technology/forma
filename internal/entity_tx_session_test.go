package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lychee-technology/forma"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The poisoning rule and the limits of a Tx (#648), driven through Tx.Delete
// on a pgxmock pool: its statements are few (lock, main-row DELETE …
// RETURNING, EAV DELETE, change_log upsert), and it reaches every error class.

var txTestRow = uuid.MustParse("66666666-6666-6666-6666-666666666666")

func deleteOp(schema string) *forma.EntityOperation {
	return &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: schema, RowID: txTestRow}}
}

func expectLock(mock pgxmock.PgxPoolIface) *pgxmock.ExpectedExec {
	return mock.ExpectExec(`^SELECT pg_advisory_xact_lock`).WithArgs(pgxmock.AnyArg())
}

// expectDeleteFound queues one Delete that finds its row, which holds eavRows
// attribute rows.
func expectDeleteFound(mock pgxmock.PgxPoolIface, eavRows int64) {
	expectLock(mock).WillReturnResult(pgxmock.NewResult("SELECT", 1))
	mock.ExpectQuery(`^DELETE FROM .*entity_main`).WithArgs(pgxmock.AnyArg(), txTestRow).WillReturnRows(
		pgxmock.NewRows([]string{"ltbase_updated_at"}).AddRow(int64(1000)).
			AddCommandTag(pgconn.NewCommandTag("DELETE 1")))
	mock.ExpectExec(`^DELETE FROM .*eav_data`).WithArgs(pgxmock.AnyArg(), txTestRow).WillReturnResult(pgxmock.NewResult("DELETE", eavRows))
	mock.ExpectExec(`^INSERT INTO .*change_log`).WithArgs(pgxmock.AnyArg(), txTestRow, int64(0), pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnResult(pgxmock.NewResult("INSERT 0", 1))
}

// poisoningCase is one row of the poisoning truth table: the operation run,
// the error class it answers, and whether it poisons the Tx.
type poisoningCase struct {
	name     string
	opts     []forma.TxOption
	expect   func(mock pgxmock.PgxPoolIface)
	run      func(ctx context.Context, tx forma.Tx) error
	class    error
	poisoned bool
}

func poisoningCases() []poisoningCase {
	return []poisoningCase{
		{
			name: "ErrNotFound after SQL is expected",
			expect: func(mock pgxmock.PgxPoolIface) {
				expectLock(mock).WillReturnResult(pgxmock.NewResult("SELECT", 1))
				mock.ExpectQuery(`^DELETE FROM .*entity_main`).WithArgs(pgxmock.AnyArg(), txTestRow).WillReturnRows(
					pgxmock.NewRows([]string{"ltbase_updated_at"}).AddCommandTag(pgconn.NewCommandTag("DELETE 0")))
			},
			run:   func(ctx context.Context, tx forma.Tx) error { return tx.Delete(ctx, deleteOp("visit")) },
			class: forma.ErrNotFound,
		},
		{
			name:  "ErrNotFound for an unknown schema is expected",
			run:   func(ctx context.Context, tx forma.Tx) error { return tx.Delete(ctx, deleteOp("no_such_schema")) },
			class: forma.ErrNotFound,
		},
		{
			name:  "ErrInvalidInput before any SQL is expected",
			run:   func(ctx context.Context, tx forma.Tx) error { return tx.Delete(ctx, deleteOp("")) },
			class: forma.ErrInvalidInput,
		},
		{
			name: "a storage error poisons",
			expect: func(mock pgxmock.PgxPoolIface) {
				expectLock(mock).WillReturnError(errors.New("connection reset by peer"))
			},
			run:      func(ctx context.Context, tx forma.Tx) error { return tx.Delete(ctx, deleteOp("visit")) },
			poisoned: true,
		},
		{
			name: "a retryable conflict poisons",
			expect: func(mock pgxmock.PgxPoolIface) {
				expectLock(mock).WillReturnError(pgError("40P01", "deadlock detected"))
			},
			run:      func(ctx context.Context, tx forma.Tx) error { return tx.Delete(ctx, deleteOp("visit")) },
			class:    forma.ErrRetryableConflict,
			poisoned: true,
		},
		{
			name: "a unique conflict raised by a failed statement poisons",
			expect: func(mock pgxmock.PgxPoolIface) {
				expectLock(mock).WillReturnResult(pgxmock.NewResult("SELECT", 1))
				mock.ExpectQuery(`^DELETE FROM .*entity_main`).WithArgs(pgxmock.AnyArg(), txTestRow).WillReturnError(pgError("23505", "duplicate key"))
			},
			run:      func(ctx context.Context, tx forma.Tx) error { return tx.Delete(ctx, deleteOp("visit")) },
			class:    forma.ErrUniqueConflict,
			poisoned: true,
		},
		{
			name: "the operation limit poisons and runs nothing",
			opts: []forma.TxOption{forma.WithTxMaxOperations(1)},
			run: func(ctx context.Context, tx forma.Tx) error {
				if err := tx.Delete(ctx, deleteOp("")); !errors.Is(err, forma.ErrInvalidInput) {
					return fmt.Errorf("operation 1 should be refused before any SQL, got %v", err)
				}
				return tx.Delete(ctx, deleteOp("visit"))
			},
			class:    forma.ErrInvalidInput,
			poisoned: true,
		},
		{
			name: "the physical-row limit poisons at the crossing statement",
			opts: []forma.TxOption{forma.WithTxMaxPhysicalRows(2)},
			expect: func(mock pgxmock.PgxPoolIface) {
				expectLock(mock).WillReturnResult(pgxmock.NewResult("SELECT", 1))
				mock.ExpectQuery(`^DELETE FROM .*entity_main`).WithArgs(pgxmock.AnyArg(), txTestRow).WillReturnRows(
					pgxmock.NewRows([]string{"ltbase_updated_at"}).AddRow(int64(1000)).
						AddCommandTag(pgconn.NewCommandTag("DELETE 1")))
				mock.ExpectExec(`^DELETE FROM .*eav_data`).WithArgs(pgxmock.AnyArg(), txTestRow).WillReturnResult(pgxmock.NewResult("DELETE", 3))
			},
			run:      func(ctx context.Context, tx forma.Tx) error { return tx.Delete(ctx, deleteOp("visit")) },
			class:    forma.ErrInvalidInput,
			poisoned: true,
		},
		{
			name: "concurrent use poisons",
			run: func(ctx context.Context, tx forma.Tx) error {
				session := tx.(*txSession)
				session.busy.Lock() // a call running on another goroutine
				defer session.busy.Unlock()
				return tx.Delete(ctx, deleteOp("visit"))
			},
			class:    forma.ErrInvalidInput,
			poisoned: true,
		},
	}
}

func TestTxPoisoningTruthTable(t *testing.T) {
	for _, tc := range poisoningCases() {
		t.Run(tc.name, func(t *testing.T) {
			txr, mock := newMockTransactor(t)
			mock.ExpectBeginTx(readCommittedTx)
			if tc.expect != nil {
				tc.expect(mock)
			}
			if tc.poisoned {
				mock.ExpectRollback()
			} else {
				mock.ExpectCommit()
			}

			var opErr error
			err := txr.InTx(context.Background(), func(ctx context.Context, tx forma.Tx) error {
				opErr = tc.run(ctx, tx)
				require.Error(t, opErr)
				if tc.class != nil {
					require.ErrorIs(t, opErr, tc.class)
				}
				// A later call answers ErrTxAborted with the cause, or runs.
				later := tx.Delete(ctx, deleteOp(""))
				if tc.poisoned {
					require.ErrorIs(t, later, forma.ErrTxAborted)
					require.ErrorIs(t, later, opErr, "the abort wraps the poisoning cause")
				} else {
					require.NotErrorIs(t, later, forma.ErrTxAborted)
				}
				return nil // fn swallows the error: only poisoning decides
			}, tc.opts...)
			if tc.poisoned {
				require.ErrorIs(t, err, forma.ErrTxAborted, "a poisoned Tx never commits")
				require.ErrorIs(t, err, opErr)
			} else {
				require.NoError(t, err, "an expected outcome leaves the Tx able to commit")
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestPoisonsByErrorClass(t *testing.T) {
	uniqueFromStatement := classifyStatementError(pgError("23505", "duplicate key"))
	cases := []struct {
		name         string
		err          error
		sent, failed bool
		want         bool
	}{
		{"not found after SQL", forma.NotFoundf("gone"), true, false, false},
		{"not found beside a failed statement", forma.NotFoundf("gone"), true, true, true},
		{"precondition failed without a failed statement", forma.PreconditionFailedf("stale"), true, false, false},
		{"unique conflict without a failed statement", forma.UniqueConflictf("taken"), true, false, false},
		{"unique conflict from a 23505", uniqueFromStatement, true, true, true},
		{"invalid input before SQL", forma.InvalidInputf("bad"), false, false, false},
		{"invalid input after SQL", forma.InvalidInputf("bad"), true, false, true},
		{"retryable conflict", classifyStatementError(pgError("40P01", "deadlock")), true, true, true},
		{"plain error before SQL", errors.New("registry down"), false, false, true},
		{"cancelled context", fmt.Errorf("lock: %w", context.Canceled), true, true, true},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, poisons(tc.err, tc.sent, tc.failed), tc.name)
	}
}

func TestInTxReportsTheRetryableCauseWhateverFnReturns(t *testing.T) {
	txr, mock := newMockTransactor(t)
	mock.ExpectBeginTx(readCommittedTx)
	expectLock(mock).WillReturnError(pgError("40P01", "deadlock detected"))
	mock.ExpectRollback()

	mine := errors.New("step failed")
	err := txr.InTx(context.Background(), func(ctx context.Context, tx forma.Tx) error {
		_ = tx.Delete(ctx, deleteOp("visit"))
		return mine
	})
	require.ErrorIs(t, err, mine, "fn's error is returned")
	require.ErrorIs(t, err, forma.ErrTxAborted)
	assert.True(t, forma.IsRetryable(err), "the deadlock that poisoned the Tx stays visible")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTxMeterCountsMainEAVAndChangeLogRows(t *testing.T) {
	txr, mock := newMockTransactor(t)
	mock.ExpectBeginTx(readCommittedTx)
	expectDeleteFound(mock, 3)
	mock.ExpectCommit()

	err := txr.InTx(context.Background(), func(ctx context.Context, tx forma.Tx) error {
		require.NoError(t, tx.Delete(ctx, deleteOp("visit")))
		meter := tx.(*txSession).meter
		assert.Equal(t, int64(5), meter.rows, "1 entity_main + 3 EAV + 1 change_log; the lock's SELECT counts nothing")
		assert.Equal(t, 4, meter.statements)
		assert.Zero(t, meter.failures)
		return nil
	}, forma.WithTxMaxPhysicalRows(5))
	require.NoError(t, err, "a Tx exactly at its row limit commits")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTxRowLimitErrorNamesTheLimit(t *testing.T) {
	txr, mock := newMockTransactor(t)
	mock.ExpectBeginTx(readCommittedTx)
	expectLock(mock).WillReturnResult(pgxmock.NewResult("SELECT", 1))
	mock.ExpectQuery(`^DELETE FROM .*entity_main`).WithArgs(pgxmock.AnyArg(), txTestRow).WillReturnRows(
		pgxmock.NewRows([]string{"ltbase_updated_at"}).AddRow(int64(1000)).
			AddCommandTag(pgconn.NewCommandTag("DELETE 1")))
	mock.ExpectExec(`^DELETE FROM .*eav_data`).WithArgs(pgxmock.AnyArg(), txTestRow).WillReturnResult(pgxmock.NewResult("DELETE", 2))
	mock.ExpectRollback()

	err := txr.InTx(context.Background(), func(ctx context.Context, tx forma.Tx) error {
		return tx.Delete(ctx, deleteOp("visit"))
	}, forma.WithTxMaxPhysicalRows(1), forma.WithTxMaxPhysicalRows(0), forma.WithTxMaxPhysicalRows(1))
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	msg, published := forma.ResolvePublicMessage(err)
	require.True(t, published)
	assert.Contains(t, msg, "limit of 1 physical rows", "options apply in order")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTxOperationLimitDefaultsTo1000(t *testing.T) {
	txr, mock := newMockTransactor(t)
	mock.ExpectBeginTx(readCommittedTx)
	mock.ExpectRollback()

	err := txr.InTx(context.Background(), func(ctx context.Context, tx forma.Tx) error {
		for i := 1; i <= forma.DefaultTxMaxOperations; i++ {
			err := tx.Delete(ctx, deleteOp(""))
			require.ErrorIs(t, err, forma.ErrInvalidInput, "operation %d", i)
			require.NotErrorIs(t, err, forma.ErrTxAborted, "operation %d", i)
		}
		err := tx.Delete(ctx, deleteOp("visit"))
		msg, _ := forma.ResolvePublicMessage(err)
		assert.Contains(t, msg, "operation 1001 would exceed the transaction's limit of 1000 operations")
		return err
	})
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.NoError(t, mock.ExpectationsWereMet(), "operation 1001 sent nothing")
}

func TestTxMeterRefusesBatches(t *testing.T) {
	meter := &txMeter{maxRows: 10}
	results := (&meteredTx{meter: meter}).SendBatch(context.Background(), nil)
	_, err := results.Exec()
	require.ErrorContains(t, err, "bypasses its statement meter")
	assert.Equal(t, 1, meter.failures, "a refused batch poisons like a failed statement")
}
