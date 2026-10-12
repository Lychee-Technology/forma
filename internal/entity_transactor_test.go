package internal

import (
	"context"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/transform"

	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The InTx lifecycle (#648) against a pgxmock pool: each test states which of
// COMMIT and ROLLBACK the transaction must end with, and the mock fails on the
// other.

var readCommittedTx = pgx.TxOptions{IsoLevel: pgx.ReadCommitted}

// newMockTransactor builds the transactor of a manager whose repository runs
// on a pgxmock pool, with the shipped schemas and named tables.
func newMockTransactor(t *testing.T) (*transactor, pgxmock.PgxPoolIface) {
	t.Helper()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	mock.MatchExpectationsInOrder(true)

	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	require.NoError(t, err)
	config := createTestConfig()
	config.Database.TableNames = forma.TableNames{EntityMain: "entity_main", EAVData: "eav_data", ChangeLog: "change_log"}
	config.Transaction.DefaultTimeout = 30 * time.Second
	repo := NewDBPersistentRecordRepository(mock, nil)
	em := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(registry), repo, nil, registry, config, nil)

	txr, err := NewTransactor(em)
	require.NoError(t, err)
	return txr.(*transactor), mock
}

func TestInTxCommitsWhenFnReturnsNil(t *testing.T) {
	txr, mock := newMockTransactor(t)
	mock.ExpectBeginTx(readCommittedTx)
	mock.ExpectCommit()

	called := 0
	err := txr.InTx(context.Background(), func(context.Context, forma.Tx) error {
		called++
		return nil
	}, nil)
	require.NoError(t, err, "a nil option is skipped")
	assert.Equal(t, 1, called, "fn runs exactly once")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestInTxRollsBackAndReturnsFnError(t *testing.T) {
	txr, mock := newMockTransactor(t)
	mock.ExpectBeginTx(readCommittedTx)
	mock.ExpectRollback()

	fnErr := forma.NotFoundf("instance gone")
	err := txr.InTx(context.Background(), func(context.Context, forma.Tx) error {
		return fnErr
	})
	require.ErrorIs(t, err, fnErr)
	require.ErrorIs(t, err, forma.ErrNotFound)
	msg, published := forma.ResolvePublicMessage(err)
	assert.True(t, published)
	assert.Equal(t, "instance gone", msg, "fn's own publication survives the rollback wrap")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestInTxRollsBackAndRepanicsWithTheSameValue(t *testing.T) {
	txr, mock := newMockTransactor(t)
	mock.ExpectBeginTx(readCommittedTx)
	mock.ExpectRollback()

	type marker struct{ name string }
	value := &marker{name: "boom"}
	var tx forma.Tx
	recovered := func() (r any) {
		defer func() { r = recover() }()
		_ = txr.InTx(context.Background(), func(_ context.Context, got forma.Tx) error {
			tx = got
			panic(value)
		})
		return nil
	}()
	require.Same(t, value, recovered, "the panic continues with its original value")
	require.NoError(t, mock.ExpectationsWereMet())
	require.ErrorIs(t, tx.Delete(context.Background(), &forma.EntityOperation{}), forma.ErrTxDone)
}

func TestInTxRollsBackOnGoexit(t *testing.T) {
	txr, mock := newMockTransactor(t)
	mock.ExpectBeginTx(readCommittedTx)
	mock.ExpectRollback()

	returned := false
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = txr.InTx(context.Background(), func(context.Context, forma.Tx) error {
			runtime.Goexit()
			return nil
		})
		returned = true
	}()
	<-done
	assert.False(t, returned, "Goexit does not return through InTx")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTxMethodsAnswerErrTxDoneAfterInTxReturns(t *testing.T) {
	txr, mock := newMockTransactor(t)
	mock.ExpectBeginTx(readCommittedTx)
	mock.ExpectCommit()

	var tx forma.Tx
	require.NoError(t, txr.InTx(context.Background(), func(_ context.Context, got forma.Tx) error {
		tx = got
		return nil
	}))

	ctx := context.Background()
	op := &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit"}, Data: visitPayload("v")}
	_, err := tx.Create(ctx, op)
	require.ErrorIs(t, err, forma.ErrTxDone)
	_, err = tx.Update(ctx, op)
	require.ErrorIs(t, err, forma.ErrTxDone)
	require.ErrorIs(t, tx.Delete(ctx, op), forma.ErrTxDone)
	require.NoError(t, mock.ExpectationsWereMet(), "a finished Tx sends nothing")
}

func TestNestedInTxIsRefusedWithoutBeginningATransaction(t *testing.T) {
	txr, mock := newMockTransactor(t)
	mock.ExpectBeginTx(readCommittedTx)
	mock.ExpectCommit()

	err := txr.InTx(context.Background(), func(ctx context.Context, _ forma.Tx) error {
		derived, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		nested := txr.InTx(derived, func(context.Context, forma.Tx) error {
			t.Error("the nested fn must not run")
			return nil
		})
		require.ErrorIs(t, nested, forma.ErrInvalidInput)
		return nil
	})
	require.NoError(t, err, "the refused nesting leaves the outer transaction usable")
	require.NoError(t, mock.ExpectationsWereMet(), "exactly one BEGIN")
}

func TestInTxRollsBackWhenItsContextEndsBeforeCommit(t *testing.T) {
	t.Run("cancelled", func(t *testing.T) {
		txr, mock := newMockTransactor(t)
		mock.ExpectBeginTx(readCommittedTx)
		mock.ExpectRollback()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := txr.InTx(ctx, func(context.Context, forma.Tx) error {
			cancel()
			return nil
		})
		require.ErrorIs(t, err, context.Canceled)
		require.NotErrorIs(t, err, forma.ErrCommitOutcomeUnknown, "no COMMIT was sent")
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("budget deadline passes", func(t *testing.T) {
		txr, mock := newMockTransactor(t)
		mock.ExpectBeginTx(readCommittedTx)
		mock.ExpectRollback()

		err := txr.InTx(context.Background(), func(ctx context.Context, _ forma.Tx) error {
			<-ctx.Done()
			return nil
		}, forma.WithTxTimeout(time.Millisecond))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("fn's own error still carries the context's", func(t *testing.T) {
		txr, mock := newMockTransactor(t)
		mock.ExpectBeginTx(readCommittedTx)
		mock.ExpectRollback()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fnErr := errors.New("step failed")
		err := txr.InTx(ctx, func(context.Context, forma.Tx) error {
			cancel()
			return fnErr
		})
		require.ErrorIs(t, err, fnErr)
		require.ErrorIs(t, err, context.Canceled)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestInTxClassifiesCommitAndBeginErrors(t *testing.T) {
	t.Run("an unanswered COMMIT is unknown", func(t *testing.T) {
		txr, mock := newMockTransactor(t)
		mock.ExpectBeginTx(readCommittedTx)
		mock.ExpectCommit().WillReturnError(io.ErrUnexpectedEOF)

		err := txr.InTx(context.Background(), func(context.Context, forma.Tx) error { return nil })
		require.ErrorIs(t, err, forma.ErrCommitOutcomeUnknown)
		assert.False(t, forma.IsRetryable(err))
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("a 40001 at COMMIT is retryable", func(t *testing.T) {
		txr, mock := newMockTransactor(t)
		mock.ExpectBeginTx(readCommittedTx)
		mock.ExpectCommit().WillReturnError(pgError("40001", "could not serialize access"))

		err := txr.InTx(context.Background(), func(context.Context, forma.Tx) error { return nil })
		assert.True(t, forma.IsRetryable(err))
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("a failed BEGIN runs nothing", func(t *testing.T) {
		txr, mock := newMockTransactor(t)
		mock.ExpectBeginTx(readCommittedTx).WillReturnError(errors.New("pool closed"))

		err := txr.InTx(context.Background(), func(context.Context, forma.Tx) error {
			t.Error("fn must not run without a transaction")
			return nil
		})
		require.ErrorContains(t, err, "begin transaction: pool closed")
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestInTxRefusesInvalidArgumentsBeforeBeginning(t *testing.T) {
	txr, mock := newMockTransactor(t)
	noop := func(context.Context, forma.Tx) error { return nil }
	for name, err := range map[string]error{
		"nil fn":               txr.InTx(context.Background(), nil),
		"zero operation limit": txr.InTx(context.Background(), noop, forma.WithTxMaxOperations(0)),
		"negative row limit":   txr.InTx(context.Background(), noop, forma.WithTxMaxPhysicalRows(-1)),
		"negative timeout":     txr.InTx(context.Background(), noop, forma.WithTxTimeout(-time.Second)),
	} {
		assert.ErrorIs(t, err, forma.ErrInvalidInput, name)
	}
	require.NoError(t, mock.ExpectationsWereMet(), "no BEGIN for a refused call")
}

func TestNewTransactorRefusesManagersItCannotDrive(t *testing.T) {
	_, err := NewTransactor(nil)
	require.ErrorIs(t, err, forma.ErrInvalidInput)

	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	require.NoError(t, err)
	overMock := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(registry),
		newMockPersistentRecordRepository(), nil, registry, createTestConfig(), nil)
	_, err = NewTransactor(overMock)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.ErrorContains(t, err, "PostgreSQL repository")
}
