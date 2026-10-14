package internal

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lychee-technology/forma"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tx reads and row assertions (#650) against a pgxmock pool: what a read
// refuses before any SQL, which outcomes leave the Tx usable, and that no
// read ever reaches the federated engine. The mock fails on any statement a
// test did not queue, so "no SQL" is checked by queuing none.

// countingEngine is a federated engine that only counts its calls.
type countingEngine struct{ calls int }

func (e *countingEngine) Query(context.Context, model.StorageTables, *model.FederatedAttributeQuery, *model.FederatedQueryOptions) (*model.PersistentRecordPage, error) {
	e.calls++
	return &model.PersistentRecordPage{}, nil
}

func federatedRead() *forma.QueryRequest {
	return &forma.QueryRequest{SchemaName: "visit", RowID: &txTestRow, Federated: &forma.FederatedQueryRequest{Enabled: true}}
}

// expectGetMissing queues the single-row read finding no row.
func expectGetMissing(mock pgxmock.PgxPoolIface) {
	mock.ExpectQuery(`^SELECT .* FROM "?entity_main"? m\s+LEFT JOIN LATERAL`).WithArgs(pgxmock.AnyArg(), txTestRow).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_row_id"}))
}

// expectRowLock queues AssertRow's lock and probe. found false means the
// probe finds no row; otherwise holds is the precondition's value.
func expectRowLock(mock pgxmock.PgxPoolIface, found, holds bool) {
	expectLock(mock).WillReturnResult(pgxmock.NewResult("SELECT", 1))
	rows := pgxmock.NewRows([]string{"coalesce"})
	if found {
		rows.AddRow(holds)
	}
	mock.ExpectQuery(`^SELECT COALESCE\(\(true\), false\) FROM "?entity_main"? m WHERE .* FOR UPDATE OF m$`).
		WithArgs(pgxmock.AnyArg(), txTestRow).WillReturnRows(rows.AddCommandTag(pgconn.NewCommandTag("SELECT 1")))
}

// expectEmptyQuery queues one optimized query that matches nothing.
func expectEmptyQuery(mock pgxmock.PgxPoolIface) {
	mock.ExpectQuery(`^\s*WITH anchor AS`).WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_row_id"}))
}

func TestTxReadsRefuseFederatedRequestsBeforeAnySQL(t *testing.T) {
	engine := &countingEngine{}
	txr, mock := newMockTransactorWithEngine(t, engine)
	mock.ExpectBeginTx(readCommittedTx)
	mock.ExpectCommit()

	err := txr.InTx(context.Background(), func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Get(ctx, federatedRead())
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		require.ErrorContains(t, err, "hot tier only")
		_, err = tx.Query(ctx, federatedRead())
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		require.ErrorContains(t, err, "hot tier only")
		return nil
	})
	require.NoError(t, err, "a refused read leaves the Tx able to commit")
	assert.Zero(t, engine.calls)
	require.NoError(t, mock.ExpectationsWereMet())
}

// Every Tx read and assertion, run on a manager that has a federated engine:
// none of them calls it.
func TestTxReadsNeverCallTheFederatedEngine(t *testing.T) {
	engine := &countingEngine{}
	txr, mock := newMockTransactorWithEngine(t, engine)
	mock.ExpectBeginTx(readCommittedTx)
	expectGetMissing(mock)
	expectRowLock(mock, true, true)
	expectGetMissing(mock)
	expectEmptyQuery(mock)
	expectRowLock(mock, true, true)
	mock.ExpectCommit()

	err := txr.InTx(context.Background(), func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Get(ctx, &forma.QueryRequest{SchemaName: "visit", RowID: &txTestRow})
		require.ErrorIs(t, err, forma.ErrNotFound)
		_, err = tx.Get(ctx, &forma.QueryRequest{SchemaName: "visit", RowID: &txTestRow}, forma.ForUpdate())
		require.ErrorIs(t, err, forma.ErrNotFound)
		result, err := tx.Query(ctx, &forma.QueryRequest{SchemaName: "visit", Page: 1, ItemsPerPage: 10})
		require.NoError(t, err)
		require.Empty(t, result.Data)
		require.NoError(t, tx.AssertRow(ctx, "visit", txTestRow, nil))
		_, err = tx.Get(ctx, federatedRead())
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		return nil
	})
	require.NoError(t, err)
	assert.Zero(t, engine.calls, "no Tx read reaches the federated engine")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTxQueryRefusesForUpdate(t *testing.T) {
	txr, mock := newMockTransactor(t)
	mock.ExpectBeginTx(readCommittedTx)
	mock.ExpectCommit()

	err := txr.InTx(context.Background(), func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Query(ctx, &forma.QueryRequest{SchemaName: "visit"}, forma.ForUpdate())
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		require.ErrorContains(t, err, "ForUpdate")
		_, err = tx.Get(ctx, nil)
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// The assertion outcomes of the poisoning rule (#648 rule 14): a missing row,
// a failed precondition and a request refused before SQL leave the Tx usable;
// a failed lock statement poisons it.
func TestTxAssertRowOutcomesAndPoisoning(t *testing.T) {
	cases := []struct {
		name     string
		expect   func(mock pgxmock.PgxPoolIface)
		run      func(ctx context.Context, tx forma.Tx) error
		class    error
		poisoned bool
	}{
		{
			name:   "a missing row is ErrNotFound",
			expect: func(mock pgxmock.PgxPoolIface) { expectRowLock(mock, false, false) },
			run:    func(ctx context.Context, tx forma.Tx) error { return tx.AssertRow(ctx, "visit", txTestRow, nil) },
			class:  forma.ErrNotFound,
		},
		{
			name:   "a row that fails the precondition is ErrPreconditionFailed",
			expect: func(mock pgxmock.PgxPoolIface) { expectRowLock(mock, true, false) },
			run:    func(ctx context.Context, tx forma.Tx) error { return tx.AssertRow(ctx, "visit", txTestRow, nil) },
			class:  forma.ErrPreconditionFailed,
		},
		{
			name: "an unknown schema is ErrNotFound before SQL",
			run: func(ctx context.Context, tx forma.Tx) error {
				return tx.AssertRow(ctx, "no_such_schema", txTestRow, nil)
			},
			class: forma.ErrNotFound,
		},
		{
			name: "a nil row ID is ErrInvalidInput before SQL",
			run: func(ctx context.Context, tx forma.Tx) error {
				return tx.AssertRow(ctx, "visit", [16]byte{}, nil)
			},
			class: forma.ErrInvalidInput,
		},
		{
			name: "a precondition on an unknown attribute is ErrInvalidInput before SQL",
			run: func(ctx context.Context, tx forma.Tx) error {
				return tx.AssertRow(ctx, "visit", txTestRow, &forma.KvCondition{Attr: "no_such_attr", Value: "equals:1"})
			},
			class: forma.ErrInvalidInput,
		},
		{
			name: "a failed lock statement poisons",
			expect: func(mock pgxmock.PgxPoolIface) {
				expectLock(mock).WillReturnError(&pgconn.PgError{Code: sqlstateDeadlockDetected})
			},
			run:      func(ctx context.Context, tx forma.Tx) error { return tx.AssertRow(ctx, "visit", txTestRow, nil) },
			class:    forma.ErrRetryableConflict,
			poisoned: true,
		},
	}
	for _, tc := range cases {
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
				require.ErrorIs(t, opErr, tc.class)
				later := tx.AssertRow(ctx, "", txTestRow, nil)
				if tc.poisoned {
					require.ErrorIs(t, later, forma.ErrTxAborted)
				} else {
					require.NotErrorIs(t, later, forma.ErrTxAborted)
				}
				return nil
			})
			if tc.poisoned {
				require.ErrorIs(t, err, forma.ErrTxAborted)
				require.ErrorIs(t, err, opErr)
			} else {
				require.NoError(t, err, "an expected outcome leaves the Tx able to commit")
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// Reads are Tx method calls: they count toward the operation limit.
func TestTxReadsCountTowardTheOperationLimit(t *testing.T) {
	txr, mock := newMockTransactor(t)
	mock.ExpectBeginTx(readCommittedTx)
	expectGetMissing(mock)
	mock.ExpectRollback()

	err := txr.InTx(context.Background(), func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Get(ctx, &forma.QueryRequest{SchemaName: "visit", RowID: &txTestRow})
		require.ErrorIs(t, err, forma.ErrNotFound)
		_, err = tx.Get(ctx, &forma.QueryRequest{SchemaName: "visit", RowID: &txTestRow})
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		require.ErrorContains(t, err, "limit of 1 operations")
		return nil
	}, forma.WithTxMaxOperations(1))
	require.ErrorIs(t, err, forma.ErrTxAborted)
	require.NoError(t, mock.ExpectationsWereMet())
}
