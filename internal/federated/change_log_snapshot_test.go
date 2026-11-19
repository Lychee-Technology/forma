package federated

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/require"
)

var snapshotTables = model.StorageTables{EntityMain: "entity_main", ChangeLog: "change_log"}

// strictDirtySetSQL is the dirty set every pass reads: the unflushed
// entries, one range of the (schema_id, flushed_at) index.
const strictDirtySetSQL = `^SELECT row_id FROM "change_log" WHERE schema_id = \$1 AND flushed_at = 0$`

func newSnapshotMock(t *testing.T) pgxmock.PgxPoolIface {
	t.Helper()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	return mock
}

func rowIDRows(ids ...uuid.UUID) *pgxmock.Rows {
	rows := pgxmock.NewRows([]string{"row_id"})
	for _, id := range ids {
		rows.AddRow(id)
	}
	return rows
}

// A snapshot reads the strict dirty set: no cutoff widens it, and nothing
// else is read.
func TestReadChangeLogSnapshotReadsTheStrictSet(t *testing.T) {
	mock := newSnapshotMock(t)
	a, b := uuid.New(), uuid.New()
	mock.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	mock.ExpectQuery(strictDirtySetSQL).WithArgs(int16(100)).WillReturnRows(rowIDRows(a, b))
	mock.ExpectCommit()

	snap, err := NewPostgresDirtyIDFetcher(mock).ReadChangeLogSnapshot(context.Background(), ChangeLogSnapshotRequest{
		Tables: snapshotTables, SchemaID: 100,
	})
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{a, b}, snap.DirtyIDs)
	require.NoError(t, mock.ExpectationsWereMet())
}

// A failed read releases the snapshot and commits nothing; its error names
// the schema.
func TestReadChangeLogSnapshotFailedReadRollsBack(t *testing.T) {
	mock := newSnapshotMock(t)
	boom := errors.New("connection reset")
	mock.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	mock.ExpectQuery(strictDirtySetSQL).WithArgs(int16(100)).WillReturnError(boom)
	mock.ExpectRollback()

	_, err := NewPostgresDirtyIDFetcher(mock).ReadChangeLogSnapshot(context.Background(), ChangeLogSnapshotRequest{
		Tables: snapshotTables, SchemaID: 100,
	})
	require.ErrorIs(t, err, boom)
	require.ErrorContains(t, err, "read the dirty set of schema 100")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReadChangeLogSnapshotFailedBeginIsReported(t *testing.T) {
	mock := newSnapshotMock(t)
	boom := errors.New("too many connections")
	mock.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}).WillReturnError(boom)

	_, err := NewPostgresDirtyIDFetcher(mock).ReadChangeLogSnapshot(context.Background(), ChangeLogSnapshotRequest{
		Tables: snapshotTables, SchemaID: 100,
	})
	require.ErrorIs(t, err, boom)
	require.ErrorContains(t, err, "begin read-only snapshot")
	require.NoError(t, mock.ExpectationsWereMet())
}

// A request without a change log is refused before any transaction opens.
func TestReadChangeLogSnapshotRefusesBadRequests(t *testing.T) {
	mock := newSnapshotMock(t)
	_, err := NewPostgresDirtyIDFetcher(mock).ReadChangeLogSnapshot(context.Background(),
		ChangeLogSnapshotRequest{Tables: model.StorageTables{EntityMain: "entity_main"}})
	require.ErrorContains(t, err, "change log table name cannot be empty")
	require.NoError(t, mock.ExpectationsWereMet())

	var nilFetcher *PostgresDirtyIDFetcher
	_, err = nilFetcher.ReadChangeLogSnapshot(context.Background(), ChangeLogSnapshotRequest{Tables: snapshotTables})
	require.ErrorContains(t, err, "postgres dirty id fetcher is not available")
}

// readChangeLogSnapshot sends its reads on the transaction it is handed, so a
// later step can read the hot keys after it in the same snapshot.
func TestReadChangeLogSnapshotOnCallerTransaction(t *testing.T) {
	mock := newSnapshotMock(t)
	a := uuid.New()
	mock.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	mock.ExpectQuery(strictDirtySetSQL).WithArgs(int16(3)).WillReturnRows(rowIDRows(a))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT 1")).WillReturnRows(pgxmock.NewRows([]string{"n"}).AddRow(1))
	mock.ExpectCommit()

	ctx := context.Background()
	tx, err := beginReadSnapshot(ctx, mock)
	require.NoError(t, err)
	snap, err := readChangeLogSnapshot(ctx, tx, ChangeLogSnapshotRequest{Tables: snapshotTables, SchemaID: 3})
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{a}, snap.DirtyIDs)
	rows, err := tx.Query(ctx, "SELECT 1")
	require.NoError(t, err)
	rows.Close()
	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, mock.ExpectationsWereMet())
}
