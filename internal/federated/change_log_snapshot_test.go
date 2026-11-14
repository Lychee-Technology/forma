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

// The strict arm, then the widened arm of the hot form, as the two ranges of
// the (schema_id, flushed_at) index.
var (
	strictDirtySetSQL = `^SELECT row_id FROM "change_log" WHERE schema_id = \$1 AND flushed_at = 0$`
	hotDirtySetSQL    = `^SELECT row_id FROM "change_log" WHERE schema_id = \$1 AND flushed_at = 0\s+UNION\s+` +
		`SELECT row_id FROM "change_log" WHERE schema_id = \$1 AND flushed_at >= \$2$`
	parquetHistorySQL = `^SELECT EXISTS \(SELECT 1 FROM "change_log" WHERE schema_id = \$1 AND flushed_at <> 0\)`
)

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

// The hot-excluded form reads today's strict set inside the snapshot, and
// reads no parquet history unless asked.
func TestReadChangeLogSnapshotHotExcludedReadsStrictSet(t *testing.T) {
	mock := newSnapshotMock(t)
	a, b := uuid.New(), uuid.New()
	mock.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	mock.ExpectQuery(strictDirtySetSQL).WithArgs(int16(100)).WillReturnRows(rowIDRows(a, b))
	mock.ExpectCommit()

	snap, err := NewPostgresDirtyIDFetcher(mock).ReadChangeLogSnapshot(context.Background(), ChangeLogSnapshotRequest{
		Tables: snapshotTables, SchemaID: 100, Form: DirtySetHotExcluded, GraceCutoffMs: 1_700_000_000_000,
	})
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{a, b}, snap.DirtyIDs)
	require.False(t, snap.HasParquetHistory)
	require.NoError(t, mock.ExpectationsWereMet())
}

// The hot form reads the widened set with the cutoff as its second argument,
// and the parquet-history answer from the same transaction.
func TestReadChangeLogSnapshotHotReadsWidenedSetAndHistory(t *testing.T) {
	mock := newSnapshotMock(t)
	a := uuid.New()
	const cutoff = int64(1_700_000_000_000)
	mock.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	mock.ExpectQuery(hotDirtySetSQL).WithArgs(int16(100), cutoff).WillReturnRows(rowIDRows(a))
	mock.ExpectQuery(parquetHistorySQL).WithArgs(int16(100)).WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectCommit()

	snap, err := NewPostgresDirtyIDFetcher(mock).ReadChangeLogSnapshot(context.Background(), ChangeLogSnapshotRequest{
		Tables: snapshotTables, SchemaID: 100, Form: DirtySetHot, GraceCutoffMs: cutoff, ParquetHistory: true,
	})
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{a}, snap.DirtyIDs)
	require.True(t, snap.HasParquetHistory)
	require.NoError(t, mock.ExpectationsWereMet())
}

// The cutoff passes through as given, even one past every marked entry: the
// reader does not resolve the cutoff itself.
func TestReadChangeLogSnapshotPassesCutoffThrough(t *testing.T) {
	const cutoff = int64(1<<63 - 1)
	query, args := dirtySetQuery(ChangeLogSnapshotRequest{Tables: snapshotTables, SchemaID: 7, Form: DirtySetHot, GraceCutoffMs: cutoff})
	require.Regexp(t, hotDirtySetSQL, query)
	require.Equal(t, []any{int16(7), cutoff}, args)
}

// A failed read releases the snapshot and commits nothing; its error names
// the form and the schema.
func TestReadChangeLogSnapshotFailedReadRollsBack(t *testing.T) {
	mock := newSnapshotMock(t)
	boom := errors.New("connection reset")
	mock.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	mock.ExpectQuery(hotDirtySetSQL).WithArgs(int16(100), int64(5)).WillReturnError(boom)
	mock.ExpectRollback()

	_, err := NewPostgresDirtyIDFetcher(mock).ReadChangeLogSnapshot(context.Background(), ChangeLogSnapshotRequest{
		Tables: snapshotTables, SchemaID: 100, Form: DirtySetHot, GraceCutoffMs: 5,
	})
	require.ErrorIs(t, err, boom)
	require.ErrorContains(t, err, "read hot dirty set of schema 100")
	require.NoError(t, mock.ExpectationsWereMet())
}

// A failed parquet-history read fails the snapshot read too: its answer must
// never default to "no history".
func TestReadChangeLogSnapshotFailedHistoryFailsTheRead(t *testing.T) {
	mock := newSnapshotMock(t)
	mock.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	mock.ExpectQuery(strictDirtySetSQL).WithArgs(int16(100)).WillReturnRows(rowIDRows())
	mock.ExpectQuery(parquetHistorySQL).WithArgs(int16(100)).WillReturnRows(pgxmock.NewRows([]string{"exists"}))
	mock.ExpectRollback()

	_, err := NewPostgresDirtyIDFetcher(mock).ReadChangeLogSnapshot(context.Background(), ChangeLogSnapshotRequest{
		Tables: snapshotTables, SchemaID: 100, Form: DirtySetHotExcluded, ParquetHistory: true,
	})
	require.ErrorContains(t, err, "read parquet history of schema 100: no row returned")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReadChangeLogSnapshotFailedBeginIsReported(t *testing.T) {
	mock := newSnapshotMock(t)
	boom := errors.New("too many connections")
	mock.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}).WillReturnError(boom)

	_, err := NewPostgresDirtyIDFetcher(mock).ReadChangeLogSnapshot(context.Background(), ChangeLogSnapshotRequest{
		Tables: snapshotTables, SchemaID: 100, Form: DirtySetHot,
	})
	require.ErrorIs(t, err, boom)
	require.ErrorContains(t, err, "begin read-only snapshot")
	require.NoError(t, mock.ExpectationsWereMet())
}

// A request the reader cannot honour is refused before any transaction opens.
func TestReadChangeLogSnapshotRefusesBadRequests(t *testing.T) {
	for name, tc := range map[string]struct {
		req  ChangeLogSnapshotRequest
		want string
	}{
		"no change log": {ChangeLogSnapshotRequest{Tables: model.StorageTables{EntityMain: "entity_main"}, Form: DirtySetHot},
			"change log table name cannot be empty"},
		"history without entity main": {ChangeLogSnapshotRequest{Tables: model.StorageTables{ChangeLog: "change_log"}, Form: DirtySetHot, ParquetHistory: true},
			"entity main table name cannot be empty"},
		"no form":      {ChangeLogSnapshotRequest{Tables: snapshotTables}, "unknown dirty set form DirtySetForm(0)"},
		"unknown form": {ChangeLogSnapshotRequest{Tables: snapshotTables, Form: DirtySetForm(9)}, "unknown dirty set form DirtySetForm(9)"},
	} {
		t.Run(name, func(t *testing.T) {
			mock := newSnapshotMock(t)
			_, err := NewPostgresDirtyIDFetcher(mock).ReadChangeLogSnapshot(context.Background(), tc.req)
			require.ErrorContains(t, err, tc.want)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
	var nilFetcher *PostgresDirtyIDFetcher
	_, err := nilFetcher.ReadChangeLogSnapshot(context.Background(), ChangeLogSnapshotRequest{Tables: snapshotTables, Form: DirtySetHot})
	require.ErrorContains(t, err, "postgres dirty id fetcher is not available")
}

// readChangeLogSnapshot sends its reads on the transaction it is handed, so a
// later step can read the hot keys after it in the same snapshot.
func TestReadChangeLogSnapshotOnCallerTransaction(t *testing.T) {
	mock := newSnapshotMock(t)
	a := uuid.New()
	mock.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	mock.ExpectQuery(hotDirtySetSQL).WithArgs(int16(3), int64(9)).WillReturnRows(rowIDRows(a))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT 1")).WillReturnRows(pgxmock.NewRows([]string{"n"}).AddRow(1))
	mock.ExpectCommit()

	ctx := context.Background()
	tx, err := beginReadSnapshot(ctx, mock)
	require.NoError(t, err)
	snap, err := readChangeLogSnapshot(ctx, tx, ChangeLogSnapshotRequest{Tables: snapshotTables, SchemaID: 3, Form: DirtySetHot, GraceCutoffMs: 9})
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{a}, snap.DirtyIDs)
	rows, err := tx.Query(ctx, "SELECT 1")
	require.NoError(t, err)
	rows.Close()
	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, mock.ExpectationsWereMet())
}
