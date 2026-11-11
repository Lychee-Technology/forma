package federated

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/pashagolub/pgxmock/v4"
)

// TestDSQLCorpus exports the change_log reads every federated query sends to
// the database for the DSQL lint gate (#740): the dirty set and the
// parquet-history probe.
func TestDSQLCorpus(t *testing.T) {
	ctx := context.Background()
	var rec corpus.Recorder
	mock, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherFunc(rec.MatchPgx)))
	if err != nil {
		t.Fatalf("create mock conn: %v", err)
	}
	t.Cleanup(func() { _ = mock.Close(ctx) })
	mock.ExpectQuery("").WithArgs(pgxmock.AnyArg()).WillReturnRows(
		pgxmock.NewRows([]string{"row_id"}).AddRow(uuid.MustParse("00000000-0000-0000-0000-000000000001")))
	mock.ExpectQuery("").WithArgs(pgxmock.AnyArg()).WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))

	fetcher := NewPostgresDirtyIDFetcher(mock)
	if _, err := fetcher.FetchDirtyRowIDs(ctx, "change_log", 100); err != nil {
		t.Fatalf("FetchDirtyRowIDs: %v", err)
	}
	if _, err := fetcher.HasParquetHistory(ctx, model.StorageTables{EntityMain: "entity_main", ChangeLog: "change_log"}, 100); err != nil {
		t.Fatalf("HasParquetHistory: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the federated change_log reads sent other statements than recorded: %v", err)
	}
	corpus.Export(t, corpus.Unit{Name: "internal-federated-change-log", Engine: forma.EngineAuroraDSQL, Statements: rec.Take(t,
		"internal/federated/change_log_reads.go:PostgresDirtyIDFetcher.FetchDirtyRowIDs",
		"internal/federated/change_log_reads.go:PostgresDirtyIDFetcher.HasParquetHistory",
	)})
}

const (
	labelChangeLogSnapshot = "internal/federated/change_log_snapshot.go:PostgresDirtyIDFetcher.ReadChangeLogSnapshot"
	labelDirtySetQuery     = "internal/federated/change_log_snapshot.go:dirtySetQuery"
	labelParquetHistory    = "internal/federated/change_log_reads.go:parquetHistoryQuery"
)

// TestDSQLCorpusChangeLogSnapshot exports the pgx hot leg's snapshot reads
// (design.md §6.1.1): the BEGIN pgx writes for the REPEATABLE READ READ ONLY
// options, the dirty set of each tier form, the parquet-history probe, and
// the COMMIT.
func TestDSQLCorpusChangeLogSnapshot(t *testing.T) {
	begin, commit := changeLogSnapshotBoundary(t)
	for name, req := range map[string]ChangeLogSnapshotRequest{
		"internal-federated-change-log-snapshot-hot": {Tables: snapshotTables, SchemaID: 100, Form: DirtySetHot,
			GraceCutoffMs: 1_700_000_000_000, ParquetHistory: true},
		"internal-federated-change-log-snapshot-hot-excluded": {Tables: snapshotTables, SchemaID: 100, Form: DirtySetHotExcluded},
	} {
		body := changeLogSnapshotReads(t, req)
		statements := append(append([]corpus.Statement{begin}, body...), commit)
		corpus.Export(t, corpus.Unit{Name: name, Engine: forma.EngineAuroraDSQL, Statements: statements})
	}
}

// changeLogSnapshotBoundary records the BEGIN and COMMIT a snapshot read
// sends, from a real pgx connection.
func changeLogSnapshotBoundary(t *testing.T) (begin, commit corpus.Statement) {
	t.Helper()
	ctx := context.Background()
	var rec corpus.Recorder
	tx, err := beginReadSnapshot(ctx, corpus.WireConn(t, &rec))
	if err != nil {
		t.Fatalf("beginReadSnapshot: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	statements := rec.Take(t, labelChangeLogSnapshot, labelChangeLogSnapshot)
	return statements[0], statements[1]
}

// changeLogSnapshotReads records the reads readChangeLogSnapshot sends for req.
func changeLogSnapshotReads(t *testing.T, req ChangeLogSnapshotRequest) []corpus.Statement {
	t.Helper()
	ctx := context.Background()
	var rec corpus.Recorder
	mock, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherFunc(rec.MatchPgx)))
	if err != nil {
		t.Fatalf("create mock conn: %v", err)
	}
	t.Cleanup(func() { _ = mock.Close(ctx) })
	mock.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	labels := []string{labelDirtySetQuery}
	_, args := dirtySetQuery(req)
	mock.ExpectQuery("").WithArgs(anyArgs(len(args))...).WillReturnRows(rowIDRows(uuid.MustParse("00000000-0000-0000-0000-000000000001")))
	if req.ParquetHistory {
		labels = append(labels, labelParquetHistory)
		mock.ExpectQuery("").WithArgs(pgxmock.AnyArg()).WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))
	}
	tx, err := beginReadSnapshot(ctx, mock)
	if err != nil {
		t.Fatalf("beginReadSnapshot: %v", err)
	}
	if _, err := readChangeLogSnapshot(ctx, tx, req); err != nil {
		t.Fatalf("readChangeLogSnapshot: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the snapshot reads sent other statements than recorded: %v", err)
	}
	return rec.Take(t, labels...)
}

func anyArgs(n int) []any {
	out := make([]any, n)
	for i := range out {
		out[i] = pgxmock.AnyArg()
	}
	return out
}
