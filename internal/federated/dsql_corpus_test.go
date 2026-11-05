package federated

import (
	"context"
	"testing"

	"github.com/google/uuid"
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
