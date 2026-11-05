package cdc

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
	"github.com/lychee-technology/forma/internal/schemalease"
)

// TestDSQLCorpus exports the CDC statements the Aurora DSQL engine depends
// on for the DSQL lint gate (#740): the schema lease that replaces the
// PostgreSQL advisory lock, and the flush mark markInChunks sends in
// statements of at most markRowsPerStatement rows. TestDSQLCorpusExport and
// TestDSQLCorpusFlushReads export the export snapshots and the batch reads.
func TestDSQLCorpus(t *testing.T) {
	ctx := context.Background()
	var rec corpus.Recorder
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(rec.MatchSQL)))
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	holder := uuid.MustParse("00000000-0000-0000-0000-0000000000aa")
	rows := []uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000001"), uuid.MustParse("00000000-0000-0000-0000-000000000002")}
	mock.ExpectQuery("").WithArgs(anyArgs(3)...).WillReturnRows(sqlmock.NewRows([]string{"holder"}).AddRow(holder.String()))
	mock.ExpectExec("").WithArgs(anyArgs(3)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("").WithArgs(anyArgs(2)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("").WithArgs(anyArgs(2 + 2*len(rows))...).WillReturnRows(sqlmock.NewRows([]string{"row_id"}).AddRow(rows[0].String()))

	lease := newLeaseSchemaLocker(db, schemalease.Table("entity_main"), time.Minute, 15*time.Second)
	if _, err := lease.acquire(ctx, 100, holder); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := lease.renew(ctx, 100, holder); err != nil {
		t.Fatalf("renew: %v", err)
	}
	lease.deleteLease(100, holder)
	versions := map[uuid.UUID]int64{rows[0]: 10, rows[1]: 11}
	if _, err := markFlushedFor(forma.EngineAuroraDSQL)(ctx, db, "change_log", 100, rows, versions, 12); err != nil {
		t.Fatalf("mark flushed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the DSQL CDC path sent other statements than recorded: %v", err)
	}
	corpus.Export(t, corpus.Unit{Name: "internal-cdc-dsql", Engine: forma.EngineAuroraDSQL, Statements: rec.Take(t,
		"internal/cdc/schema_lease.go:leaseSchemaLocker.acquire",
		"internal/cdc/schema_lease.go:leaseSchemaLocker.renew",
		"internal/cdc/schema_lease.go:leaseSchemaLocker.deleteLease",
		"internal/cdc/helpers.go:MarkFlushedVersions",
	)})
}

func anyArgs(n int) []driver.Value {
	args := make([]driver.Value, n)
	for i := range args {
		args[i] = sqlmock.AnyArg()
	}
	return args
}
