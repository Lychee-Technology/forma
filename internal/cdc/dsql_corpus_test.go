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
// PostgreSQL advisory lock. The flush mark and the publication are
// internal/cdcv2's and internal/publication's (their own corpus units).
// TestDSQLCorpusExport and TestDSQLCorpusFlushReads export the export
// snapshots and the run's planning reads.
func TestDSQLCorpus(t *testing.T) {
	ctx := context.Background()
	var rec corpus.Recorder
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(rec.MatchSQL)))
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	holder := uuid.MustParse("00000000-0000-0000-0000-0000000000aa")
	mock.ExpectQuery("").WithArgs(anyArgs(3)...).WillReturnRows(sqlmock.NewRows([]string{"holder"}).AddRow(holder.String()))
	mock.ExpectExec("").WithArgs(anyArgs(3)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("").WithArgs(anyArgs(2)...).WillReturnResult(sqlmock.NewResult(0, 1))

	lease := newLeaseSchemaLocker(db, schemalease.Table("entity_main"), time.Minute, 15*time.Second)
	if _, err := lease.acquire(ctx, 100, holder); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := lease.renew(ctx, 100, holder); err != nil {
		t.Fatalf("renew: %v", err)
	}
	lease.deleteLease(100, holder)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the DSQL CDC path sent other statements than recorded: %v", err)
	}
	corpus.Export(t, corpus.Unit{Name: "internal-cdc-dsql", Engine: forma.EngineAuroraDSQL, Statements: rec.Take(t,
		"internal/cdc/schema_lease.go:leaseSchemaLocker.acquire",
		"internal/cdc/schema_lease.go:leaseSchemaLocker.renew",
		"internal/cdc/schema_lease.go:leaseSchemaLocker.deleteLease",
	)})
}

func anyArgs(n int) []driver.Value {
	args := make([]driver.Value, n)
	for i := range args {
		args[i] = sqlmock.AnyArg()
	}
	return args
}
