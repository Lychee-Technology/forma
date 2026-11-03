package internal

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/pashagolub/pgxmock/v4"
)

// TestDSQLCorpus renders the write path's Aurora DSQL row locking for the
// DSQL lint gate (#740): the guard-row upsert and recheck probe guardRowLocking
// sends, the precondition probe and the transaction clock read. Whole write
// transactions and the unique-key statements are rendered by
// TestDSQLCorpusWriteTransactions, TestDSQLCorpusMainTableStatements and
// TestDSQLCorpusUniqueKeyStatements.
func TestDSQLCorpus(t *testing.T) {
	ctx := context.Background()
	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_data"}
	// The precondition fragment stands in for what buildHybridConditions
	// renders; the probe around it is production SQL.
	guard := &renderedGuard{clause: "m.text_01 = $3", args: []any{"open"}}

	var rec corpus.Recorder
	mock, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherFunc(rec.Match)))
	if err != nil {
		t.Fatalf("create mock conn: %v", err)
	}
	t.Cleanup(func() { _ = mock.Close(ctx) })
	mock.ExpectBegin()
	mock.ExpectExec("").WithArgs(anyArgs(2)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectQuery("").WithArgs(anyArgs(3)...).WillReturnRows(pgxmock.NewRows([]string{"holds"}).AddRow(true))
	mock.ExpectQuery("").WithArgs(anyArgs(3)...).WillReturnRows(pgxmock.NewRows([]string{"holds"}).AddRow(true))
	mock.ExpectQuery("").WillReturnRows(pgxmock.NewRows([]string{"ms"}).AddRow(int64(1)))
	tx, err := mock.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	rowID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	if err := lockRowInTx(ctx, tx, guardRowLocking{}, tables, 100, rowID, guard); err != nil {
		t.Fatalf("lockRowInTx: %v", err)
	}
	if _, err := checkRowGuard(ctx, tx, tables, 100, rowID, guard); err != nil {
		t.Fatalf("checkRowGuard: %v", err)
	}
	if _, err := txClock(tx)(ctx); err != nil {
		t.Fatalf("txClock: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the DSQL locking path sent other statements than recorded: %v", err)
	}
	statements := rec.Take(t,
		"internal/postgres_persistent_repository_row_locking.go:guardUpsertSQL",
		"internal/postgres_persistent_repository_row_lock.go:lockRowInTx",
		"internal/postgres_persistent_repository_guard.go:checkRowGuard",
		"internal/postgres_persistent_repository_guard.go:txClock",
	)
	// A batch writes up to guardRowsPerStatement guards per statement.
	statements = append(statements, corpus.Statement{
		Origin: "internal/postgres_persistent_repository_row_locking.go:guardUpsertSQL",
		SQL:    guardUpsertSQL(sanitizeIdentifier("entity_main_row_guard"), 3),
	})
	corpus.Export(t, corpus.Unit{Name: "internal-dsql-row-locking", Statements: statements})
}

func anyArgs(n int) []any {
	args := make([]any, n)
	for i := range args {
		args[i] = pgxmock.AnyArg()
	}
	return args
}
