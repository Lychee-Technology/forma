//go:build dsqlspike

package spikefix

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
	"github.com/pashagolub/pgxmock/v4"
)

// Labels are the expected execution site: "<Function>|<literal fragment>".
func TestDSQLCorpusSpikeAttribution(t *testing.T) {
	ctx := context.Background()
	var rec corpus.Recorder
	mock, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherFunc(rec.Match)))
	if err != nil {
		t.Fatal(err)
	}
	exec := func() { mock.ExpectExec("").WillReturnResult(pgxmock.NewResult("SELECT", 1)) }
	exec()
	exec()
	exec()
	mock.ExpectQuery("").WillReturnRows(pgxmock.NewRows([]string{"x"}))
	mock.ExpectQuery("").WillReturnRows(pgxmock.NewRows([]string{"x"}))
	exec()
	exec()
	exec()
	mock.ExpectExec("").WithArgs(7).WillReturnResult(pgxmock.NewResult("SELECT", 1))
	mock.ExpectExec("").WillReturnResult(pgxmock.NewResult("CREATE", 0))
	b := mock.ExpectBatch()
	b.ExpectExec("").WillReturnResult(pgxmock.NewResult("SELECT", 1))
	b.ExpectExec("").WillReturnResult(pgxmock.NewResult("SELECT", 1))

	must(t, DirectExec(ctx, mock))
	must(t, TwoCalls(ctx, mock))
	OneLine(ctx, mock)
	must(t, WrapperCallerA(ctx, mock))
	must(t, WrapperCallerB(ctx, mock))
	must(t, NestedCaller(ctx, mock))
	must(t, MultiLine(ctx, mock, 7))
	must(t, UsesBuilder(ctx, mock))
	must(t, Batched(ctx, mock))
	rec.Take(t, "DirectExec|direct", "TwoCalls|two-first", "TwoCalls|two-second", "OneLine|line-a", "OneLine|line-b",
		"WrapperCallerA|wrap-a", "WrapperCallerB|wrap-b", "NestedCaller|nested", "MultiLine|multi", "UsesBuilder|built",
		"Batched|batch-1", "Batched|batch-2", "Batched|batch-1", "Batched|batch-2")

	conn := corpus.WireConn(t, &rec)
	must(t, BeginWithOptions(ctx, conn))
	must(t, InClosure(ctx, conn))
	rec.Take(t, "BeginWithOptions|begin", "BeginWithOptions|commit", "InClosure|begin", "InClosure|closure", "InClosure|commit")

	db, smock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(rec.Match)))
	if err != nil {
		t.Fatal(err)
	}
	smock.ExpectExec("").WillReturnResult(sqlmock.NewResult(0, 1))
	must(t, SQLDirect(ctx, db))
	rec.Take(t, "SQLDirect|database-sql")
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
