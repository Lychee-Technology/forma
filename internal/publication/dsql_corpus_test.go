package publication

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
)

// TestDSQLCorpus exports the publication contract's statements for the DSQL
// lint gate (#740): the two tables' DDL, and every statement the transitions,
// the reader's read, the commit-outcome check and the collector send, in the
// order a writer's life sends them.
func TestDSQLCorpus(t *testing.T) {
	ctx := context.Background()
	var rec corpus.Recorder
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(rec.MatchSQL)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	tb := TablesFor("entity_main")
	cur := Record{SchemaID: 100, Epoch: 1, State: Published, Generation: 3, ManifestKey: "k", ManifestSHA256: "s", Lineage: uuid.New(), Token: uuid.New()}
	recordRow := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"schema_id", "epoch", "state", "generation", "parent_generation", "manifest_key", "manifest_sha256", "lineage", "cas_token", "updated_at", "now"}).
			AddRow(cur.SchemaID, cur.Epoch, string(cur.State), cur.Generation, 2, cur.ManifestKey, cur.ManifestSHA256, cur.Lineage.String(), cur.Token.String(), 1, 2)
	}
	genRows := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"generation", "epoch", "kind", "manifest_key", "superseded_at"}).AddRow(3, 1, "flush", "k", 0)
	}

	mock.ExpectExec("").WithArgs(anyArgs(2)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnRows(recordRow())
	mock.ExpectQuery("").WithArgs(anyArgs(10)...).WillReturnRows(sqlmock.NewRows([]string{"generation", "updated_at"}).AddRow(4, 3))
	mock.ExpectExec("").WithArgs(anyArgs(2)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("").WithArgs(anyArgs(10)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("").WithArgs(anyArgs(3)...).WillReturnRows(sqlmock.NewRows([]string{"generation"}).AddRow(4))
	mock.ExpectQuery("").WithArgs(anyArgs(3)...).WillReturnRows(sqlmock.NewRows([]string{"generation", "kind"}).AddRow(4, "flush"))
	mock.ExpectQuery("").WithArgs(anyArgs(2)...).WillReturnRows(sqlmock.NewRows([]string{"cas_token"}).AddRow(uuid.NewString()))
	mock.ExpectQuery("").WithArgs(anyArgs(4)...).WillReturnRows(sqlmock.NewRows([]string{"epoch", "updated_at"}).AddRow(2, 4))
	mock.ExpectExec("").WithArgs(anyArgs(2)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("").WithArgs(anyArgs(2)...).WillReturnRows(genRows())
	mock.ExpectQuery("").WithArgs(anyArgs(2)...).WillReturnRows(genRows())
	mock.ExpectExec("").WithArgs(anyArgs(3)...).WillReturnResult(sqlmock.NewResult(0, 1))

	_, err = Initialize(ctx, db, tb, 100)
	require.NoError(t, err)
	_, err = Read(ctx, db, tb, 100)
	require.NoError(t, err)
	next := Next{Kind: KindFlush, Token: uuid.New(), ManifestKey: "k2", ManifestSHA256: "s2", Lineage: cur.Lineage, StagedAt: 1, StagingBudget: time.Minute}
	head, err := Publish(ctx, db, tb, cur, next)
	require.NoError(t, err)
	_, err = HoldLineage(ctx, db, tb, 100, head.Epoch, head.Lineage)
	require.NoError(t, err)
	_, err = KindsSince(ctx, db, tb, cur, head)
	require.NoError(t, err)
	_, err = Resolve(ctx, db, tb, 100, head.Generation, head.Token)
	require.NoError(t, err)
	_, err = Reset(ctx, db, tb, head)
	require.NoError(t, err)
	_, err = Retained(ctx, db, tb, 100, time.Hour)
	require.NoError(t, err)
	_, err = Expired(ctx, db, tb, 100, time.Hour)
	require.NoError(t, err)
	_, err = Forget(ctx, db, tb, 100, 3, time.Hour)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet(), "the publication contract sent other statements than recorded")

	ddl := Statements(forma.EngineAuroraDSQL, "entity_main")
	statements := []corpus.Statement{
		corpus.Rendered("internal/publication/ddl.go:recordDDL", ddl[0]),
		corpus.Rendered("internal/publication/ddl.go:generationDDL", ddl[1]),
	}
	statements = append(statements, rec.Take(t,
		"internal/publication/record.go:Initialize",
		"internal/publication/record.go:Read",
		"internal/publication/publish.go:Publish",
		"internal/publication/publish.go:Publish",
		"internal/publication/publish.go:Publish",
		"internal/publication/lifecycle.go:HoldLineage",
		"internal/publication/rebase.go:KindsSince",
		"internal/publication/lifecycle.go:Resolve",
		"internal/publication/lifecycle.go:Reset",
		"internal/publication/lifecycle.go:Reset",
		"internal/publication/retention.go:Retained",
		"internal/publication/retention.go:Expired",
		"internal/publication/retention.go:Forget",
	)...)
	corpus.Export(t, corpus.Unit{Name: "internal-publication", Engine: forma.EngineAuroraDSQL, Statements: statements})
}

func anyArgs(n int) []driver.Value {
	args := make([]driver.Value, n)
	for i := range args {
		args[i] = sqlmock.AnyArg()
	}
	return args
}
