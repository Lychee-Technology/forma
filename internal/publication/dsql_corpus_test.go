package publication

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
)

// TestDSQLCorpus exports the publication contract's statements for the DSQL
// lint gate (#740): the four tables' DDL, and every statement the transitions,
// the reader's read, the commit-outcome check and its fence, and the
// collector send, in the order a writer's life sends them. Each transition
// runs in a transaction, as its Tx parameter demands.
func TestDSQLCorpus(t *testing.T) {
	ctx := context.Background()
	var rec corpus.Recorder
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(rec.MatchSQL)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	tb := TablesFor("entity_main")
	cur := Record{SchemaID: 100, Epoch: 1, State: Published, Generation: 3, ManifestKey: "k", ManifestSHA256: "s", Lineage: uuid.New(), Token: uuid.New()}
	expectTheContract(mock, cur)

	inTx := func(fn func(Tx) error) {
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		require.NoError(t, fn(SQLTx(tx)))
		require.NoError(t, tx.Commit())
	}
	inTx(func(tx Tx) error { _, err := Initialize(ctx, tx, tb, 100); return err })
	inTx(func(tx Tx) error { _, err := Initialize(ctx, tx, tb, 100); return err })
	_, err = ReadHistory(ctx, DB(db), tb, 100)
	require.NoError(t, err)
	inTx(func(tx Tx) error {
		_, _, err := InitializeAfterLoss(ctx, tx, tb, 100, History{Epoch: 3, Generations: 1, MaxEpoch: 1, MaxGeneration: 3}, AwaitingInit)
		return err
	})
	_, err = Read(ctx, DB(db), tb, 100)
	require.NoError(t, err)
	next := Next{Kind: KindFlush, Token: uuid.New(), ManifestKey: "k2", ManifestSHA256: strings.Repeat("2", 64), Lineage: cur.Lineage, StagedAt: 1, StagingBudget: time.Minute}
	var head Record
	inTx(func(tx Tx) error {
		if head, err = Publish(ctx, tx, tb, cur, next); err != nil {
			return err
		}
		_, err := HoldLineage(ctx, tx, tb, 100, head.Epoch, head.Lineage)
		return err
	})
	_, err = KindsSince(ctx, DB(db), tb, cur, head)
	require.NoError(t, err)
	_, err = Resolve(ctx, DB(db), tb, cur, next)
	require.NoError(t, err)
	inTx(func(tx Tx) error { _, err := Fence(ctx, tx, tb, cur); return err })
	var dropped Record
	inTx(func(tx Tx) error { dropped, err = Reset(ctx, tx, tb, head); return err })
	inTx(func(tx Tx) error {
		_, err := Activate(ctx, tx, tb, dropped, []Residue{{Table: "entity_main_change_log"}})
		return err
	})
	_, err = RaiseHorizon(ctx, DB(db), tb, 100, time.Hour)
	require.NoError(t, err)
	_, err = Retire(ctx, DB(db), tb, 100, 3)
	require.NoError(t, err)
	_, err = Retained(ctx, DB(db), tb, 100, time.Hour)
	require.NoError(t, err)
	_, err = Expired(ctx, DB(db), tb, 100, time.Hour)
	require.NoError(t, err)
	_, err = Forget(ctx, DB(db), tb, 100, 3, time.Hour)
	require.NoError(t, err)
	_, err = SchemaIDs(ctx, DB(db), tb)
	require.NoError(t, err)
	_, err = ReadLifecycleFence(ctx, DB(db), tb, 100, time.Hour)
	require.NoError(t, err)
	_, err = BindStorage(ctx, DB(db), tb, StorageTarget{Bucket: "bucket", Prefix: "data"})
	require.NoError(t, err)
	_, _, err = ReadStorage(ctx, DB(db), tb)
	require.NoError(t, err)
	_, err = Displaced(ctx, DB(db), tb, 100)
	require.NoError(t, err)
	_, err = ForgetDisplaced(ctx, DB(db), tb, 100, 4, time.Hour)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet(), "the publication contract sent other statements than recorded")

	ddl := Statements(forma.EngineAuroraDSQL, "entity_main")
	statements := []corpus.Statement{
		corpus.Rendered("internal/publication/ddl.go:recordDDL", ddl[0]),
		corpus.Rendered("internal/publication/ddl.go:generationDDL", ddl[1]),
		corpus.Rendered("internal/publication/ddl.go:epochDDL", ddl[2]),
		corpus.Rendered("internal/publication/displaced.go:displacedDDL", ddl[3]),
		corpus.Rendered("internal/publication/storage.go:storageDDL", StorageStatement(forma.EngineAuroraDSQL, "entity_main")),
	}
	statements = append(statements, rec.Take(t, contractLabels...)...)
	corpus.Export(t, corpus.Unit{Name: "internal-publication", Engine: forma.EngineAuroraDSQL, Statements: statements})
}

// contractLabels are the origins of the statements TestDSQLCorpus sends,
// in order.
var contractLabels = []string{
	"internal/publication/record.go:Initialize",
	"internal/publication/lifecycle_epoch.go:raiseEpoch",
	"internal/publication/record.go:Initialize",
	"internal/publication/record.go:Initialize",
	"internal/publication/recovery.go:ReadHistory",
	"internal/publication/recovery.go:InitializeAfterLoss",
	"internal/publication/lifecycle_epoch.go:raiseEpoch",
	"internal/publication/recovery.go:stampRecovery",
	"internal/publication/recovery.go:InitializeAfterLoss",
	"internal/publication/record.go:Read",
	"internal/publication/publish.go:Publish",
	"internal/publication/displaced.go:displace",
	"internal/publication/publish.go:supersede",
	"internal/publication/publish.go:Publish",
	"internal/publication/lifecycle.go:HoldLineage",
	"internal/publication/rebase.go:KindsSince",
	"internal/publication/lifecycle.go:Resolve",
	"internal/publication/lifecycle.go:Fence",
	"internal/publication/lifecycle.go:Reset",
	"internal/publication/lifecycle_epoch.go:raiseEpoch",
	"internal/publication/displaced.go:displace",
	"internal/publication/displaced.go:displace",
	"internal/publication/publish.go:supersede",
	"internal/publication/lifecycle.go:Activate",
	"internal/publication/lifecycle.go:Activate",
	"internal/publication/lifecycle_epoch.go:raiseEpoch",
	"internal/publication/retention.go:RaiseHorizon",
	"internal/publication/retention.go:Retire",
	"internal/publication/retention.go:Retained",
	"internal/publication/retention.go:Expired",
	"internal/publication/retention.go:Forget",
	"internal/publication/retention.go:SchemaIDs",
	"internal/publication/lifecycle_epoch.go:ReadLifecycleFence",
	"internal/publication/storage.go:BindStorage",
	"internal/publication/storage.go:BindStorage",
	"internal/publication/storage.go:ReadStorage",
	"internal/publication/displaced.go:Displaced",
	"internal/publication/displaced.go:ForgetDisplaced",
}

// expectTheContract answers, in order, every statement TestDSQLCorpus sends
// for cur's schema.
func expectTheContract(mock sqlmock.Sqlmock, cur Record) {
	recordRow := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"schema_id", "epoch", "state", "generation", "parent_generation", "manifest_key", "manifest_sha256", "lineage", "cas_token", "updated_at", "gc_horizon", "now"}).
			AddRow(cur.SchemaID, cur.Epoch, string(cur.State), cur.Generation, 2, cur.ManifestKey, cur.ManifestSHA256, cur.Lineage.String(), cur.Token.String(), 1, 0, 2)
	}
	genRows := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"generation", "epoch", "kind", "parent_generation", "manifest_key", "manifest_sha256", "cas_token", "superseded_at", "retired_at"}).
			AddRow(3, 1, "flush", 2, "k", "s", uuid.NewString(), 0, 0)
	}
	epochRow := func(epoch int64) *sqlmock.Rows {
		return sqlmock.NewRows([]string{"epoch", "updated_at"}).AddRow(epoch, 4)
	}

	mock.ExpectBegin()
	mock.ExpectExec("").WithArgs(anyArgs(3)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("").WithArgs(anyArgs(2)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec("").WithArgs(anyArgs(3)...).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnRows(sqlmock.NewRows([]string{"exists", "epoch"}).AddRow(true, 1))
	mock.ExpectCommit()
	mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnRows(sqlmock.NewRows([]string{"epoch", "count", "max_epoch", "max_generation"}).AddRow(3, 1, 1, 3))
	mock.ExpectBegin()
	mock.ExpectExec("").WithArgs(anyArgs(5)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("").WithArgs(anyArgs(2)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("").WithArgs(anyArgs(1)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("").WithArgs(anyArgs(1)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnRows(recordRow())
	mock.ExpectBegin()
	mock.ExpectQuery("").WithArgs(anyArgs(10)...).WillReturnRows(sqlmock.NewRows([]string{"generation", "parent_generation", "updated_at"}).AddRow(4, 3, 3))
	mock.ExpectQuery("").WithArgs(anyArgs(2)...).WillReturnRows(historyRow(cur.Epoch, cur.ManifestKey, cur.ManifestSHA256, cur.Token))
	mock.ExpectExec("").WithArgs(anyArgs(8)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("").WithArgs(anyArgs(10)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("").WithArgs(anyArgs(3)...).WillReturnRows(sqlmock.NewRows([]string{"generation"}).AddRow(4))
	mock.ExpectCommit()
	mock.ExpectQuery("").WithArgs(anyArgs(3)...).WillReturnRows(sqlmock.NewRows([]string{"generation", "kind"}).AddRow(4, "flush"))
	mock.ExpectQuery("").WithArgs(anyArgs(2)...).WillReturnRows(sqlmock.NewRows([]string{"epoch", "generation", "cas_token", "lineage", "cas_token"}).
		AddRow(1, 4, uuid.NewString(), cur.Lineage.String(), uuid.NewString()))
	mock.ExpectBegin()
	mock.ExpectExec("").WithArgs(anyArgs(4)...).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectQuery("").WithArgs(anyArgs(4)...).WillReturnRows(epochRow(2))
	mock.ExpectExec("").WithArgs(anyArgs(2)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("").WithArgs(anyArgs(2)...).WillReturnRows(historyRow(cur.Epoch, "elsewhere", cur.ManifestSHA256, cur.Token))
	mock.ExpectExec("").WithArgs(anyArgs(6)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("").WithArgs(anyArgs(8)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnRows(sqlmock.NewRows([]string{"one"}))
	mock.ExpectQuery("").WithArgs(anyArgs(4)...).WillReturnRows(epochRow(3))
	mock.ExpectExec("").WithArgs(anyArgs(2)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectQuery("").WithArgs(anyArgs(2)...).WillReturnRows(sqlmock.NewRows([]string{"gc_horizon"}).AddRow(5))
	mock.ExpectExec("").WithArgs(anyArgs(2)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("").WithArgs(anyArgs(2)...).WillReturnRows(genRows())
	mock.ExpectQuery("").WithArgs(anyArgs(2)...).WillReturnRows(genRows())
	mock.ExpectExec("").WithArgs(anyArgs(3)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"schema_id"}).AddRow(100))
	mock.ExpectQuery("").WithArgs(anyArgs(2)...).WillReturnRows(sqlmock.NewRows([]string{"epoch", "updated_at", "inside"}).AddRow(3, 4, false))
	mock.ExpectExec("").WithArgs(anyArgs(3)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("").WillReturnRows(storageRow())
	mock.ExpectQuery("").WillReturnRows(storageRow())
	mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnRows(sqlmock.NewRows([]string{"generation", "epoch", "manifest_key", "manifest_sha256", "cas_token", "displaced_at"}).
		AddRow(4, 1, "elsewhere", "s", uuid.NewString(), 7))
	mock.ExpectExec("").WithArgs(anyArgs(3)...).WillReturnResult(sqlmock.NewResult(0, 1))
}

// historyRow is the replaced generation's history row supersede reads.
func historyRow(epoch int64, key, digest string, token uuid.UUID) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"epoch", "manifest_key", "manifest_sha256", "cas_token"}).AddRow(epoch, key, digest, token.String())
}

func storageRow() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"endpoint", "bucket", "data_prefix", "bound_at"}).AddRow("", "bucket", "data", 6)
}

func anyArgs(n int) []driver.Value {
	args := make([]driver.Value, n)
	for i := range args {
		args[i] = sqlmock.AnyArg()
	}
	return args
}
