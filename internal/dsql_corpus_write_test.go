package internal

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/pashagolub/pgxmock/v4"
)

// Origins of the write path's statements in the DSQL lint corpus (#740).
const (
	originWriteBegin   = "internal/postgres_persistent_repository_row_locking.go:guardRowLocking.writeTxOptions"
	originWriteCommit  = "internal/postgres_persistent_repository_tx.go:DBPersistentRecordRepository.inWriteTx"
	originGuardUpsert  = "internal/postgres_persistent_repository_row_locking.go:guardUpsertSQL"
	originChangeLog    = "internal/postgres_persistent_repository.go:DBPersistentRecordRepository.upsertChangeLog"
	originRowVersion   = "internal/postgres_persistent_repository.go:nextRowVersion"
	originInsertMain   = "internal/postgres_persistent_repository_main_table.go:buildInsertMainStatement"
	originUpdateMain   = "internal/postgres_persistent_repository_main_table.go:buildUpdateMainStatement"
	originRequeueMain  = "internal/postgres_persistent_repository_requeue.go:buildRequeueMainStatement"
	originInsertEAV    = "internal/postgres_persistent_repository_eav.go:DBPersistentRecordRepository.insertEAVAttributes"
	originReplaceEAV   = "internal/postgres_persistent_repository_eav.go:DBPersistentRecordRepository.replaceEAVAttributesScoped"
	originDeleteRow    = "internal/postgres_persistent_repository.go:DBPersistentRecordRepository.deleteRecordInTx"
	originBatchDelete  = "internal/postgres_persistent_repository_batch.go:DBPersistentRecordRepository.batchDeleteInTx"
	originReleaseKeys  = "internal/postgres_persistent_repository_unique_keys.go:DBPersistentRecordRepository.releaseRowUniqueKeys"
	corpusSchemaID     = int16(100)
	corpusKeyedSchema  = "lead"
	corpusNowMillis    = int64(1_700_000_000_000)
	corpusPriorVersion = int64(1_699_999_999_000)
)

var (
	corpusTables = model.StorageTables{EntityMain: "entity_main", EAVData: "eav_data", ChangeLog: "change_log"}
	corpusRowA   = uuid.MustParse("00000000-0000-0000-0000-000000000001")
	corpusRowB   = uuid.MustParse("00000000-0000-0000-0000-000000000002")
)

// splitPool begins every transaction twice: on a corpus wire, where real
// pgx writes the BEGIN and COMMIT for the transaction's options, and on a
// pgxmock connection, which receives the statements of the body. Both
// record into one corpus.Recorder, so the corpus holds the transaction in
// the order the repository sends it.
type splitPool struct {
	wire *pgx.Conn
	mock pgxmock.PgxConnIface
}

func (p splitPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.mock.Query(ctx, sql, args...)
}

func (p splitPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.mock.QueryRow(ctx, sql, args...)
}

func (p splitPool) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	wireTx, err := p.wire.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	mockTx, err := p.mock.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return splitTx{Tx: mockTx, wire: wireTx}, nil
}

// splitTx runs statements on the mock and ends the transaction on the wire.
type splitTx struct {
	pgx.Tx
	wire pgx.Tx
}

func (t splitTx) Commit(ctx context.Context) error   { return t.wire.Commit(ctx) }
func (t splitTx) Rollback(ctx context.Context) error { return t.wire.Rollback(ctx) }

// corpusRepo answers a repository on the Aurora DSQL engine's row locking,
// enforcing one unique key on corpusSchemaID, over a split pool recording
// into rec, and the mock the test scripts results on.
func corpusRepo(t *testing.T, rec *corpus.Recorder) (*DBPersistentRecordRepository, pgxmock.PgxConnIface) {
	t.Helper()
	mock, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherFunc(rec.Match)))
	if err != nil {
		t.Fatalf("create mock conn: %v", err)
	}
	t.Cleanup(func() { _ = mock.Close(context.Background()) })
	catalog := &UniqueKeyCatalog{
		bySchema: map[int16][]uniqueKeySpec{corpusSchemaID: {{id: 1, name: "email"}}},
		names:    map[int16]string{corpusSchemaID: corpusKeyedSchema},
	}
	repo := NewDBPersistentRecordRepository(splitPool{wire: corpus.WireConn(t, rec), mock: mock}, nil, WithUniqueKeyCatalog(catalog))
	repo.locks = guardRowLocking{}
	repo.nowFunc = func() time.Time { return time.UnixMilli(corpusNowMillis) }
	return repo, mock
}

// TestDSQLCorpusWriteTransactions renders whole write transactions of the
// Aurora DSQL engine for the DSQL lint gate (#740), BEGIN to COMMIT, by
// calling the repository's entry points: a create, a delete, a batch delete
// and a requeue. The create's schema declares no unique key, so its claim
// is empty; the unique-key statements are rendered on their own.
func TestDSQLCorpusWriteTransactions(t *testing.T) {
	ctx := context.Background()
	var rec corpus.Recorder
	repo, mock := corpusRepo(t, &rec)

	// Create: schema 101 has no unique key.
	text := "hello"
	record := &model.PersistentRecord{SchemaID: 101, RowID: corpusRowA, TextItems: map[string]string{"text_01": "open"},
		OtherAttributes: []model.EAVRecord{{SchemaID: 101, RowID: corpusRowA, AttrID: 7, ValueText: &text}}}
	mock.ExpectBegin()
	mock.ExpectExec("").WithArgs(anyArgs(2)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectQuery("").WithArgs(anyArgs(2)...).WillReturnRows(pgxmock.NewRows([]string{"max"}).AddRow(int64(0)))
	mock.ExpectExec("").WithArgs(anyArgs(5)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec("").WithArgs(anyArgs(6)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec("").WithArgs(anyArgs(5)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	if err := repo.InsertPersistentRecord(ctx, corpusTables, record); err != nil {
		t.Fatalf("InsertPersistentRecord: %v", err)
	}
	create := rec.Take(t, originWriteBegin, originGuardUpsert, originRowVersion, originInsertMain, originInsertEAV, originChangeLog, originWriteCommit)

	// Delete of a row of the keyed schema: its key entries are released.
	mock.ExpectBegin()
	mock.ExpectExec("").WithArgs(anyArgs(2)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectQuery("").WithArgs(anyArgs(2)...).WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at"}).AddRow(corpusPriorVersion))
	mock.ExpectExec("").WithArgs(anyArgs(2)...).WillReturnResult(pgxmock.NewResult("DELETE", 1))
	mock.ExpectExec("").WithArgs(anyArgs(2)...).WillReturnResult(pgxmock.NewResult("DELETE", 1))
	mock.ExpectExec("").WithArgs(anyArgs(5)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	if err := repo.DeletePersistentRecord(ctx, corpusTables, corpusSchemaID, corpusRowA, model.RowGuard{}); err != nil {
		t.Fatalf("DeletePersistentRecord: %v", err)
	}
	del := rec.Take(t, originWriteBegin, originGuardUpsert, originDeleteRow, originDeleteRow, originReleaseKeys, originChangeLog, originWriteCommit)

	// Batch delete of two rows: one guard upsert for both, then each row.
	mock.ExpectBegin()
	mock.ExpectExec("").WithArgs(anyArgs(4)...).WillReturnResult(pgxmock.NewResult("INSERT", 2))
	for range 2 {
		mock.ExpectQuery("").WithArgs(anyArgs(2)...).WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at"}).AddRow(corpusPriorVersion))
		mock.ExpectExec("").WithArgs(anyArgs(2)...).WillReturnResult(pgxmock.NewResult("DELETE", 1))
		mock.ExpectExec("").WithArgs(anyArgs(2)...).WillReturnResult(pgxmock.NewResult("DELETE", 1))
		mock.ExpectExec("").WithArgs(anyArgs(5)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	}
	keys := []model.PersistentRecordKey{{SchemaID: corpusSchemaID, RowID: corpusRowA}, {SchemaID: corpusSchemaID, RowID: corpusRowB}}
	if err := repo.BatchDeletePersistentRecords(ctx, corpusTables, keys); err != nil {
		t.Fatalf("BatchDeletePersistentRecords: %v", err)
	}
	perRow := []string{originBatchDelete, originBatchDelete, originReleaseKeys, originChangeLog}
	batch := rec.Take(t, append(append(append([]string{originWriteBegin, originGuardUpsert}, perRow...), perRow...), originWriteCommit)...)

	// Requeue: a version-only write.
	mock.ExpectBegin()
	mock.ExpectExec("").WithArgs(anyArgs(2)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectQuery("").WithArgs(anyArgs(3)...).WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at", "ltbase_deleted_at"}).AddRow(corpusNowMillis, (*int64)(nil)))
	mock.ExpectExec("").WithArgs(anyArgs(5)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	if err := repo.RequeueForFlush(ctx, corpusTables, corpusSchemaID, corpusRowA); err != nil {
		t.Fatalf("RequeueForFlush: %v", err)
	}
	requeue := rec.Take(t, originWriteBegin, originGuardUpsert, originRequeueMain, originChangeLog, originWriteCommit)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the DSQL write path sent other statements than recorded: %v", err)
	}
	corpus.Export(t, corpus.Unit{Name: "internal-dsql-write-create", Statements: create})
	corpus.Export(t, corpus.Unit{Name: "internal-dsql-write-delete", Statements: del})
	corpus.Export(t, corpus.Unit{Name: "internal-dsql-write-batch-delete", Statements: batch})
	corpus.Export(t, corpus.Unit{Name: "internal-dsql-write-requeue", Statements: requeue})
}

// TestDSQLCorpusMainTableStatements renders the entity_main insert and
// update builders with every branch their SQL takes: a live and a deleted
// record, and a record setting a column of every typed family.
func TestDSQLCorpusMainTableStatements(t *testing.T) {
	deletedAt := corpusNowMillis
	full := &model.PersistentRecord{
		SchemaID: corpusSchemaID, RowID: corpusRowA, CreatedAt: corpusNowMillis, UpdatedAt: corpusNowMillis, DeletedAt: &deletedAt,
		TextItems: map[string]string{"text_01": "open", "ltbase_created_by": "u1"}, Int16Items: map[string]int16{"smallint_01": 1},
		Int32Items: map[string]int32{"integer_01": 2}, Int64Items: map[string]int64{"bigint_01": 3},
		Float64Items: map[string]float64{"double_01": 4.5}, UUIDItems: map[string]uuid.UUID{"uuid_01": corpusRowB},
	}
	live := &model.PersistentRecord{SchemaID: corpusSchemaID, RowID: corpusRowA, CreatedAt: corpusNowMillis, UpdatedAt: corpusNowMillis}
	var statements []corpus.Statement
	for _, r := range []*model.PersistentRecord{live, full} {
		insert, _, err := buildInsertMainStatement(corpusTables.EntityMain, r)
		if err != nil {
			t.Fatalf("buildInsertMainStatement: %v", err)
		}
		update, _, err := buildUpdateMainStatement(corpusTables.EntityMain, r)
		if err != nil {
			t.Fatalf("buildUpdateMainStatement: %v", err)
		}
		statements = append(statements, corpus.Statement{Origin: originInsertMain, SQL: insert}, corpus.Statement{Origin: originUpdateMain, SQL: update})
	}

	// An update replaces the row's EAV attributes within the schema's known ids.
	var rec corpus.Recorder
	mock, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherFunc(rec.Match)))
	if err != nil {
		t.Fatalf("create mock conn: %v", err)
	}
	t.Cleanup(func() { _ = mock.Close(context.Background()) })
	mock.ExpectBegin()
	mock.ExpectExec("").WithArgs(anyArgs(3)...).WillReturnResult(pgxmock.NewResult("DELETE", 1))
	mock.ExpectExec("").WithArgs(anyArgs(12)...).WillReturnResult(pgxmock.NewResult("INSERT", 2))
	tx, err := mock.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	text, number := "x", 1.5
	attrs := []model.EAVRecord{
		{SchemaID: corpusSchemaID, RowID: corpusRowA, AttrID: 7, ValueText: &text},
		{SchemaID: corpusSchemaID, RowID: corpusRowA, AttrID: 8, ArrayIndices: "0", ValueNumeric: &number},
	}
	repo := &DBPersistentRecordRepository{}
	if err := repo.replaceEAVAttributesScoped(context.Background(), tx, corpusTables.EAVData, corpusSchemaID, corpusRowA, attrs, []int16{7, 8}); err != nil {
		t.Fatalf("replaceEAVAttributesScoped: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("replaceEAVAttributesScoped sent other statements than recorded: %v", err)
	}
	statements = append(statements, rec.Take(t, originReplaceEAV, originInsertEAV)...)
	corpus.Export(t, corpus.Unit{Name: "internal-dsql-main-table", Statements: statements})
}
