package factory

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/federatedv2"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/testdb"
)

// The production construction of the federated engine (newPublicationReader
// and newRepositoryAndEngine, as newEntityManagerWithConfigContext calls
// them) over real PostgreSQL storage and publication tables, a DuckDB that
// fails every statement and an S3 client that fails every call.
// internal/federatedv2 pins the reader's record and manifest semantics; these
// tests pin that the factory wires them, with the epochs pinned at startup.

const (
	unpublishedSchema int16 = 7
	unrecordedSchema  int16 = 8
)

type wiredEngine struct {
	ctx    context.Context
	pool   *pgxpool.Pool
	tables model.StorageTables
	pub    publication.Tables
	s3     *stubS3Client
	engine *federated.DBFederatedQueryEngine
}

// newWiredEngine registers two schemas, records the first as UNPUBLISHED
// with three live rows, leaves the second without a record, and builds the
// engine as the factory does.
func newWiredEngine(t *testing.T) *wiredEngine {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	pool := testdb.Connect(t, ctx)
	w := &wiredEngine{ctx: ctx, pool: pool, tables: testdb.CreateDSQLStorageTables(t, ctx, pool, "fpr"), s3: &stubS3Client{}}
	w.pub = publication.TablesFor(w.tables.EntityMain)
	for _, stmt := range publication.Statements(forma.EnginePostgreSQL, w.tables.EntityMain) {
		_, err := pool.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	t.Cleanup(func() {
		for _, table := range []string{w.pub.Record, w.pub.Generation, w.pub.Epoch, w.pub.Displaced} {
			_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
		}
	})
	w.initialize(t, unpublishedSchema)
	for i := 1; i <= 3; i++ {
		w.write(t, unpublishedSchema, i)
	}

	meta := schemameta.NewMetadataCache()
	attrs := forma.SchemaAttributeCache{
		"name": {AttributeID: 1, ValueType: forma.ValueTypeText, ColumnBinding: &forma.MainColumnBinding{ColumnName: "text_01"}},
	}
	require.NoError(t, meta.RegisterSchema("published_later", unpublishedSchema, attrs))
	require.NoError(t, meta.RegisterSchema("never_recorded", unrecordedSchema, attrs))
	cfg := forma.DefaultConfig(newMockSchemaRegistry())
	cfg.Database.TableNames = forma.TableNames{EntityMain: w.tables.EntityMain, EAVData: w.tables.EAVData, ChangeLog: w.tables.ChangeLog}
	cfg.DuckDB = duckDBReadConfig()
	cfg.DuckDB.AllowPartialDegradedMode = true

	swapPublicationS3Client(t, func(context.Context, forma.DuckDBConfig) (publicationS3Client, error) { return w.s3, nil })
	epochs, err := readPublicationEpochs(ctx, pool, w.tables.EntityMain)
	require.NoError(t, err)
	reader, err := newPublicationReader(ctx, publicationReaderSpec{duck: cfg.DuckDB, pool: pool, meta: meta, entityMain: w.tables.EntityMain, registryEpochs: epochs})
	require.NoError(t, err)
	_, w.engine = newRepositoryAndEngine(pool, meta, cfg, closedDuckDBClient(t), reader)
	return w
}

// closedDuckDBClient is a DuckDB client every statement fails on: a pass that
// answers through it ran none.
func closedDuckDBClient(t *testing.T) *federated.DuckDBClient {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	require.NoError(t, db.Close())
	return &federated.DuckDBClient{DB: db}
}

func (w *wiredEngine) initialize(t *testing.T, id int16) {
	t.Helper()
	tx, err := w.pool.Begin(w.ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(w.ctx) }()
	_, err = publication.Initialize(w.ctx, publication.PgxTx(tx), w.pub, id)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(w.ctx))
}

// rowID is the row ID of the fixture's row i.
func rowID(i int) uuid.UUID { return uuid.MustParse(fmt.Sprintf("00000000-0000-7000-8000-%012d", i)) }

// write inserts row i of schema id with an unflushed change_log entry.
func (w *wiredEngine) write(t *testing.T, id int16, i int) {
	t.Helper()
	rowID := rowID(i)
	quote := func(name string) string { return pgx.Identifier{name}.Sanitize() }
	_, err := w.pool.Exec(w.ctx, "INSERT INTO "+quote(w.tables.EntityMain)+
		" (ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, text_01) VALUES ($1, $2, $3, $3, $4)",
		id, rowID, int64(1000+i), fmt.Sprintf("row %d", i))
	require.NoError(t, err)
	_, err = w.pool.Exec(w.ctx, "INSERT INTO "+quote(w.tables.ChangeLog)+" (schema_id, row_id, changed_at) VALUES ($1, $2, $3)",
		id, rowID, int64(1000+i))
	require.NoError(t, err)
}

func query(id int16) *model.FederatedAttributeQuery {
	return &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: id, Limit: 10}}
}

// A never-published schema answers its live rows from PostgreSQL, offset and
// cursor pages alike, through the engine the factory built: DuckDB is closed,
// so no statement ran, and S3 was never called.
func TestFactoryEngineServesANeverPublishedSchemaFromPostgreSQL(t *testing.T) {
	w := newWiredEngine(t)

	page, err := w.engine.Query(w.ctx, w.tables, query(unpublishedSchema), &model.FederatedQueryOptions{})
	require.NoError(t, err)
	require.Len(t, page.Records, 3)
	require.Equal(t, int64(3), page.TotalRecords)

	columns := []model.KeysetColumn{{Attribute: "created_at", Direction: forma.SortOrderDesc}, {Attribute: "row_id", Direction: forma.SortOrderAsc}}
	q := query(unpublishedSchema)
	q.KeysetCursor = &model.KeysetCursor{Columns: columns, Values: []any{int64(1003), rowID(3)}, Mode: model.KeysetCursorModeAfter}
	records, _, err := w.engine.ExecuteDuckDBFederatedQuery(w.ctx, w.tables, q, 10, 0, nil, nil)
	require.NoError(t, err)
	require.Len(t, records, 2, "the cursor page after the newest row")
	require.Equal(t, []uuid.UUID{rowID(2), rowID(1)}, []uuid.UUID{records[0].RowID, records[1].RowID})
	require.Zero(t, w.s3.called(), "no manifest fetched, nothing probed")
}

// A schema without a record at startup gets no pin. Its reads fail closed
// with the typed, non-degradable consistency error even under
// AllowPartialDegradedMode, and keep failing once a record appears: the pins
// are the process's, read with its metadata.
func TestFactoryEnginePinsEpochsAtStartup(t *testing.T) {
	w := newWiredEngine(t)
	w.write(t, unrecordedSchema, 9)

	_, err := w.engine.Query(w.ctx, w.tables, query(unrecordedSchema), &model.FederatedQueryOptions{AllowPartialDegradedMode: true})
	require.ErrorIs(t, err, forma.ErrPublicationInconsistent)
	require.ErrorIs(t, err, publication.ErrNoRecord)
	require.NotErrorIs(t, err, federated.ErrFederatedReadFailed)

	w.initialize(t, unrecordedSchema)
	_, err = w.engine.Query(w.ctx, w.tables, query(unrecordedSchema), &model.FederatedQueryOptions{AllowPartialDegradedMode: true})
	require.ErrorIs(t, err, forma.ErrPublicationInconsistent)
	require.ErrorIs(t, err, federatedv2.ErrNotPinned)
	var typed *forma.PublicationConsistencyError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, unrecordedSchema, typed.SchemaID)
	require.Zero(t, w.s3.called())
}
