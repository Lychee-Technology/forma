package cdcv2

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// TestDSQLCorpus exports, for the DSQL lint gate (#740), every statement the
// Aurora DSQL engine's writer sends, in the order it sends them: a first
// flush of an unpublished schema (the record, the entries, the clock, the
// publication and its plain mark) and an init of a published one (two
// export snapshots, the publication superseding its parent, a handoff).
func TestDSQLCorpus(t *testing.T) {
	ctx := context.Background()
	var rec corpus.Recorder
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(rec.MatchSQL)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	st := &manifestv2.FSStore{Dir: t.TempDir()}
	cache, err := manifestv2.NewCache(st, 4)
	require.NoError(t, err)
	w, err := New(Config{
		DB: db, Engine: forma.EngineAuroraDSQL, EntityMain: "entity_main", ChangeLog: "change_log",
		Layout: manifestv2.Layout{Prefix: "data"}, Store: st, Cache: cache,
	})
	require.NoError(t, err)
	rows := []uuid.UUID{uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7())}

	expectFlush(mock, rows)
	rep, err := w.Flush(ctx, schemaID, 1, corpusStager{})
	require.NoError(t, err)
	require.Equal(t, 2, rep.Marked)
	flush := rec.Take(t,
		"internal/publication/record.go:Read",
		"internal/cdcv2/marks.go:Writer.selectEntries",
		"internal/cdcv2/writer.go:Writer.now",
		"internal/publication/publish.go:Publish",
		"internal/publication/publish.go:Publish",
		"internal/cdcv2/marks.go:Writer.mark",
	)

	lineage := uuid.New()
	expectInit(mock, rows[0], lineage)
	irep, err := w.Init(ctx, schemaID, 1, corpusStager{})
	require.NoError(t, err)
	require.Equal(t, 1, irep.Marked)
	initStatements := rec.Take(t,
		"internal/publication/record.go:Read",
		"internal/cdcv2/writer.go:Writer.now",
		"internal/cdcv2/init.go:Writer.readVersions",
		"internal/cdcv2/init.go:Writer.readVersions",
		"internal/publication/publish.go:Publish",
		"internal/publication/displaced.go:displace",
		"internal/publication/publish.go:supersede",
		"internal/publication/publish.go:Publish",
		"internal/publication/lifecycle.go:HoldLineage",
		"internal/cdcv2/writer.go:Writer.now",
		"internal/cdcv2/marks.go:Writer.mark",
	)
	require.NoError(t, mock.ExpectationsWereMet(), "the writer sent other statements than recorded")
	corpus.Export(t, corpus.Unit{Name: "internal-cdcv2", Engine: forma.EngineAuroraDSQL, Statements: append(flush, initStatements...)})
}

// expectFlush answers a flush of rows on an unpublished schema 100, one
// generation.
func expectFlush(mock sqlmock.Sqlmock, rows []uuid.UUID) {
	unpublished := recordRow(publication.Unpublished, 0, uuid.Nil)
	mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnRows(unpublished)
	entries := sqlmock.NewRows([]string{"row_id", "changed_at"})
	for _, id := range rows {
		entries.AddRow(id.String(), 1000)
	}
	mock.ExpectQuery("").WithArgs(anyArgs(2)...).WillReturnRows(entries)
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"now"}).AddRow(5000))
	mock.ExpectBegin()
	mock.ExpectQuery("").WithArgs(anyArgs(10)...).WillReturnRows(sqlmock.NewRows([]string{"generation", "parent_generation", "updated_at"}).AddRow(1, 0, 5001))
	mock.ExpectExec("").WithArgs(anyArgs(10)...).WillReturnResult(sqlmock.NewResult(0, 1))
	marked := sqlmock.NewRows([]string{"row_id"})
	for _, id := range rows {
		marked.AddRow(id.String())
	}
	mock.ExpectQuery("").WithArgs(anyArgs(2 + 2*len(rows))...).WillReturnRows(marked)
	mock.ExpectCommit()
}

// expectInit answers an init of schema 100 at generation 1 of lineage that
// exports row in one snapshot and hands it off.
func expectInit(mock sqlmock.Sqlmock, row, lineage uuid.UUID) {
	mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnRows(recordRow(publication.Published, 1, lineage))
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"now"}).AddRow(6000))
	mock.ExpectBegin()
	mock.ExpectQuery("").WithArgs(anyArgs(3)...).WillReturnRows(sqlmock.NewRows([]string{"ltbase_row_id", "changed_at"}).AddRow(row.String(), 1000))
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectQuery("").WithArgs(anyArgs(3)...).WillReturnRows(sqlmock.NewRows([]string{"ltbase_row_id", "changed_at"}))
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectQuery("").WithArgs(anyArgs(10)...).WillReturnRows(sqlmock.NewRows([]string{"generation", "parent_generation", "updated_at"}).AddRow(2, 1, 6001))
	mock.ExpectQuery("").WithArgs(anyArgs(2)...).WillReturnRows(sqlmock.NewRows([]string{"epoch", "manifest_key", "manifest_sha256", "cas_token"}))
	mock.ExpectExec("").WithArgs(anyArgs(8)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("").WithArgs(anyArgs(10)...).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectQuery("").WithArgs(anyArgs(3)...).WillReturnRows(sqlmock.NewRows([]string{"generation"}).AddRow(2))
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"now"}).AddRow(6002))
	mock.ExpectQuery("").WithArgs(anyArgs(4)...).WillReturnRows(sqlmock.NewRows([]string{"row_id"}).AddRow(row.String()))
	mock.ExpectCommit()
}

func recordRow(state publication.State, generation int64, lineage uuid.UUID) *sqlmock.Rows {
	var key, digest string
	var lin any
	if state == publication.Published {
		key, digest, lin = manifestv2.Layout{Prefix: "data"}.ManifestKey(schemaID, 1, generation, uuid.New()), manifestv2.Digest([]byte("m")), lineage.String()
	}
	return sqlmock.NewRows([]string{"schema_id", "epoch", "state", "generation", "parent_generation", "manifest_key", "manifest_sha256", "lineage", "cas_token", "updated_at", "gc_horizon", "now"}).
		AddRow(schemaID, 1, string(state), generation, 0, key, digest, lin, uuid.NewString(), 1, 0, 2)
}

func anyArgs(n int) []driver.Value {
	args := make([]driver.Value, n)
	for i := range args {
		args[i] = sqlmock.AnyArg()
	}
	return args
}

// corpusStager stages valid files and sends no SQL.
type corpusStager struct{}

func (corpusStager) StageDelta(_ context.Context, req DeltaRequest) ([]manifestv2.File, error) {
	key, err := req.Layout.NewParquetKey(req.SchemaID, req.Epoch, manifestv2.TierDelta)
	if err != nil {
		return nil, err
	}
	return []manifestv2.File{corpusFile(manifestv2.TierDelta, key)}, nil
}

func (corpusStager) ExportBase(_ context.Context, _ *sql.Tx, req BaseRequest) (manifestv2.File, error) {
	return corpusFile(manifestv2.TierBase, req.Key), nil
}

func corpusFile(tier manifestv2.Tier, key string) manifestv2.File {
	id := uuid.Must(uuid.NewV7())
	return manifestv2.File{
		Tier: tier, Key: key, SizeBytes: 1, SHA256: manifestv2.Digest([]byte(key)), RowCount: 1,
		RowIDMin: id, RowIDMax: id, Columns: map[string]string{"ltbase_row_id": "UUID"},
	}
}
