package cdc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/schemalease"
	"github.com/lychee-technology/forma/internal/testdb"
)

// The wired-path tests run cdc-flush and cdc-init as the tools do, from the
// run's flush pass (runFlushPass) and init pass (processInitSchemas) through
// the real DuckDB export, the V2 writer (internal/cdcv2) and the publication
// record, with a local directory standing in for the bucket
// (manifestv2.FSStore shares S3Store's write-once contract). They run on the
// integration PostgreSQL, and on MiniStack's strict proxy as the aurora-dsql
// engine when FORMA_DSQL_TARGET names it. internal/cdcv2's own failpoint and
// race suites prove the writer; these recheck its guarantees on the wired
// path.

const wiredSchema int16 = 7

var errWiredFault = errors.New("injected wired-path fault")

type wiredFixture struct {
	pool   *pgxpool.Pool
	db     *sql.DB
	engine forma.DatabaseEngine
	cfg    CDCConfig
	tables publication.Tables
	store  *manifestv2.FSStore
	duck   *DuckExporter
	logger *zap.Logger
}

func newWiredFixture(t *testing.T) *wiredFixture {
	t.Helper()
	ctx := context.Background()
	pool, kind := testdb.OpenDSQLPool(t, ctx)
	storage := testdb.CreateDSQLStorageTables(t, ctx, pool, "cdc_wired")
	engine := forma.EnginePostgreSQL
	if kind != testdb.DSQLEmulated {
		engine = forma.EngineAuroraDSQL
	}
	f := &wiredFixture{
		pool: pool, db: stdlib.OpenDBFromPool(pool), engine: engine,
		tables: publication.TablesFor(storage.EntityMain), store: &manifestv2.FSStore{Dir: t.TempDir()},
		logger: zaptest.NewLogger(t),
	}
	t.Cleanup(func() { _ = f.db.Close() })
	ddl := append(publication.Statements(engine, storage.EntityMain), schemalease.StatementFor(storage.EntityMain))
	t.Cleanup(func() {
		for _, table := range []string{f.tables.Record, f.tables.Generation, schemalease.Table(storage.EntityMain)} {
			_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
		}
	})
	for _, stmt := range ddl {
		_, err := pool.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	f.cfg = CDCConfig{
		ChangeLogTable: storage.ChangeLog, EntityMainTable: storage.EntityMain, EAVDataTable: storage.EAVData,
		Engine: engine, S3Prefix: "data", MinRecords: 1, MaxAgeMs: 1,
	}.WithDefaults()
	duck, err := NewDuckExporter(ctx, f.cfg, "", "", "", f.logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = duck.DB.Close() })
	f.duck = duck
	return f
}

// register writes the schema's record in state, as registration or the V2
// cutover does.
func (f *wiredFixture) register(t *testing.T, state publication.State) {
	t.Helper()
	f.inTx(t, func(tx pgx.Tx) error {
		var err error
		if state == publication.AwaitingInit {
			_, err = publication.InitializeAwaitingInit(context.Background(), publication.PgxTx(tx), f.tables, wiredSchema)
		} else {
			_, err = publication.Initialize(context.Background(), publication.PgxTx(tx), f.tables, wiredSchema)
		}
		return err
	})
}

// drop moves the schema's record to its next epoch, DROPPED, as a drop does.
func (f *wiredFixture) drop(ctx context.Context) error {
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin the drop: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	snap, err := publication.Read(ctx, publication.PgxTx(tx), f.tables, wiredSchema)
	if err != nil {
		return fmt.Errorf("read the record to drop: %w", err)
	}
	if _, err := publication.Reset(ctx, publication.PgxTx(tx), f.tables, snap.Record); err != nil {
		return fmt.Errorf("reset the record: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit the drop: %w", err)
	}
	return nil
}

func (f *wiredFixture) inTx(t *testing.T, fn func(pgx.Tx) error) {
	t.Helper()
	ctx := context.Background()
	tx, err := f.pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	require.NoError(t, fn(tx))
	require.NoError(t, tx.Commit(ctx))
}

// record answers the schema's record; ok is false when it has none.
func (f *wiredFixture) record(t *testing.T) (publication.Record, bool) {
	t.Helper()
	snap, err := publication.Read(context.Background(), publication.DB(f.db), f.tables, wiredSchema)
	if errors.Is(err, publication.ErrNoRecord) {
		return publication.Record{}, false
	}
	require.NoError(t, err)
	return snap.Record, true
}

// write is one write of the data plane: the row's entity_main state and its
// one unflushed change_log entry at version, in one transaction. A deleted
// row keeps its entity_main row with ltbase_deleted_at set.
func (f *wiredFixture) write(t *testing.T, id uuid.UUID, version int64, name string, deleted bool) {
	t.Helper()
	main, cl := sanitizeIdentifier(f.cfg.EntityMainTable), sanitizeIdentifier(f.cfg.ChangeLogTable)
	var deletedAt any
	if deleted {
		deletedAt = version
	}
	f.inTx(t, func(tx pgx.Tx) error {
		ctx := context.Background()
		for _, s := range []struct {
			sql  string
			args []any
		}{
			{"DELETE FROM " + main + " WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2", []any{wiredSchema, id}},
			{"INSERT INTO " + main + " (ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, text_01) VALUES ($1, $2, $3, $3, $4, $5)",
				[]any{wiredSchema, id, version, deletedAt, name}},
			{"DELETE FROM " + cl + " WHERE schema_id = $1 AND row_id = $2 AND flushed_at = 0", []any{wiredSchema, id}},
			{"INSERT INTO " + cl + " (schema_id, row_id, flushed_at, changed_at, deleted_at) VALUES ($1, $2, 0, $3, $4)",
				[]any{wiredSchema, id, version, deletedAt}},
		} {
			if _, err := tx.Exec(ctx, s.sql, s.args...); err != nil {
				return fmt.Errorf("%s: %w", s.sql, err)
			}
		}
		return nil
	})
}

// seed writes n live rows at versions base+1.. and answers their ids.
func (f *wiredFixture) seed(t *testing.T, n int, base int64) []uuid.UUID {
	t.Helper()
	ids := make([]uuid.UUID, n)
	for i := range ids {
		ids[i] = uuid.New()
		f.write(t, ids[i], base+int64(i)+1, fmt.Sprintf("row-%d", i), false)
	}
	return ids
}

// dirty answers the schema's unflushed entries and their versions.
func (f *wiredFixture) dirty(t *testing.T) map[uuid.UUID]int64 {
	t.Helper()
	rows, err := f.db.Query("SELECT row_id, changed_at FROM "+sanitizeIdentifier(f.cfg.ChangeLogTable)+" WHERE schema_id = $1 AND flushed_at = 0", wiredSchema)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	out := map[uuid.UUID]int64{}
	for rows.Next() {
		var id uuid.UUID
		var v int64
		require.NoError(t, rows.Scan(&id, &v))
		out[id] = v
	}
	require.NoError(t, rows.Err())
	return out
}

// entries counts the schema's change_log entries, flushed or not: V2 never
// deletes one.
func (f *wiredFixture) entries(t *testing.T) int {
	t.Helper()
	var n int
	require.NoError(t, f.db.QueryRow("SELECT count(*) FROM "+sanitizeIdentifier(f.cfg.ChangeLogTable)+" WHERE schema_id = $1", wiredSchema).Scan(&n))
	return n
}

// manifest answers the record's generation, verified as a reader verifies
// it (manifestv2.Cache).
func (f *wiredFixture) manifest(t *testing.T, rec publication.Record) *manifestv2.Manifest {
	t.Helper()
	require.Equal(t, publication.Published, rec.State)
	cache, err := manifestv2.NewCache(f.store, 8)
	require.NoError(t, err)
	ref, err := manifestv2.RecordRef(rec)
	require.NoError(t, err)
	m, err := cache.Get(context.Background(), ref)
	require.NoError(t, err)
	return m
}

// objects answers the number of parquet and manifest objects in the store.
func (f *wiredFixture) objects(t *testing.T) (parquet, manifests int) {
	t.Helper()
	err := filepath.WalkDir(f.store.Dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || strings.HasSuffix(path, ".tmp") {
			return err
		}
		switch {
		case strings.HasSuffix(path, ".parquet"):
			parquet++
		case strings.HasSuffix(path, ".json"):
			manifests++
		}
		return nil
	})
	require.NoError(t, err)
	return parquet, manifests
}

// fileRow is one row of a published parquet object.
type fileRow struct {
	version   int64
	deletedAt int64
	name      sql.NullString
}

// fileRows reads the parquet object key from the store.
func (f *wiredFixture) fileRows(t *testing.T, key string) map[uuid.UUID]fileRow {
	t.Helper()
	raw, err := f.store.Get(context.Background(), key)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "object.parquet")
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	rows, err := f.duck.DB.Query(fmt.Sprintf("SELECT row_id, changed_at, deleted_at, name FROM read_parquet('%s')", path))
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	out := map[uuid.UUID]fileRow{}
	for rows.Next() {
		var id uuid.UUID
		var r fileRow
		require.NoError(t, rows.Scan(&id, &r.version, &r.deletedAt, &r.name))
		out[id] = r
	}
	require.NoError(t, rows.Err())
	return out
}

// generationRows reads every row the generation's files hold, keyed by row
// and version.
func (f *wiredFixture) generationRows(t *testing.T, m *manifestv2.Manifest) map[uuid.UUID][]int64 {
	t.Helper()
	out := map[uuid.UUID][]int64{}
	for _, file := range m.Files {
		for id, r := range f.fileRows(t, file.Key) {
			out[id] = append(out[id], r.version)
		}
	}
	return out
}

// flush runs one cdc-flush pass as the tools do, with cfg adjusted by mods.
func (f *wiredFixture) flush(t *testing.T, registry forma.SchemaRegistry, dryRun bool, mods ...func(*CDCConfig)) (FlushReport, error) {
	t.Helper()
	cfg := f.cfg
	for _, mod := range mods {
		mod(&cfg)
	}
	if registry == nil {
		registry = stubSchemaRegistry{cache: testAttrCache()}
	}
	return runFlushPass(context.Background(), flushContextParams{
		cfg: cfg, db: f.db, duck: f.duck, store: f.store, dryRun: dryRun, logger: f.logger, schemaRegistry: registry,
	})
}

// runInit runs one cdc-init pass of the schema as RunInit does, with cfg
// adjusted by mods.
func (f *wiredFixture) runInit(t *testing.T, registry forma.SchemaRegistry, dryRun bool, mods ...func(*CDCConfig)) (InitSummary, error) {
	t.Helper()
	cfg := f.cfg
	for _, mod := range mods {
		mod(&cfg)
	}
	if registry == nil {
		registry = stubSchemaRegistry{cache: testAttrCache()}
	}
	locker, err := NewSchemaLocker(f.db, cfg.Engine, cfg.EntityMainTable)
	require.NoError(t, err)
	runCtx := &initRunContext{
		cfg: cfg, db: f.db, duck: f.duck, store: f.store, schemaRegistry: registry,
		logger: f.logger, dryRun: dryRun, locker: locker,
	}
	return processInitSchemas(context.Background(), runCtx, []int64{int64(wiredSchema)})
}

// failAt answers a PublishFailpoint that fails the first n hits of point
// with err, and passes every other hit.
func failAt(point string, n int32, err error) func(*CDCConfig) {
	var hits atomic.Int32
	return func(c *CDCConfig) {
		c.PublishFailpoint = func(_ context.Context, p string) error {
			if p != point || hits.Add(1) > n {
				return nil
			}
			return err
		}
	}
}

// runAt answers a PublishFailpoint that runs fn at the first hit of point.
func runAt(point string, fn func(context.Context) error) func(*CDCConfig) {
	var fired atomic.Bool
	return func(c *CDCConfig) {
		c.PublishFailpoint = func(ctx context.Context, p string) error {
			if p != point || !fired.CompareAndSwap(false, true) {
				return nil
			}
			return fn(ctx)
		}
	}
}
