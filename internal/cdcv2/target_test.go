package cdcv2

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/testdb"
)

// The integration tests run on the integration PostgreSQL by default.
// FORMA_DSQL_DSN names an Aurora DSQL cluster instead, and with
// FORMA_DSQL_TARGET=ministack MiniStack's strict SQL proxy, whose backend is
// PostgreSQL at DSQL's REPEATABLE READ (docs/dsql-ministack.md); there the
// writer runs as the Aurora DSQL engine, with the plain mark and generations
// of at most DSQLGenerationLimit entries.

const schemaID int16 = 100

// fixture is one test's tables, object store and engine on its target.
type fixture struct {
	db         *sql.DB
	engine     forma.DatabaseEngine
	entityMain string
	changeLog  string
	tables     publication.Tables
	store      *manifestv2.FSStore
	layout     manifestv2.Layout
	// cluster says the target is a real Aurora DSQL cluster, where nothing
	// waits on a row lock.
	cluster bool
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	db, engine, cluster := openTarget(t)
	entityMain := fmt.Sprintf("cdcv2_test_%d", time.Now().UnixNano())
	f := &fixture{
		db: db, engine: engine, entityMain: entityMain, changeLog: entityMain + "_change_log",
		tables: publication.TablesFor(entityMain), store: &manifestv2.FSStore{Dir: t.TempDir()},
		layout: manifestv2.Layout{Prefix: "data"}, cluster: cluster,
	}
	ddl := append(publication.Statements(engine, entityMain),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			schema_id  SMALLINT NOT NULL,
			row_id     UUID     NOT NULL,
			flushed_at BIGINT   NOT NULL DEFAULT 0,
			changed_at BIGINT   NOT NULL,
			deleted_at BIGINT,
			PRIMARY KEY (schema_id, row_id, flushed_at)
		)`, f.changeLog),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			ltbase_schema_id  SMALLINT NOT NULL,
			ltbase_row_id     UUID     NOT NULL,
			ltbase_updated_at BIGINT   NOT NULL,
			ltbase_deleted_at BIGINT,
			PRIMARY KEY (ltbase_schema_id, ltbase_row_id)
		)`, f.entityMain))
	for _, stmt := range ddl {
		_, err := db.Exec(stmt)
		require.NoError(t, err, stmt)
	}
	t.Cleanup(func() {
		for _, table := range []string{f.tables.Record, f.tables.Generation, f.changeLog, f.entityMain} {
			_, _ = db.Exec("DROP TABLE IF EXISTS " + table)
		}
	})
	return f
}

// openTarget opens what FORMA_DSQL_DSN and FORMA_DSQL_TARGET name, or else the
// integration Postgres (skipping when it is absent locally), with its engine
// and whether it is a real cluster.
func openTarget(t *testing.T) (*sql.DB, forma.DatabaseEngine, bool) {
	t.Helper()
	dsn, kind := testdb.ResolveDSQL(t)
	if kind == testdb.DSQLCluster || kind == testdb.DSQLMiniStack {
		cfg, err := pgx.ParseConfig(dsn)
		require.NoError(t, err, "parse FORMA_DSQL_DSN")
		db := stdlib.OpenDB(*cfg)
		t.Cleanup(func() { _ = db.Close() })
		require.NoError(t, db.Ping(), "connect to FORMA_DSQL_DSN")
		return db, forma.EngineAuroraDSQL, kind == testdb.DSQLCluster
	}
	testdb.Connect(t, context.Background()) // skips when Postgres is absent locally
	db, err := sql.Open("pgx", testdb.ResolveDSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db, forma.EnginePostgreSQL, false
}

// writer answers a writer over the fixture, adjusted by mods.
func (f *fixture) writer(t *testing.T, mods ...func(*Config)) *Writer {
	t.Helper()
	cache, err := manifestv2.NewCache(f.store, 64)
	require.NoError(t, err)
	cfg := Config{
		DB: f.db, Engine: f.engine, EntityMain: f.entityMain, ChangeLog: f.changeLog,
		Layout: f.layout, Store: f.store, Cache: cache, StagingBudget: time.Minute,
	}
	for _, mod := range mods {
		mod(&cfg)
	}
	w, err := New(cfg)
	require.NoError(t, err)
	return w
}

// register writes the schema's record, as registration does.
func (f *fixture) register(t *testing.T) publication.Record {
	t.Helper()
	f.inTx(t, func(tx *sql.Tx) error {
		_, err := publication.Initialize(context.Background(), publication.SQLTx(tx), f.tables, schemaID)
		return err
	})
	return f.record(t)
}

func (f *fixture) record(t *testing.T) publication.Record {
	t.Helper()
	s, err := publication.Read(context.Background(), publication.DB(f.db), f.tables, schemaID)
	require.NoError(t, err)
	return s.Record
}

func (f *fixture) inTx(t *testing.T, fn func(*sql.Tx) error) {
	t.Helper()
	tx, err := f.db.Begin()
	require.NoError(t, err)
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
}

// put writes row at version, live or deleted, as the write path does: the
// entity_main row and the row's unflushed change_log entry in one
// transaction.
func (f *fixture) put(t *testing.T, row uuid.UUID, version int64, deleted bool) {
	t.Helper()
	var deletedAt *int64
	if deleted {
		deletedAt = &version
	}
	f.inTx(t, func(tx *sql.Tx) error {
		if _, err := tx.Exec(fmt.Sprintf(`INSERT INTO %s (ltbase_schema_id, ltbase_row_id, ltbase_updated_at, ltbase_deleted_at)
			VALUES ($1, $2, $3, $4) ON CONFLICT (ltbase_schema_id, ltbase_row_id)
			DO UPDATE SET ltbase_updated_at = EXCLUDED.ltbase_updated_at, ltbase_deleted_at = EXCLUDED.ltbase_deleted_at`, f.entityMain),
			schemaID, row, version, deletedAt); err != nil {
			return err
		}
		_, err := tx.Exec(fmt.Sprintf(`INSERT INTO %s (schema_id, row_id, changed_at, deleted_at) VALUES ($1, $2, $3, $4)
			ON CONFLICT (schema_id, row_id, flushed_at) DO UPDATE SET changed_at = EXCLUDED.changed_at, deleted_at = EXCLUDED.deleted_at`, f.changeLog),
			schemaID, row, version, deletedAt)
		return err
	})
}

// insert writes n new live rows at version 1000 in statements of 500 rows,
// and answers them.
func (f *fixture) insert(t *testing.T, n int) []uuid.UUID {
	t.Helper()
	rows := make([]uuid.UUID, n)
	for i := range rows {
		rows[i] = uuid.Must(uuid.NewV7())
	}
	for start := 0; start < n; start += 500 {
		chunk := rows[start:min(start+500, n)]
		values := make([]string, len(chunk))
		args := []any{schemaID}
		for i, id := range chunk {
			values[i] = fmt.Sprintf("($1, $%d, 1000)", len(args)+1)
			args = append(args, id)
		}
		f.inTx(t, func(tx *sql.Tx) error {
			list := strings.Join(values, ",")
			if _, err := tx.Exec(fmt.Sprintf("INSERT INTO %s (ltbase_schema_id, ltbase_row_id, ltbase_updated_at) VALUES %s", f.entityMain, list), args...); err != nil {
				return err
			}
			_, err := tx.Exec(fmt.Sprintf("INSERT INTO %s (schema_id, row_id, changed_at) VALUES %s", f.changeLog, list), args...)
			return err
		})
	}
	return rows
}

// dirty answers the schema's unflushed entries.
func (f *fixture) dirty(t *testing.T) map[uuid.UUID]int64 {
	t.Helper()
	rows, err := f.db.Query(fmt.Sprintf("SELECT row_id, changed_at FROM %s WHERE schema_id = $1 AND flushed_at = 0", f.changeLog), schemaID)
	require.NoError(t, err)
	defer rows.Close()
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

func (f *fixture) count(t *testing.T, query string) int {
	t.Helper()
	var n int
	require.NoError(t, f.db.QueryRow(query, schemaID).Scan(&n))
	return n
}

// entries counts the schema's change_log entries, flushed or not: V2 never
// deletes one.
func (f *fixture) entries(t *testing.T) int {
	t.Helper()
	return f.count(t, fmt.Sprintf("SELECT count(*) FROM %s WHERE schema_id = $1", f.changeLog))
}
