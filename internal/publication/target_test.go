package publication

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/testdb"
)

// The tests run on the integration PostgreSQL by default, at its default READ
// COMMITTED. FORMA_DSQL_DSN names an Aurora DSQL cluster instead, and with
// FORMA_DSQL_TARGET=ministack MiniStack's strict SQL proxy, whose backend is
// PostgreSQL at DSQL's REPEATABLE READ (docs/dsql-ministack.md). A test that
// needs one transaction to wait on another's row lock skips on a cluster,
// where nothing waits. A pass on any of them is not Aurora DSQL validation of
// the optimistic-concurrency behaviour, which #817 tracks.

// fixture is one test's publication tables and change_log on its target.
type fixture struct {
	db        *sql.DB
	t         Tables
	changeLog string
	// cluster says the target is a real Aurora DSQL cluster.
	cluster bool
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	db, engine, cluster := openTarget(t)
	entityMain := fmt.Sprintf("pub_test_%d", time.Now().UnixNano())
	f := &fixture{db: db, t: TablesFor(entityMain), changeLog: entityMain + "_change_log", cluster: cluster}
	ddl := append(Statements(engine, entityMain), fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		schema_id  SMALLINT NOT NULL,
		row_id     UUID     NOT NULL,
		flushed_at BIGINT   NOT NULL DEFAULT 0,
		changed_at BIGINT   NOT NULL,
		deleted_at BIGINT,
		PRIMARY KEY (schema_id, row_id, flushed_at)
	)`, f.changeLog))
	for _, stmt := range ddl {
		_, err := db.Exec(stmt)
		require.NoError(t, err, stmt)
	}
	t.Cleanup(func() {
		for _, table := range []string{f.t.Record, f.t.Generation, f.t.Epoch, f.t.Displaced, f.changeLog} {
			_, _ = db.Exec("DROP TABLE IF EXISTS " + table)
		}
	})
	return f
}

// openTarget opens what FORMA_DSQL_DSN and FORMA_DSQL_TARGET name, or else the
// integration Postgres (skipping when it is absent locally), with the engine
// whose DDL it takes and whether it is a real cluster.
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

// initialized writes schemaID's record and answers it.
func (f *fixture) initialized(t *testing.T, schemaID int16) Record {
	t.Helper()
	ok, err := f.initialize(t, schemaID)
	require.NoError(t, err)
	require.True(t, ok)
	return f.read(t, schemaID)
}

// initialize runs Initialize in a registration transaction of its own.
func (f *fixture) initialize(t *testing.T, schemaID int16) (bool, error) {
	t.Helper()
	var ok bool
	err := f.inTx(context.Background(), func(tx Tx) error {
		var err error
		ok, err = Initialize(context.Background(), tx, f.t, schemaID)
		return err
	})
	return ok, err
}

// inTx runs fn in a transaction it commits when fn succeeds and rolls back
// otherwise.
func (f *fixture) inTx(ctx context.Context, fn func(Tx) error) error {
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(SQLTx(tx)); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (f *fixture) read(t *testing.T, schemaID int16) Record {
	t.Helper()
	s, err := Read(context.Background(), DB(f.db), f.t, schemaID)
	require.NoError(t, err)
	return s.Record
}

func (f *fixture) serverMillis(t *testing.T) int64 {
	t.Helper()
	var ms int64
	require.NoError(t, f.db.QueryRow("SELECT "+sqlgen.ServerTimeMillisSQL).Scan(&ms))
	return ms
}

// next stages a generation of kind on cur: a fresh token, a manifest key named
// after it, and a staging time of now with a minute's budget.
func (f *fixture) next(t *testing.T, cur Record, kind Kind) Next {
	t.Helper()
	token := uuid.New()
	lineage := cur.Lineage
	if kind == KindInit || cur.State != Published {
		lineage = uuid.New()
	}
	return Next{
		Kind:           kind,
		Token:          token,
		ManifestKey:    fmt.Sprintf("data/%d/_manifests/e%d/g%020d-%s.json", cur.SchemaID, cur.Epoch, cur.Generation+1, token),
		ManifestSHA256: fmt.Sprintf("%064x", cur.Generation+1),
		Lineage:        lineage,
		StagedAt:       f.serverMillis(t),
		StagingBudget:  time.Minute,
	}
}

// publish commits next on cur in one transaction, marking rows at their
// listed versions in it after the compare-and-set.
func (f *fixture) publish(ctx context.Context, cur Record, next Next, rows map[uuid.UUID]int64) (Record, error) {
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return Record{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	out, err := Publish(ctx, SQLTx(tx), f.t, cur, next)
	if err != nil {
		return Record{}, err
	}
	if err := f.mark(ctx, tx, cur.SchemaID, rows); err != nil {
		return Record{}, err
	}
	if err := tx.Commit(); err != nil {
		return Record{}, fmt.Errorf("commit: %w", err)
	}
	return out, nil
}

// mark marks each row's unflushed entry flushed when it is still at its listed
// version, as the flush mark does.
func (f *fixture) mark(ctx context.Context, tx *sql.Tx, schemaID int16, rows map[uuid.UUID]int64) error {
	for id, version := range rows {
		_, err := tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET flushed_at = %s
			WHERE schema_id = $1 AND row_id = $2 AND changed_at = $3 AND flushed_at = 0`, f.changeLog, sqlgen.ServerTimeMillisSQL),
			schemaID, id, version)
		if err != nil {
			return fmt.Errorf("mark %s: %w", id, err)
		}
	}
	return nil
}

// change writes row again at version, live or deleted: it updates the row's
// unflushed entry, or adds one when every entry of the row is flushed.
func (f *fixture) change(t *testing.T, schemaID int16, row uuid.UUID, version int64, deleted bool) {
	t.Helper()
	var deletedAt *int64
	if deleted {
		deletedAt = &version
	}
	_, err := f.db.Exec(fmt.Sprintf(`INSERT INTO %s (schema_id, row_id, changed_at, deleted_at) VALUES ($1, $2, $3, $4)
		ON CONFLICT (schema_id, row_id, flushed_at) DO UPDATE SET changed_at = EXCLUDED.changed_at, deleted_at = EXCLUDED.deleted_at`, f.changeLog),
		schemaID, row, version, deletedAt)
	require.NoError(t, err)
}

// reset drops cur's schema in a transaction of its own.
func (f *fixture) reset(t *testing.T, cur Record) Record {
	t.Helper()
	var out Record
	require.NoError(t, f.inTx(context.Background(), func(tx Tx) error {
		var err error
		out, err = Reset(context.Background(), tx, f.t, cur)
		return err
	}))
	return out
}

// activate registers a dropped schema id again, checking the test's
// change_log for the dropped schema's rows.
func (f *fixture) activate(t *testing.T, cur Record) Record {
	t.Helper()
	var out Record
	require.NoError(t, f.inTx(context.Background(), func(tx Tx) error {
		var err error
		out, err = Activate(context.Background(), tx, f.t, cur, []Residue{{Table: f.changeLog}})
		return err
	}))
	require.Equal(t, out, f.read(t, cur.SchemaID))
	return out
}

// forget collects generation as a collector would with a zero grace: it
// retires the generation, then forgets its history row.
func (f *fixture) forget(t *testing.T, generation int64) {
	t.Helper()
	ctx := context.Background()
	ok, err := Retire(ctx, DB(f.db), f.t, 100, generation)
	require.NoError(t, err)
	require.True(t, ok, "generation %d is superseded and not yet retired", generation)
	time.Sleep(2 * StampPrecision) // a zero grace still waits out the stamp's precision (GraceMillis)
	ok, err = Forget(ctx, DB(f.db), f.t, 100, generation, 0)
	require.NoError(t, err)
	require.True(t, ok, "generation %d is retired, so a zero grace expires it", generation)
}

// write inserts n unflushed entries and answers them with their versions.
func (f *fixture) write(t *testing.T, schemaID int16, n int) map[uuid.UUID]int64 {
	t.Helper()
	rows := make(map[uuid.UUID]int64, n)
	for i := range n {
		id, version := uuid.New(), int64(1000+i)
		_, err := f.db.Exec(fmt.Sprintf("INSERT INTO %s (schema_id, row_id, changed_at) VALUES ($1, $2, $3)", f.changeLog), schemaID, id, version)
		require.NoError(t, err)
		rows[id] = version
	}
	return rows
}

// view is what one reader snapshot sees: the record and the dirty count.
type view struct {
	State      State
	Generation int64
	Dirty      int
}

// snapshot opens a REPEATABLE READ READ ONLY transaction, the reader's, and
// reads the record first and the dirty set after it.
func (f *fixture) snapshot(t *testing.T, schemaID int16) (*sql.Tx, func() view) {
	t.Helper()
	tx, err := f.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	return tx, func() view {
		s, err := Read(context.Background(), SQLTx(tx), f.t, schemaID)
		require.NoError(t, err)
		v := view{State: s.State, Generation: s.Generation}
		require.NoError(t, tx.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s WHERE schema_id = $1 AND flushed_at = 0", f.changeLog), schemaID).Scan(&v.Dirty))
		return v
	}
}
