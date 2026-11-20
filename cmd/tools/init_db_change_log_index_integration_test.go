package main

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sqlutil"
	"github.com/lychee-technology/forma/internal/testdb"
)

// tracedQuery is one statement and its arguments as pgx sent them.
type tracedQuery struct {
	sql  string
	args []any
}

// queryTrace is a pgx.QueryTracer keeping every statement with its arguments,
// so a test can EXPLAIN exactly what the code under test sent.
type queryTrace struct {
	mu      sync.Mutex
	queries []tracedQuery
}

func (q *queryTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queries = append(q.queries, tracedQuery{sql: data.SQL, args: data.Args})
	return ctx
}

func (q *queryTrace) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// take answers the traced statements that contain substr and forgets all.
func (q *queryTrace) take(substr string) []tracedQuery {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []tracedQuery
	for _, tq := range q.queries {
		if strings.Contains(tq.sql, substr) {
			out = append(out, tq)
		}
	}
	q.queries = nil
	return out
}

func tracedPool(t *testing.T, trace *queryTrace) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(testdb.ResolveDSN())
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cfg.ConnConfig.Tracer = trace
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open traced pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// dirtySetFixture is what seedDirtySetFixture wrote for schema 100.
type dirtySetFixture struct {
	strict []uuid.UUID // an unflushed entry
}

// seedDirtySetFixture writes a change_log whose schema 100 has a long flushed
// history, so that a read walking every entry costs what it would in
// production, unflushed rows, and recently flushed rows the strict set must
// leave out. Schema 101's unflushed rows must stay out of schema 100's set.
func seedDirtySetFixture(t *testing.T, pool *pgxpool.Pool, changeLog string) dirtySetFixture {
	t.Helper()
	ctx := context.Background()
	cl := quoteIdentifier(changeLog)
	const history = int64(1_600_000_000_000)
	f := dirtySetFixture{}
	const recent = int64(1_700_000_000_000)
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed change log: %v", err)
		}
	}
	exec(fmt.Sprintf(`INSERT INTO %s (schema_id, row_id, flushed_at, changed_at)
		SELECT 100, gen_random_uuid(), $1::bigint + g, $1::bigint + g FROM generate_series(1, 20000) g`, cl), history)
	entry := func(schema int16, row uuid.UUID, flushedAt int64) {
		exec(fmt.Sprintf(`INSERT INTO %s (schema_id, row_id, flushed_at, changed_at) VALUES ($1, $2, $3, $4)`, cl),
			schema, row, flushedAt, history)
	}
	for i := 0; i < 3; i++ { // unflushed, one of them over a flushed version
		row := uuid.New()
		entry(100, row, 0)
		f.strict = append(f.strict, row)
		if i == 0 {
			entry(100, row, history-1)
		}
	}
	both := uuid.New() // an unflushed entry and a recent mark: one ID
	entry(100, both, 0)
	entry(100, both, recent+1)
	f.strict = append(f.strict, both)
	entry(100, uuid.New(), recent) // flushed just now: clean
	entry(100, uuid.New(), recent+5)
	entry(101, uuid.New(), 0)
	entry(101, uuid.New(), recent+1)
	exec("VACUUM ANALYZE " + cl)
	return f
}

func sortedIDs(ids []uuid.UUID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	slices.Sort(out)
	return out
}

// referenceDirtySet reads the strict dirty set (flushed_at = 0) with a
// predicate written out here, independently of the engine's
// strictDirtySetQuery.
func referenceDirtySet(t *testing.T, pool *pgxpool.Pool, changeLog string, schemaID int16) []uuid.UUID {
	t.Helper()
	query := fmt.Sprintf(`SELECT DISTINCT row_id FROM %s WHERE schema_id = $1 AND flushed_at = 0`, quoteIdentifier(changeLog))
	rows, err := pool.Query(context.Background(), query, schemaID)
	if err != nil {
		t.Fatalf("read the reference dirty set: %v", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	if err != nil {
		t.Fatalf("collect the reference dirty set: %v", err)
	}
	return ids
}

// explain answers the plan PostgreSQL chooses for tq.
func explain(t *testing.T, pool *pgxpool.Pool, tq tracedQuery) string {
	t.Helper()
	rows, err := pool.Query(context.Background(), "EXPLAIN "+tq.sql, tq.args...)
	if err != nil {
		t.Fatalf("explain %q: %v", tq.sql, err)
	}
	lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("collect plan of %q: %v", tq.sql, err)
	}
	return strings.Join(lines, "\n")
}

// The (schema_id, flushed_at) index init-db builds serves the federated
// pass's snapshot read of the strict dirty set (#849, plan step 1), and the
// read selects the set its reference predicate selects.
func TestInitDBChangeLogIndexServesSnapshotDirtySetReads(t *testing.T) {
	tg := newInitDBTarget(t)
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db: %v\n%s", err, out)
	}
	ctx := context.Background()
	fx := seedDirtySetFixture(t, tg.pool, tg.opts.changeLog)
	trace := &queryTrace{}
	pool := tracedPool(t, trace)
	fetcher := federated.NewPostgresDirtyIDFetcher(pool)
	tables := model.StorageTables{ChangeLog: tg.opts.changeLog, EntityMain: tg.opts.entityMain}
	index := changeLogFlushIndex(tg.opts.changeLog)

	trace.take("")
	snap, err := fetcher.ReadChangeLogSnapshot(ctx, federated.ChangeLogSnapshotRequest{Tables: tables, SchemaID: 100})
	if err != nil {
		t.Fatalf("ReadChangeLogSnapshot: %v", err)
	}
	if got, want := sortedIDs(snap.DirtyIDs), sortedIDs(fx.strict); !slices.Equal(got, want) {
		t.Errorf("dirty set = %v, want %v", got, want)
	}
	reference := referenceDirtySet(t, tg.pool, tg.opts.changeLog, 100)
	if got, want := sortedIDs(snap.DirtyIDs), sortedIDs(reference); !slices.Equal(got, want) {
		t.Errorf("dirty set = %v, the reference predicate selects %v", got, want)
	}
	reads := trace.take("SELECT row_id FROM")
	if len(reads) != 1 {
		t.Fatalf("traced %d dirty-set reads, want 1", len(reads))
	}
	plan := explain(t, tg.pool, reads[0])
	if !strings.Contains(plan, index) || strings.Contains(plan, "Seq Scan") {
		t.Errorf("dirty-set read does not use %s:\n%s", index, plan)
	}
}

// A change_log name at PostgreSQL's 63-byte identifier limit still gets the
// flush index, on that table. A plain "<name>_flushed_idx" truncates to the
// table's own name, so CREATE INDEX IF NOT EXISTS found that relation and
// skipped the index without an error, leaving only the primary key.
func TestInitDBBuildsTheFlushIndexOfAMaxLengthChangeLogName(t *testing.T) {
	tg := newInitDBTarget(t)
	tg.opts.changeLog += strings.Repeat("x", sqlutil.MaxIdentifierLen-len(tg.opts.changeLog))
	index := changeLogFlushIndex(tg.opts.changeLog)
	if index == tg.opts.changeLog || len(index) > sqlutil.MaxIdentifierLen {
		t.Fatalf("flush index name %q does not fit beside table %q", index, tg.opts.changeLog)
	}
	for run := 1; run <= 2; run++ {
		if out, err := tg.run(t); err != nil {
			t.Fatalf("init-db run %d: %v\n%s", run, err, out)
		}
	}
	var def string
	err := tg.pool.QueryRow(context.Background(), `SELECT pg_get_indexdef(indexrelid) FROM pg_index
		WHERE indexrelid = to_regclass($1) AND indrelid = to_regclass($2)`,
		quoteIdentifier(index), quoteIdentifier(tg.opts.changeLog)).Scan(&def)
	if err != nil {
		t.Fatalf("flush index %s on %s: %v", index, tg.opts.changeLog, err)
	}
	if want := "(schema_id, flushed_at) INCLUDE (row_id, changed_at)"; !strings.Contains(def, want) {
		t.Errorf("flush index definition = %q, want columns %s", def, want)
	}
}
