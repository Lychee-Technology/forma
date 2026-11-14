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
	cutoff int64
	strict []uuid.UUID // an unflushed entry
	hot    []uuid.UUID // strict, and the rows marked at or after cutoff
}

// seedDirtySetFixture writes a change_log whose schema 100 has a long flushed
// history, so that a read walking every entry costs what it would in
// production, and a few rows in each case the two dirty-set forms tell apart.
// Schema 101's unflushed rows must stay out of schema 100's sets.
func seedDirtySetFixture(t *testing.T, pool *pgxpool.Pool, changeLog string) dirtySetFixture {
	t.Helper()
	ctx := context.Background()
	cl := quoteIdentifier(changeLog)
	const history = int64(1_600_000_000_000)
	f := dirtySetFixture{cutoff: 1_700_000_000_000}
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
	both := uuid.New() // an unflushed entry and one marked in the window: one ID
	entry(100, both, 0)
	entry(100, both, f.cutoff+1)
	f.strict = append(f.strict, both)
	f.hot = append(slices.Clone(f.strict), uuid.New(), uuid.New())
	entry(100, f.hot[len(f.hot)-2], f.cutoff) // the cutoff is inclusive
	entry(100, f.hot[len(f.hot)-1], f.cutoff+5)
	entry(100, uuid.New(), f.cutoff-1) // marked before the cutoff: clean
	entry(101, uuid.New(), 0)
	entry(101, uuid.New(), f.cutoff+1)
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

// referenceDirtySet reads a dirty set with a predicate written out here,
// independently of the engine's dirtySetQuery: the strict set (flushed_at = 0)
// when cutoff is nil, otherwise the set widened to the entries marked at or
// after *cutoff. The retired attach template read the widened set this way
// in DuckDB (#858).
func referenceDirtySet(t *testing.T, pool *pgxpool.Pool, changeLog string, schemaID int16, cutoff *int64) []uuid.UUID {
	t.Helper()
	query := fmt.Sprintf(`SELECT DISTINCT row_id FROM %s WHERE schema_id = $1 AND flushed_at = 0`, quoteIdentifier(changeLog))
	args := []any{schemaID}
	if cutoff != nil {
		query = fmt.Sprintf(`SELECT DISTINCT row_id FROM %s WHERE schema_id = $1 AND (flushed_at = 0 OR flushed_at >= $2)`, quoteIdentifier(changeLog))
		args = append(args, *cutoff)
	}
	rows, err := pool.Query(context.Background(), query, args...)
	if err != nil {
		t.Fatalf("read the reference dirty set: %v", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	if err != nil {
		t.Fatalf("collect the reference dirty set: %v", err)
	}
	return ids
}

// referenceParquetHistory answers, with a query written out here, whether
// schemaID has a flushed change_log entry or a live entity_main row without
// any entry: the two records the snapshot's parquet-history answer reads.
func referenceParquetHistory(t *testing.T, pool *pgxpool.Pool, tables model.StorageTables, schemaID int16) bool {
	t.Helper()
	var found bool
	err := pool.QueryRow(context.Background(), fmt.Sprintf(`SELECT
		EXISTS (SELECT 1 FROM %[1]s WHERE schema_id = $1 AND flushed_at <> 0)
		OR EXISTS (SELECT 1 FROM %[2]s m WHERE m.ltbase_schema_id = $1
			AND NOT EXISTS (SELECT 1 FROM %[1]s c WHERE c.schema_id = $1 AND c.row_id = m.ltbase_row_id))`,
		quoteIdentifier(tables.ChangeLog), quoteIdentifier(tables.EntityMain)), schemaID).Scan(&found)
	if err != nil {
		t.Fatalf("read the reference parquet history of schema %d: %v", schemaID, err)
	}
	return found
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

// The (schema_id, flushed_at) index init-db builds serves both dirty-set
// forms of the federated pass's snapshot read (#849, plan step 1), and each
// form reads the set its reference predicate selects: the strict set for the
// hot-excluded form, the widened one for the hot form. The parquet-history
// answer read in the snapshot is the reference's.
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

	cases := []struct {
		form      federated.DirtySetForm
		want      []uuid.UUID
		reference []uuid.UUID
	}{
		{federated.DirtySetHotExcluded, fx.strict, referenceDirtySet(t, tg.pool, tg.opts.changeLog, 100, nil)},
		{federated.DirtySetHot, fx.hot, referenceDirtySet(t, tg.pool, tg.opts.changeLog, 100, &fx.cutoff)},
	}
	for _, tc := range cases {
		trace.take("")
		snap, err := fetcher.ReadChangeLogSnapshot(ctx, federated.ChangeLogSnapshotRequest{
			Tables: tables, SchemaID: 100, Form: tc.form, GraceCutoffMs: fx.cutoff, ParquetHistory: true,
		})
		if err != nil {
			t.Fatalf("%s: ReadChangeLogSnapshot: %v", tc.form, err)
		}
		if got, want := sortedIDs(snap.DirtyIDs), sortedIDs(tc.want); !slices.Equal(got, want) {
			t.Errorf("%s dirty set = %v, want %v", tc.form, got, want)
		}
		if got, want := sortedIDs(snap.DirtyIDs), sortedIDs(tc.reference); !slices.Equal(got, want) {
			t.Errorf("%s dirty set = %v, the reference predicate selects %v", tc.form, got, want)
		}
		if !snap.HasParquetHistory {
			t.Errorf("%s: schema 100 has flushed entries, but the snapshot reads no parquet history", tc.form)
		}
		reads := trace.take("SELECT row_id FROM")
		if len(reads) != 1 {
			t.Fatalf("%s: traced %d dirty-set reads, want 1", tc.form, len(reads))
		}
		plan := explain(t, tg.pool, reads[0])
		if !strings.Contains(plan, index) || strings.Contains(plan, "Seq Scan") {
			t.Errorf("%s dirty-set read does not use %s:\n%s", tc.form, index, plan)
		}
	}
	assertSnapshotHistoryMatches(t, tg.pool, fetcher, tables)
}

// assertSnapshotHistoryMatches compares the snapshot's parquet-history answer
// with the reference's for a schema with history and one without.
func assertSnapshotHistoryMatches(t *testing.T, pool *pgxpool.Pool, fetcher *federated.PostgresDirtyIDFetcher, tables model.StorageTables) {
	t.Helper()
	ctx := context.Background()
	for _, schemaID := range []int16{100, 101, 102} {
		want := referenceParquetHistory(t, pool, tables, schemaID)
		snap, err := fetcher.ReadChangeLogSnapshot(ctx, federated.ChangeLogSnapshotRequest{
			Tables: tables, SchemaID: schemaID, Form: federated.DirtySetHotExcluded, ParquetHistory: true,
		})
		if err != nil {
			t.Fatalf("ReadChangeLogSnapshot(%d): %v", schemaID, err)
		}
		if snap.HasParquetHistory != want {
			t.Errorf("schema %d: snapshot parquet history = %v, the reference answers %v", schemaID, snap.HasParquetHistory, want)
		}
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
