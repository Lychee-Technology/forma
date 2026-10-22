package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/declaredindex"
	"github.com/lychee-technology/forma/internal/rowguard"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/testdb"
	"github.com/lychee-technology/forma/internal/uniquekey"
)

// These tests run init-db against a real PostgreSQL (internal/testdb: CI's
// provisioned service, skipped locally when none is reachable). Each one gets
// its own table names, so they share the database with every other suite.

// loggedStatement is one statement init-db issued and whether its connection
// was inside a transaction block at the time.
type loggedStatement struct {
	sql  string
	inTx bool
}

// statementLog is a pgx.QueryTracer recording every statement init-db issues.
type statementLog struct {
	mu      sync.Mutex
	entries []loggedStatement
}

func (l *statementLog) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, loggedStatement{sql: data.SQL, inTx: conn.PgConn().TxStatus() != 'I'})
	return ctx
}

func (l *statementLog) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// matching returns the logged statements containing substr.
func (l *statementLog) matching(substr string) []loggedStatement {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []loggedStatement
	for _, e := range l.entries {
		if strings.Contains(e.sql, substr) {
			out = append(out, e)
		}
	}
	return out
}

func (l *statementLog) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = nil
}

// initDBTarget is one test's init-db target: unique table names, a schema
// directory, and init-db's pool swapped for one that logs its statements.
type initDBTarget struct {
	pool *pgxpool.Pool
	opts initDBOptions
	log  *statementLog
}

func newInitDBTarget(t *testing.T) *initDBTarget {
	t.Helper()
	ctx := context.Background()
	pool := testdb.Connect(t, ctx)

	prefix := fmt.Sprintf("i647_%08x", rand.Uint32())
	tg := &initDBTarget{
		pool: pool,
		opts: initDBOptions{
			schemaTable: prefix + "_schema_registry",
			eavTable:    prefix + "_eav",
			entityMain:  prefix + "_entity_main",
			changeLog:   prefix + "_change_log",
			schemaDir:   t.TempDir(),
		},
		log: &statementLog{},
	}
	t.Cleanup(func() {
		for _, table := range []string{tg.opts.schemaTable, tg.opts.eavTable, tg.opts.entityMain, tg.opts.changeLog,
			uniquekey.KeyTable(tg.opts.entityMain), uniquekey.StateTable(tg.opts.entityMain), rowguard.Table(tg.opts.entityMain)} {
			if _, err := pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+quoteIdentifier(table)); err != nil {
				t.Logf("drop %s: %v", table, err)
			}
		}
	})

	old := toolPostgresPoolFn
	t.Cleanup(func() { toolPostgresPoolFn = old })
	toolPostgresPoolFn = func(ctx context.Context, _ forma.DatabaseConfig) (*pgxpool.Pool, error) {
		cfg, err := pgxpool.ParseConfig(testdb.ResolveDSN())
		if err != nil {
			return nil, err
		}
		cfg.ConnConfig.Tracer = tg.log
		return pgxpool.NewWithConfig(ctx, cfg)
	}
	return tg
}

// run executes init-db's whole flow and returns what it printed.
func (tg *initDBTarget) run(t *testing.T) (string, error) {
	t.Helper()
	var err error
	stdout, _ := captureStdStreams(t, func() {
		err = initDatabase(context.Background(), tg.opts, forma.DatabaseConfig{})
	})
	return stdout, err
}

// writeSchema writes name.json declaring indexes and its attributes file;
// attributes bound to a bigint column are date-times, the rest text.
func (tg *initDBTarget) writeSchema(t *testing.T, name string, attrs map[string]string, indexes []forma.IndexDeclaration) {
	t.Helper()
	props := map[string]any{}
	attributes := map[string]any{}
	id := 1
	for attr, col := range attrs {
		prop, valueType := map[string]any{"type": "string"}, "text"
		if strings.HasPrefix(col, "bigint_") {
			prop["format"], valueType = "date-time", "datetime"
		}
		props[attr] = prop
		attributes[attr] = map[string]any{"attributeID": id, "valueType": valueType, "column_binding": map[string]any{"col_name": col}}
		id++
	}
	doc := map[string]any{"type": "object", "properties": props}
	if len(indexes) > 0 {
		doc[schemameta.IndexDeclarationsKey] = indexes
	}
	for file, v := range map[string]any{name + ".json": doc, name + "_attributes.json": attributes} {
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal %s: %v", file, err)
		}
		if err := os.WriteFile(filepath.Join(tg.opts.schemaDir, file), data, 0o600); err != nil {
			t.Fatalf("write %s: %v", file, err)
		}
	}
}

// writeOutboxAndTimer writes the issue's outbox (due, lease, fifo) and a timer
// schema whose due_at binds the same (text_01, bigint_01) tuple as due.
func (tg *initDBTarget) writeOutboxAndTimer(t *testing.T, outboxIndexes ...string) {
	t.Helper()
	all := map[string]forma.IndexDeclaration{
		"due":   {Name: "due", Attributes: []string{"status", "next_attempt_at"}},
		"lease": {Name: "lease", Attributes: []string{"status", "locked_until"}},
		"fifo":  {Name: "fifo", Attributes: []string{"partition_key", "enqueued_at"}},
	}
	var decls []forma.IndexDeclaration
	for _, name := range outboxIndexes {
		decls = append(decls, all[name])
	}
	tg.writeSchema(t, "outbox_message", map[string]string{
		"status": "text_01", "partition_key": "text_02",
		"next_attempt_at": "bigint_01", "locked_until": "bigint_02", "enqueued_at": "bigint_03",
	}, decls)
	tg.writeSchema(t, "timer", map[string]string{"state": "text_01", "fire_at": "bigint_01"},
		[]forma.IndexDeclaration{{Name: "due_at", Attributes: []string{"state", "fire_at"}}})
}

// declaredIndexDef is one declared index as the catalog describes it.
type declaredIndexDef struct {
	def   string
	valid bool
	oid   uint32
}

// declaredIndexes reads every declared-name index on the target's entity_main
// from pg_indexes and pg_index, independently of declaredindex's own query.
func (tg *initDBTarget) declaredIndexes(t *testing.T) map[string]declaredIndexDef {
	t.Helper()
	rows, err := tg.pool.Query(context.Background(), `
		SELECT p.indexname::text, p.indexdef, i.indisvalid, i.indexrelid::oid::int8
		FROM pg_indexes p
		JOIN pg_class c ON c.relname = p.indexname AND c.relnamespace = to_regnamespace(p.schemaname)
		JOIN pg_index i ON i.indexrelid = c.oid
		WHERE p.tablename = $1 AND p.indexname LIKE $2`, tg.opts.entityMain, tg.opts.entityMain+`\_cx\_%`)
	if err != nil {
		t.Fatalf("read declared indexes: %v", err)
	}
	found := map[string]declaredIndexDef{}
	var name, def string
	var valid bool
	var oid int64
	if _, err := pgx.ForEachRow(rows, []any{&name, &def, &valid, &oid}, func() error {
		found[name] = declaredIndexDef{def: def, valid: valid, oid: uint32(oid)}
		return nil
	}); err != nil {
		t.Fatalf("scan declared indexes: %v", err)
	}
	return found
}

// wantDef is the definition pg_indexes reports for a plain btree index over
// cols: no WHERE clause, so not partial.
func (tg *initDBTarget) wantDef(cols ...string) (string, string) {
	key := append(append([]string{"ltbase_schema_id"}, cols...), "ltbase_row_id")
	name := declaredindex.Name(tg.opts.entityMain, key)
	return name, fmt.Sprintf("CREATE INDEX %s ON public.%s USING btree (%s)", name, tg.opts.entityMain, strings.Join(key, ", "))
}

// The issue's first Given/When/Then plus deduplication and idempotence: the
// due, lease and fifo declarations become three valid, non-partial indexes
// keyed (ltbase_schema_id, cols…, ltbase_row_id), timer.due_at shares due's
// index, each build ran outside any transaction, and a second run issues no
// index DDL at all.
func TestInitDBBuildsDeclaredIndexes(t *testing.T) {
	tg := newInitDBTarget(t)
	tg.writeOutboxAndTimer(t, "due", "lease", "fifo")

	out, err := tg.run(t)
	if err != nil {
		t.Fatalf("init-db: %v\n%s", err, out)
	}
	tables := tg.log.matching("CREATE TABLE")
	for _, ddl := range tables {
		if !ddl.inTx {
			t.Fatalf("the tracer did not see init-db's table transaction: %s", ddl.sql)
		}
	}
	builds := tg.log.matching("CREATE INDEX CONCURRENTLY")
	tg.log.reset()
	if len(tables) == 0 || len(builds) != 3 {
		t.Fatalf("init-db issued %d table statements and %d concurrent builds, want some and 3 (due and timer.due_at share one): %+v", len(tables), len(builds), builds)
	}
	for _, b := range builds {
		if b.inTx {
			t.Fatalf("declared index built inside a transaction block: %s", b.sql)
		}
	}

	found := tg.declaredIndexes(t)
	if len(found) != 3 {
		t.Fatalf("entity_main carries %d declared indexes, want 3: %+v", len(found), found)
	}
	for _, cols := range [][]string{{"text_01", "bigint_01"}, {"text_01", "bigint_02"}, {"text_02", "bigint_03"}} {
		name, def := tg.wantDef(cols...)
		got, ok := found[name]
		switch {
		case !ok:
			t.Fatalf("no declared index %s over %v; found %+v", name, cols, found)
		case got.def != def:
			t.Fatalf("index %s:\n got %s\nwant %s", name, got.def, def)
		case !got.valid:
			t.Fatalf("index %s is not valid", name)
		}
	}
	shared, _ := tg.wantDef("text_01", "bigint_01")
	if !strings.Contains(out, "Created declared index: "+shared+" on "+tg.opts.entityMain+
		" (ltbase_schema_id, text_01, bigint_01, ltbase_row_id) for outbox_message.due, timer.due_at\n") {
		t.Fatalf("init-db output does not show the shared index serving both schemas:\n%s", out)
	}

	out, err = tg.run(t)
	if err != nil {
		t.Fatalf("second init-db: %v\n%s", err, out)
	}
	if again := tg.log.matching("CREATE INDEX CONCURRENTLY"); len(again) != 0 {
		t.Fatalf("second run issued declared-index DDL: %+v", again)
	}
	if !strings.Contains(out, "Declared indexes: 0 created, 3 already present, 0 unreferenced\n") {
		t.Fatalf("second run output:\n%s", out)
	}
	for name, before := range found {
		if after := tg.declaredIndexes(t)[name]; after.oid != before.oid {
			t.Fatalf("index %s was rebuilt by the second run (oid %d -> %d)", name, before.oid, after.oid)
		}
	}
}

// A declaration removed from the schema leaves its index in place: init-db
// reports it unreferenced, exits zero, and drops nothing.
func TestInitDBReportsUnreferencedIndexAndNeverDrops(t *testing.T) {
	tg := newInitDBTarget(t)
	tg.writeOutboxAndTimer(t, "due", "lease", "fifo")
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db: %v\n%s", err, out)
	}

	tg.writeOutboxAndTimer(t, "due", "lease")
	out, err := tg.run(t)
	if err != nil {
		t.Fatalf("init-db after removing fifo: %v\n%s", err, out)
	}
	if drops := tg.log.matching("DROP"); len(drops) != 0 {
		t.Fatalf("init-db dropped something: %+v", drops)
	}
	fifo, _ := tg.wantDef("text_02", "bigint_03")
	if !strings.Contains(out, "Unreferenced declared index: "+fifo+" on "+tg.opts.entityMain+" (ltbase_schema_id, text_02, bigint_03, ltbase_row_id)") {
		t.Fatalf("output does not report %s as unreferenced:\n%s", fifo, out)
	}
	if got, ok := tg.declaredIndexes(t)[fifo]; !ok || !got.valid {
		t.Fatalf("unreferenced index %s must stay in place and valid, found %+v", fifo, got)
	}
}

// The issue's invalid-index case: a concurrent build canceled while it waits
// for an open write transaction leaves an invalid index under the planned
// name. init-db then exits non-zero naming it and the remediation, and leaves
// it where it is.
func TestInitDBExitsNonZeroOnInvalidDeclaredIndex(t *testing.T) {
	tg := newInitDBTarget(t)
	tg.writeOutboxAndTimer(t, "due")
	args := []string{"init-db",
		"--schema-table", tg.opts.schemaTable, "--eav-table", tg.opts.eavTable,
		"--entity-main-table", tg.opts.entityMain, "--change-log-table", tg.opts.changeLog,
		"--schema-dir", tg.opts.schemaDir}

	var errOut bytes.Buffer
	var code int
	captureStdStreams(t, func() { code = runToolMain(context.Background(), append(args, "--skip-declared-indexes"), &errOut) })
	if code != 0 {
		t.Fatalf("init-db --skip-declared-indexes exited %d: %s", code, errOut.String())
	}
	due, _ := tg.wantDef("text_01", "bigint_01")
	leaveInvalidIndex(t, tg.pool, tg.opts.entityMain, due)

	stdout, _ := captureStdStreams(t, func() { code = runToolMain(context.Background(), args, &errOut) })
	if code == 0 {
		t.Fatalf("init-db exited 0 with an invalid declared index:\n%s", stdout)
	}
	for _, want := range []string{"init-db: ", due, "is invalid", "DROP INDEX CONCURRENTLY"} {
		if !strings.Contains(errOut.String(), want) {
			t.Fatalf("init-db error %q does not contain %q", errOut.String(), want)
		}
	}
	if strings.Contains(stdout, "Database initialized successfully.") {
		t.Fatalf("init-db claimed success:\n%s", stdout)
	}
	if got, ok := tg.declaredIndexes(t)[due]; !ok || got.valid {
		t.Fatalf("the invalid index must be left in place, still invalid; found %+v (present %v)", got, ok)
	}
}

// An index under the planned name that PostgreSQL reports valid but that is
// not the plain btree CreateSQL builds makes init-db exit non-zero naming the
// difference, and it is left in place. Each look-alike keeps the planned
// columns in order, so only the catalog's shape check can tell them apart.
func TestInitDBRefusesLookAlikeDeclaredIndex(t *testing.T) {
	cases := []struct {
		name, key, deviation string
	}{
		{"brin", "USING brin (ltbase_schema_id, text_01, bigint_01, ltbase_row_id)", "brin access method"},
		{"include", "(ltbase_schema_id, text_01) INCLUDE (bigint_01, ltbase_row_id)", "INCLUDE columns"},
		{"opclass", "(ltbase_schema_id, text_01 text_pattern_ops, bigint_01, ltbase_row_id)", "non-default operator class"},
		{"collation", `(ltbase_schema_id, text_01 COLLATE "C", bigint_01, ltbase_row_id)`, "non-default collation"},
		{"ordering", "(ltbase_schema_id, text_01, bigint_01 DESC, ltbase_row_id)", "DESC or NULLS FIRST ordering"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tg := newInitDBTarget(t)
			tg.writeOutboxAndTimer(t, "due")
			tg.opts.skipDeclaredIndexes = true
			if out, err := tg.run(t); err != nil {
				t.Fatalf("init-db --skip-declared-indexes: %v\n%s", err, out)
			}
			due, _ := tg.wantDef("text_01", "bigint_01")
			if _, err := tg.pool.Exec(context.Background(), fmt.Sprintf("CREATE INDEX %s ON %s %s",
				quoteIdentifier(due), quoteIdentifier(tg.opts.entityMain), tc.key)); err != nil {
				t.Fatalf("create look-alike index: %v", err)
			}

			tg.opts.skipDeclaredIndexes = false
			out, err := tg.run(t)
			if err == nil {
				t.Fatalf("init-db accepted the %s look-alike as the declared index:\n%s", tc.name, out)
			}
			for _, want := range []string{due, "is not a plain btree (" + tc.deviation + ")", "DROP INDEX CONCURRENTLY"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("init-db error %q does not contain %q", err, want)
				}
			}
			if builds := tg.log.matching("CREATE INDEX CONCURRENTLY"); len(builds) != 0 {
				t.Fatalf("init-db issued declared-index DDL over the look-alike: %+v", builds)
			}
			if got, ok := tg.declaredIndexes(t)[due]; !ok || !got.valid {
				t.Fatalf("the look-alike index must be left in place; found %+v (present %v)", got, ok)
			}
		})
	}
}

// leaveInvalidIndex starts the planned build of name on table while another
// connection holds an open write transaction there, waits until
// pg_stat_progress_create_index shows the build waiting on it, and cancels the
// build. PostgreSQL leaves the index in the catalog with indisvalid = false.
func leaveInvalidIndex(t *testing.T, pool *pgxpool.Pool, table, name string) {
	t.Helper()
	ctx := context.Background()
	writer, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin blocking writer: %v", err)
	}
	defer func() { _ = writer.Rollback(ctx) }()
	if _, err := writer.Exec(ctx, "INSERT INTO "+quoteIdentifier(table)+
		" (ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at) VALUES (999, gen_random_uuid(), 0, 0)"); err != nil {
		t.Fatalf("open write transaction: %v", err)
	}

	builder, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire builder: %v", err)
	}
	defer builder.Release()
	var pid int32
	if err := builder.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatalf("builder pid: %v", err)
	}
	cols := []string{"ltbase_schema_id", "text_01", "bigint_01", "ltbase_row_id"}
	build := declaredindex.Index{Table: table, Name: name, Columns: cols}
	done := make(chan error, 1)
	go func() {
		_, err := builder.Exec(ctx, build.CreateSQL())
		done <- err
	}()

	waitUntil(t, "the build waits on the open writer", func() bool {
		var waiting bool
		err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_progress_create_index
			WHERE pid = $1 AND phase LIKE 'waiting for%')`, pid).Scan(&waiting)
		return err == nil && waiting
	})
	if _, err := pool.Exec(ctx, "SELECT pg_cancel_backend($1)", pid); err != nil {
		t.Fatalf("cancel build: %v", err)
	}
	var pgErr *pgconn.PgError
	if err := <-done; !errors.As(err, &pgErr) || pgErr.Code != "57014" {
		t.Fatalf("canceled build returned %v, want query_canceled (57014)", err)
	}
}

// waitUntil polls cond until it holds, failing after 30 seconds.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
