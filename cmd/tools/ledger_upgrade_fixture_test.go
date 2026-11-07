package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/sqlutil"
	"github.com/lychee-technology/forma/internal/testdb"
	"github.com/lychee-technology/forma/internal/widthaudit"
)

// The ledger upgrade suite (#738) runs the upgrade in docs/schema-evolution.md,
// "Upgrading an existing deployment", against a deployment from before the
// generation ledger. It runs wherever testdb.ResolveDSQL points: the
// integration PostgreSQL by default, and MiniStack's strict SQL proxy in
// make test-dsql-ministack, which is why everything here is what Aurora DSQL
// accepts: one DDL statement a transaction and no secondary index.
//
// The fixture never seeds the ledger itself (ledgertest.Seed): the first
// generation of every schema comes from the real baseline-schema code.

// Schema ids of the fixture's two registered schemas.
const (
	upgradeOrders = int16(100)
	upgradeVisits = int16(101)
)

// The declarations the deployment runs before the upgrade: orders binds qty
// to smallint_01 and keeps note in EAV, visits keeps an EAV-only integer.
var (
	upgradeOrdersAttrs = "{" + boundAttr("qty", 1, "smallint", "smallint_01") + `,"note":{"attributeID":2,"valueType":"text"}}`
	upgradeVisitsAttrs = `{"count":{"attributeID":1,"valueType":"integer"}}`
)

// upgradeTarget is one test's pre-ledger deployment: its own tables, the
// schemas init-db registered before the ledger existed, stored rows, and the
// schema directory it runs today.
type upgradeTarget struct {
	ctx       context.Context
	dsn       string
	kind      testdb.DSQLKind
	pool      *pgxpool.Pool
	tables    widthaudit.Tables
	registry  string
	schemaDir string
}

var upgradeSerial atomic.Int64

func newUpgradeTarget(t *testing.T) *upgradeTarget {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	dsn, kind := testdb.ResolveDSQL(t)
	if kind == testdb.DSQLEmulated {
		testdb.Connect(t, ctx) // skips locally, and fails in CI, when PostgreSQL is absent
		dsn = testdb.ResolveDSN()
	}
	prefix := fmt.Sprintf("lu_%d_%d", time.Now().UnixNano(), upgradeSerial.Add(1))
	u := &upgradeTarget{
		ctx: ctx, dsn: dsn, kind: kind,
		registry: prefix + "_schema_registry",
		tables: widthaudit.Tables{
			EntityMain: prefix + "_entity_main", EAV: prefix + "_eav", ChangeLog: prefix + "_change_log",
		},
		schemaDir: t.TempDir(),
	}
	u.pool = u.openPool(t, nil)
	t.Cleanup(u.dropTables)
	for _, stmt := range u.preLedgerDDL() {
		u.exec(t, stmt)
	}
	writeLedgerSchema(t, u.schemaDir, "orders", upgradeOrdersAttrs)
	writeLedgerSchema(t, u.schemaDir, "visits", upgradeVisitsAttrs)
	for name, id := range map[string]int16{"orders": upgradeOrders, "visits": upgradeVisits} {
		u.exec(t, fmt.Sprintf("INSERT INTO %s (schema_name, schema_id) VALUES ('%s', %d)", quoteIdentifier(u.registry), name, id))
	}
	u.storeOrder(t, 7, "first")
	u.storeOrder(t, 300, "second")
	u.storeVisit(t, "41")
	return u
}

// openPool opens a pool on the target. tracer, when set, sees every statement.
func (u *upgradeTarget) openPool(t *testing.T, tracer *pointerBarrier) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(u.dsn)
	if err != nil {
		t.Fatalf("parse the target's DSN: %v", err)
	}
	cfg.MaxConns = 4
	if tracer != nil {
		cfg.ConnConfig.Tracer = tracer
	}
	pool, err := pgxpool.NewWithConfig(u.ctx, cfg)
	if err != nil {
		t.Fatalf("open a pool on the target: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// preLedgerDDL is the core of an init-db run from before #733, from
// init-db's own table sources: the registry without its active_generation
// column, entity_main, the EAV table and the change log. Each runs alone, as
// DSQL runs DDL, and none is an index.
func (u *upgradeTarget) preLedgerDDL() []string {
	return []string{
		schemaRegistryDDL(quoteIdentifier(u.registry)),
		entityMainDDL(quoteIdentifier(u.tables.EntityMain)),
		eavTableDDL(quoteIdentifier(u.tables.EAV)),
		changeLogDDL(quoteIdentifier(u.tables.ChangeLog)),
	}
}

// addLedger runs the upgrade's first step as the runbook gives it for DSQL:
// the exact schemaledger.Statements, each in its own transaction.
func (u *upgradeTarget) addLedger(t *testing.T) {
	t.Helper()
	for _, stmt := range schemaledger.Statements(u.registry) {
		u.exec(t, stmt)
	}
}

// exec runs one statement in its own implicit transaction.
func (u *upgradeTarget) exec(t *testing.T, stmt string, args ...any) {
	t.Helper()
	if _, err := u.pool.Exec(u.ctx, stmt, args...); err != nil {
		t.Fatalf("statement %q: %v", stmt, err)
	}
}

func (u *upgradeTarget) dropTables() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ledger := schemaledger.TablesFor(u.registry)
	for _, table := range []string{
		quoteIdentifier(u.registry), ledger.Generation, ledger.Attribute,
		quoteIdentifier(u.tables.EntityMain), quoteIdentifier(u.tables.EAV), quoteIdentifier(u.tables.ChangeLog),
	} {
		_, _ = u.pool.Exec(ctx, "DROP TABLE IF EXISTS "+table)
	}
}

// storeOrder stores an orders row as the write path does: qty in its bound
// column, note in EAV, and the row's change_log version.
func (u *upgradeTarget) storeOrder(t *testing.T, qty int, note string) {
	t.Helper()
	row := uuid.New()
	now := time.Now().UnixMilli()
	u.exec(t, fmt.Sprintf("INSERT INTO %s (ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, smallint_01) VALUES ($1, $2, $3, $3, $4)",
		quoteIdentifier(u.tables.EntityMain)), upgradeOrders, row, now, qty)
	u.exec(t, fmt.Sprintf("INSERT INTO %s (schema_id, row_id, attr_id, value_text) VALUES ($1, $2, 2, $3)",
		quoteIdentifier(u.tables.EAV)), upgradeOrders, row, note)
	u.exec(t, fmt.Sprintf("INSERT INTO %s (schema_id, row_id, changed_at) VALUES ($1, $2, $3)",
		quoteIdentifier(u.tables.ChangeLog)), upgradeOrders, row, now)
}

// storeVisit stores a visits row whose EAV-only count holds value, a
// NUMERIC literal: anything but an integer is outside the declaration.
func (u *upgradeTarget) storeVisit(t *testing.T, value string) uuid.UUID {
	t.Helper()
	row := uuid.New()
	now := time.Now().UnixMilli()
	u.exec(t, fmt.Sprintf("INSERT INTO %s (ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at) VALUES ($1, $2, $3, $3)",
		quoteIdentifier(u.tables.EntityMain)), upgradeVisits, row, now)
	u.exec(t, fmt.Sprintf("INSERT INTO %s (schema_id, row_id, attr_id, value_numeric) VALUES ($1, $2, 1, %s)",
		quoteIdentifier(u.tables.EAV), value), upgradeVisits, row)
	return row
}

// deleteVisit removes a visits row from every table, as a delete and its
// flush leave it.
func (u *upgradeTarget) deleteVisit(t *testing.T, row uuid.UUID) {
	t.Helper()
	u.exec(t, fmt.Sprintf("DELETE FROM %s WHERE schema_id = $1 AND row_id = $2", quoteIdentifier(u.tables.EAV)), upgradeVisits, row)
	u.exec(t, fmt.Sprintf("DELETE FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2",
		quoteIdentifier(u.tables.EntityMain)), upgradeVisits, row)
}

func (u *upgradeTarget) baselineOptions(dryRun bool, parquet *parquetTier) baselineOptions {
	return baselineOptions{
		ledgerToolOptions: ledgerToolOptions{schemaTable: u.registry, schemaDir: u.schemaDir, dryRun: dryRun},
		tables:            u.tables,
		parquet:           parquet,
	}
}

// baseline runs baseline-schema over every registered schema; parquet nil is
// --no-parquet-tier.
func (u *upgradeTarget) baseline(t *testing.T, dryRun bool, parquet *parquetTier) (string, error) {
	t.Helper()
	var out bytes.Buffer
	err := baselineSchemas(u.ctx, u.pool, u.baselineOptions(dryRun, parquet), &out)
	return out.String(), err
}

// apply runs apply-schema over every registered schema with the
// declarations in dir.
func (u *upgradeTarget) apply(t *testing.T, pool *pgxpool.Pool, dir string, dryRun bool) (string, error) {
	t.Helper()
	var out bytes.Buffer
	opts := ledgerToolOptions{schemaTable: u.registry, schemaDir: dir, dryRun: dryRun}
	err := applySchemas(u.ctx, pool, opts, &out)
	return out.String(), err
}

// declarationDir writes a schema directory holding these declarations of
// orders and visits.
func (u *upgradeTarget) declarationDir(t *testing.T, ordersAttrs, visitsAttrs string) string {
	t.Helper()
	dir := t.TempDir()
	writeLedgerSchema(t, dir, "orders", ordersAttrs)
	writeLedgerSchema(t, dir, "visits", visitsAttrs)
	return dir
}

// pointers answers every registered schema's active generation, 0 for none.
func (u *upgradeTarget) pointers(t *testing.T) map[int16]int64 {
	t.Helper()
	rows, err := u.pool.Query(u.ctx, fmt.Sprintf("SELECT schema_id, COALESCE(%s, 0) FROM %s",
		schemaledger.PointerColumn, quoteIdentifier(u.registry)))
	if err != nil {
		t.Fatalf("read the active pointers: %v", err)
	}
	defer rows.Close()
	got := map[int16]int64{}
	for rows.Next() {
		var id int16
		var n int64
		if err := rows.Scan(&id, &n); err != nil {
			t.Fatalf("scan an active pointer: %v", err)
		}
		got[id] = n
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read the active pointers: %v", err)
	}
	return got
}

// requirePointers fails unless every registered schema's active generation
// is the one given.
func (u *upgradeTarget) requirePointers(t *testing.T, orders, visits int64) {
	t.Helper()
	got := u.pointers(t)
	if len(got) != 2 || got[upgradeOrders] != orders || got[upgradeVisits] != visits {
		t.Fatalf("active generations = %v, want orders %d and visits %d (0 is none)", got, orders, visits)
	}
}

// requireActiveIs fails unless schemaID's active generation is number and
// holds exactly the declaration in dir.
func (u *upgradeTarget) requireActiveIs(t *testing.T, schemaID int16, name, dir string, number int64) {
	t.Helper()
	gen, ok, err := schemaledger.ReadActive(u.ctx, u.pool, u.registry, schemaID)
	if err != nil || !ok {
		t.Fatalf("read the active generation of %s: ok=%t err=%v", name, ok, err)
	}
	decl, _, err := schemameta.ReadDeclaration(dir, name, schemaID)
	if err != nil {
		t.Fatalf("read the declaration of %s: %v", name, err)
	}
	if gen.Number != number || gen.Hash != decl.Hash || len(gen.Entries) != len(decl.Entries) {
		t.Fatalf("%s's active generation = %d (hash %s, %d entries), want %d holding the declaration (hash %s, %d entries)",
			name, gen.Number, gen.Hash, len(gen.Entries), number, decl.Hash, len(decl.Entries))
	}
	for i, want := range decl.Entries {
		if gen.Entries[i] != want {
			t.Fatalf("%s's generation %d entry %d = %+v, want %+v", name, number, i, gen.Entries[i], want)
		}
	}
}

// ledgerRows counts the ledger's rows: generations per schema, and attribute
// rows whose generation the generation table does not hold.
func (u *upgradeTarget) ledgerRows(t *testing.T) (generations map[int16][]int64, orphans int) {
	t.Helper()
	ledger := schemaledger.TablesFor(u.registry)
	rows, err := u.pool.Query(u.ctx, fmt.Sprintf("SELECT schema_id, generation FROM %s ORDER BY schema_id, generation", ledger.Generation))
	if err != nil {
		t.Fatalf("read the generation table: %v", err)
	}
	defer rows.Close()
	generations = map[int16][]int64{}
	for rows.Next() {
		var id int16
		var n int64
		if err := rows.Scan(&id, &n); err != nil {
			t.Fatalf("scan a generation: %v", err)
		}
		generations[id] = append(generations[id], n)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read the generation table: %v", err)
	}
	orphanQuery := fmt.Sprintf(`SELECT COUNT(*) FROM %s a LEFT JOIN %s g ON g.schema_id = a.schema_id AND g.generation = a.generation
		WHERE g.generation IS NULL`, ledger.Attribute, ledger.Generation)
	if err := u.pool.QueryRow(u.ctx, orphanQuery).Scan(&orphans); err != nil {
		t.Fatalf("count orphan attribute rows: %v", err)
	}
	return generations, orphans
}

// registryAnswers selects column from the registry table: nil when the column
// exists. It asks with a query rather than a catalog, whose answers differ on
// DSQL.
func (u *upgradeTarget) registryAnswers(t *testing.T, column string) error {
	t.Helper()
	_, err := u.pool.Exec(u.ctx, fmt.Sprintf("SELECT %s FROM %s LIMIT 1", sqlutil.SanitizeIdentifier(column), quoteIdentifier(u.registry)))
	return err
}

// mentionsAll fails unless text contains every want.
func mentionsAll(t *testing.T, what, text string, want ...string) {
	t.Helper()
	for _, w := range want {
		if !strings.Contains(text, w) {
			t.Fatalf("%s does not mention %q:\n%s", what, w, text)
		}
	}
}
