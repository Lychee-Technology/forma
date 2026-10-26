package schemaledger

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/testdb"
)

// ledgerFixture creates a schema registry table, in the shape init-db
// creates it before the ledger, with one registered schema (id 9), and adds
// the ledger with Statements as an upgrade does.
func ledgerFixture(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	ctx := context.Background()
	pool := testdb.Connect(t, ctx)
	registry := fmt.Sprintf("ledger_registry_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		for _, table := range []string{registry, registry + GenerationTableSuffix, registry + AttributeTableSuffix} {
			_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
		}
	})
	stmts := append([]string{
		fmt.Sprintf("CREATE TABLE %s (schema_name TEXT PRIMARY KEY, schema_id SMALLINT UNIQUE NOT NULL)", registry),
		fmt.Sprintf("INSERT INTO %s (schema_name, schema_id) VALUES ('orders', 9)", registry),
	}, Statements(registry)...)
	stmts = append(stmts, Statements(registry)...) // idempotent, as init-db reruns it
	for _, stmt := range stmts {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("fixture statement %q: %v", stmt, err)
		}
	}
	return pool, registry
}

func record(from int64, hash string, entries ...Entry) Record {
	return Record{SchemaID: 9, From: from, Hash: hash, Entries: entries, RecordedBy: "test"}
}

func writeCommitted(t *testing.T, pool *pgxpool.Pool, registry string, rec Record) error {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := Write(ctx, tx, registry, rec); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return nil
}

func TestLedgerRecordsAndReadsTheActiveGeneration(t *testing.T) {
	pool, registry := ledgerFixture(t)
	ctx := context.Background()
	if _, ok, err := ReadActive(ctx, pool, registry, 9); err != nil || ok {
		t.Fatalf("a schema with no baseline must read as none, got ok=%t err=%v", ok, err)
	}
	retired := eav(3, "gone", forma.ValueTypeNumeric)
	retired.Retired = true
	entries := []Entry{bound(1, "total", forma.ValueTypeBigInt, "bigint_1"), list(2, "tags", forma.ValueTypeText), retired}
	if err := writeCommitted(t, pool, registry, record(0, "h1", entries...)); err != nil {
		t.Fatalf("Write from none: %v", err)
	}
	rec := record(1, "h2", entries...)
	rec.Warnings = []string{"w1", "w2"}
	if err := writeCommitted(t, pool, registry, rec); err != nil {
		t.Fatalf("Write from 1: %v", err)
	}
	got, ok, err := ReadActive(ctx, pool, registry, 9)
	if err != nil || !ok {
		t.Fatalf("ReadActive: ok=%t err=%v", ok, err)
	}
	if got.Number != 2 || got.Hash != "h2" || len(got.Entries) != 3 {
		t.Fatalf("ReadActive = %+v, want generation 2 with hash h2 and 3 entries", got)
	}
	for i, want := range entries {
		if got.Entries[i] != want {
			t.Fatalf("entry %d reads %+v, want %+v", i, got.Entries[i], want)
		}
	}
	var warnings string
	if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT warnings FROM %s WHERE schema_id = 9 AND generation = 2", TablesFor(registry).Generation)).Scan(&warnings); err != nil {
		t.Fatalf("read warnings: %v", err)
	}
	if warnings != "w1\nw2" {
		t.Fatalf("stored warnings = %q, want both, one per line", warnings)
	}
}

func TestLedgerWriteRefusesAStaleExpectation(t *testing.T) {
	pool, registry := ledgerFixture(t)
	if err := writeCommitted(t, pool, registry, record(0, "h1", eav(1, "a", forma.ValueTypeText))); err != nil {
		t.Fatalf("Write from none: %v", err)
	}
	for _, from := range []int64{0, 2} {
		err := writeCommitted(t, pool, registry, record(from, "hx", eav(1, "a", forma.ValueTypeText)))
		if !errors.Is(err, ErrGenerationConflict) {
			t.Fatalf("Write expecting %d while 1 is active = %v, want ErrGenerationConflict", from, err)
		}
	}
	if n, _, err := ReadPointer(context.Background(), pool, registry, 9); err != nil || n != 1 {
		t.Fatalf("a refused Write must leave generation 1 active, got %d (%v)", n, err)
	}
}

// TestLedgerConcurrentAppliesRecordOne: two applies judged against the same
// generation both try to record the next one. The second waits on the first's
// pointer row lock, then finds the pointer moved and records nothing.
func TestLedgerConcurrentAppliesRecordOne(t *testing.T) {
	pool, registry := ledgerFixture(t)
	ctx := context.Background()
	if err := writeCommitted(t, pool, registry, record(0, "h1", eav(1, "a", forma.ValueTypeSmallInt))); err != nil {
		t.Fatalf("Write from none: %v", err)
	}
	first, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = first.Rollback(ctx) }()
	if err := Write(ctx, first, registry, record(1, "h2", eav(1, "a", forma.ValueTypeInteger))); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	second := make(chan error, 1)
	go func() {
		second <- writeCommitted(t, pool, registry, record(1, "h3", eav(1, "a", forma.ValueTypeBigInt)))
	}()
	select {
	case err := <-second:
		t.Fatalf("the second apply must wait for the first, but returned %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if err := first.Commit(ctx); err != nil {
		t.Fatalf("commit first: %v", err)
	}
	if err := <-second; !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("second Write = %v, want ErrGenerationConflict", err)
	}
	got, _, err := ReadActive(ctx, pool, registry, 9)
	if err != nil || got.Number != 2 || got.Hash != "h2" {
		t.Fatalf("active = %+v (%v), want the first apply's generation 2", got, err)
	}
}

func TestGateReadsEverySchemaAndNamesEachRefusal(t *testing.T) {
	pool, registry := ledgerFixture(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (schema_name, schema_id) VALUES ('fresh', 11)", registry)); err != nil {
		t.Fatalf("register a second schema: %v", err)
	}
	if err := writeCommitted(t, pool, registry, record(0, "h1", bound(1, "a", forma.ValueTypeInteger, "integer_1"))); err != nil {
		t.Fatalf("Write: %v", err)
	}
	orders := Declaration{SchemaName: "orders", SchemaID: 9, Entries: []Entry{bound(1, "a", forma.ValueTypeInteger, "integer_1")}}
	fresh := Declaration{SchemaName: "fresh", SchemaID: 11, Entries: []Entry{eav(1, "x", forma.ValueTypeText)}}

	loaded, err := Gate(ctx, pool, registry, []Declaration{orders})
	if err != nil || loaded[9] != 1 {
		t.Fatalf("Gate on the active declaration = %v (%v), want schema 9 on generation 1", loaded, err)
	}
	stale := orders
	stale.Entries = []Entry{bound(1, "a", forma.ValueTypeSmallInt, "integer_1")}
	_, err = Gate(ctx, pool, registry, []Declaration{fresh, stale})
	if !errors.Is(err, ErrNotActive) {
		t.Fatalf("Gate = %v, want ErrNotActive", err)
	}
	for _, want := range []string{"schema orders (id 9)", "smallint bound to integer_1", "schema fresh (id 11)", "no baseline"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Gate error %q does not mention %q", err, want)
		}
	}
}

// TestGateNamesTheUpgradeStep: a registry from before the ledger fails with
// the upgrade's remediation, not a bare undefined-column error.
func TestGateNamesTheUpgradeStep(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Connect(t, ctx)
	registry := fmt.Sprintf("ledger_old_registry_%d", time.Now().UnixNano())
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+registry) })
	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (schema_name TEXT PRIMARY KEY, schema_id SMALLINT UNIQUE NOT NULL)", registry)); err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err := Gate(ctx, pool, registry, nil)
	if err == nil || !strings.Contains(err.Error(), "baseline-schema") || !strings.Contains(err.Error(), "init-db") {
		t.Fatalf("Gate on a registry without the ledger = %v, want the upgrade remediation", err)
	}
}
