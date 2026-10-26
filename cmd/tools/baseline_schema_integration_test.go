package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"github.com/lychee-technology/forma/internal/widthaudit"
)

// forgetLedger puts the target back in the state of a deployment upgraded to
// the ledger: its schemas are registered, the ledger exists, and no schema
// has a generation.
func (lt *ledgerTarget) forgetLedger(t *testing.T) {
	t.Helper()
	tables := schemaledger.TablesFor(lt.opts.schemaTable)
	for _, stmt := range []string{
		"DELETE FROM " + tables.Attribute,
		"DELETE FROM " + tables.Generation,
		fmt.Sprintf("UPDATE %s SET %s = NULL", tables.Registry, schemaledger.PointerColumn),
	} {
		if _, err := lt.pool.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
}

func (lt *ledgerTarget) baseline(t *testing.T, audit parquetAuditFn) (string, error) {
	t.Helper()
	var out bytes.Buffer
	opts := baselineOptions{
		ledgerToolOptions: ledgerToolOptions{schemaTable: lt.opts.schemaTable, schemaDir: lt.opts.schemaDir},
		tables:            widthaudit.Tables{EAV: lt.opts.eavTable, EntityMain: lt.opts.entityMain, ChangeLog: lt.opts.changeLog},
		auditParquet:      audit,
	}
	err := baselineSchemas(context.Background(), lt.pool, opts, &out)
	return out.String(), err
}

func (lt *ledgerTarget) pointer(t *testing.T, schemaID int16) (int64, bool) {
	t.Helper()
	n, ok, err := schemaledger.ReadPointer(context.Background(), lt.pool, lt.opts.schemaTable, schemaID)
	if err != nil {
		t.Fatalf("read pointer: %v", err)
	}
	return n, ok
}

const eavIntegerAttrs = `{"qty":{"attributeID":1,"valueType":"integer"}}`

func TestBaselineSchemaSeedsACleanSchema(t *testing.T) {
	lt := newLedgerTarget(t, map[string]string{"orders": eavIntegerAttrs})
	lt.forgetLedger(t)
	insert := fmt.Sprintf("INSERT INTO %s (schema_id, row_id, attr_id, value_numeric) VALUES (100, gen_random_uuid(), 1, 1001)", quoteIdentifier(lt.opts.eavTable))
	if _, err := lt.pool.Exec(context.Background(), insert); err != nil {
		t.Fatalf("seed a row: %v", err)
	}

	out, err := lt.baseline(t, nil)
	if err != nil || !strings.Contains(out, "Recorded generation 1 of schema orders") {
		t.Fatalf("baseline = %v\n%s", err, out)
	}
	if n, ok := lt.pointer(t, 100); !ok || n != 1 {
		t.Fatalf("pointer = %d (%t), want generation 1", n, ok)
	}
	out, err = lt.baseline(t, nil)
	if err != nil || !strings.Contains(out, "nothing to baseline") {
		t.Fatalf("a second baseline must leave the active generation alone: %v\n%s", err, out)
	}
}

func TestBaselineSchemaRefusesAPostgresValueOutsideTheDeclaration(t *testing.T) {
	lt := newLedgerTarget(t, map[string]string{"orders": eavIntegerAttrs})
	lt.forgetLedger(t)
	insert := fmt.Sprintf("INSERT INTO %s (schema_id, row_id, attr_id, value_numeric) VALUES (100, gen_random_uuid(), 1, 1001.5)", quoteIdentifier(lt.opts.eavTable))
	if _, err := lt.pool.Exec(context.Background(), insert); err != nil {
		t.Fatalf("seed a row: %v", err)
	}
	out, err := lt.baseline(t, nil)
	if err == nil || !strings.Contains(out, "attribute qty (id 1) declared integer") || !strings.Contains(out, "1001.5") {
		t.Fatalf("baseline = %v, want the 1001.5 row refused\n%s", err, out)
	}
	if _, ok := lt.pointer(t, 100); ok {
		t.Fatal("a refused baseline recorded a generation")
	}
}

// TestBaselineSchemaRefusesAParquetValueTheFooterHides is the acceptance
// case: PostgreSQL is clean, and a live DOUBLE object, the export type of an
// EAV-only integer, still holds 1001.5.
func TestBaselineSchemaRefusesAParquetValueTheFooterHides(t *testing.T) {
	lt := newLedgerTarget(t, map[string]string{"orders": eavIntegerAttrs})
	lt.forgetLedger(t)
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = duck.Close() })
	object := filepath.Join(t.TempDir(), "delta.parquet")
	copyStmt := fmt.Sprintf("COPY (SELECT gen_random_uuid() AS row_id, CAST(100 AS BIGINT) AS changed_at, CAST(0 AS BIGINT) AS deleted_at, "+
		"CAST(50 AS BIGINT) AS ltbase_created_at, CAST(1001.5 AS DOUBLE) AS qty) TO '%s' (FORMAT PARQUET)", object)
	if _, err := duck.Exec(copyStmt); err != nil {
		t.Fatalf("write object: %v", err)
	}
	audit := func(ctx context.Context, schemaID int16, cache forma.SchemaAttributeCache) ([]federated.DomainFinding, error) {
		return federated.AuditParquetDomain(ctx, duck, []string{object}, nil, cache)
	}

	out, err := lt.baseline(t, audit)
	if err == nil || !strings.Contains(out, "object "+object) || !strings.Contains(out, "1001.5") {
		t.Fatalf("baseline = %v, want the object refused\n%s", err, out)
	}
	if _, ok := lt.pointer(t, 100); ok {
		t.Fatal("a refused baseline recorded a generation")
	}
}
