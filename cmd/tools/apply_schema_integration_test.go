package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/schemaledger"
)

// ledgerTarget is an init-db target whose schemas init-db registered, with
// generation 1 recorded, ready for apply-schema.
type ledgerTarget struct {
	*initDBTarget
}

func newLedgerTarget(t *testing.T, attributes map[string]string) *ledgerTarget {
	t.Helper()
	tg := newInitDBTarget(t)
	for name, attrs := range attributes {
		writeLedgerSchema(t, tg.opts.schemaDir, name, attrs)
	}
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db: %v\n%s", err, out)
	}
	return &ledgerTarget{tg}
}

func writeLedgerSchema(t *testing.T, dir, name, attrs string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name+".json"), []byte(`{"type":"object"}`), 0o644); err != nil {
		t.Fatalf("write schema %s: %v", name, err)
	}
	if err := os.WriteFile(filepath.Join(dir, name+"_attributes.json"), []byte(attrs), 0o644); err != nil {
		t.Fatalf("write attributes %s: %v", name, err)
	}
}

func (lt *ledgerTarget) apply(t *testing.T, dryRun bool) (string, error) {
	t.Helper()
	var out bytes.Buffer
	opts := ledgerToolOptions{schemaTable: lt.opts.schemaTable, schemaDir: lt.opts.schemaDir, dryRun: dryRun}
	err := applySchemas(context.Background(), lt.pool, opts, &out)
	return out.String(), err
}

func (lt *ledgerTarget) active(t *testing.T, schemaID int16) schemaledger.Generation {
	t.Helper()
	gen, ok, err := schemaledger.ReadActive(context.Background(), lt.pool, lt.opts.schemaTable, schemaID)
	if err != nil || !ok {
		t.Fatalf("read active generation of schema %d: ok=%t err=%v", schemaID, ok, err)
	}
	return gen
}

func boundAttr(name string, id int, valueType, column string) string {
	return fmt.Sprintf(`"%s":{"attributeID":%d,"valueType":"%s","column_binding":{"col_name":"%s"}}`, name, id, valueType, column)
}

func TestApplySchemaRecordsAnAllowedWidening(t *testing.T) {
	lt := newLedgerTarget(t, map[string]string{"orders": "{" + boundAttr("qty", 1, "smallint", "smallint_01") + "}"})
	writeLedgerSchema(t, lt.opts.schemaDir, "orders", "{"+boundAttr("qty", 1, "integer", "smallint_01")+"}")

	out, err := lt.apply(t, true)
	if err != nil || !strings.Contains(out, "generation 1 to 2") || !strings.Contains(out, "Dry run") {
		t.Fatalf("dry run = %v\n%s", err, out)
	}
	if gen := lt.active(t, 100); gen.Number != 1 {
		t.Fatalf("a dry run recorded generation %d", gen.Number)
	}

	out, err = lt.apply(t, false)
	if err != nil || !strings.Contains(out, "Recorded generation 2 of schema orders") {
		t.Fatalf("apply = %v\n%s", err, out)
	}
	gen := lt.active(t, 100)
	if gen.Number != 2 || gen.Entries[0].ValueType != "integer" {
		t.Fatalf("active = %+v, want generation 2 with qty integer", gen)
	}

	out, err = lt.apply(t, false)
	if err != nil || !strings.Contains(out, "nothing to apply") {
		t.Fatalf("a second apply of the same files = %v\n%s", err, out)
	}
}

// TestApplySchemaRefusesAndRecordsNothing: a refused change in one schema
// records nothing for any schema, and the refusal names the schema, the
// attribute, both types and the remediation.
func TestApplySchemaRefusesAndRecordsNothing(t *testing.T) {
	lt := newLedgerTarget(t, map[string]string{
		"orders": "{" + boundAttr("price", 1, "numeric", "double_01") + "}",
		"visits": "{" + boundAttr("count", 1, "smallint", "smallint_01") + "}",
	})
	// A hand-edited attributes file: numeric to bigint narrows.
	writeLedgerSchema(t, lt.opts.schemaDir, "orders", "{"+boundAttr("price", 1, "bigint", "double_01")+"}")
	writeLedgerSchema(t, lt.opts.schemaDir, "visits", "{"+boundAttr("count", 1, "integer", "smallint_01")+"}")

	out, err := lt.apply(t, false)
	if err == nil {
		t.Fatalf("apply of a narrowing must fail\n%s", out)
	}
	for _, want := range []string{"schema orders (id 100)", "price (id 1)", "numeric bound to double_01", "bigint bound to double_01", "#730"} {
		if !strings.Contains(out, want) {
			t.Fatalf("refusal output lacks %q:\n%s", want, out)
		}
	}
	for _, id := range []int16{100, 101} {
		if gen := lt.active(t, id); gen.Number != 1 {
			t.Fatalf("schema %d moved to generation %d although the apply was refused", id, gen.Number)
		}
	}
}

func TestApplySchemaStoresTheRoundingWarning(t *testing.T) {
	lt := newLedgerTarget(t, map[string]string{"orders": "{" + boundAttr("total", 1, "bigint", "bigint_01") + "}"})
	writeLedgerSchema(t, lt.opts.schemaDir, "orders", "{"+boundAttr("total", 1, "numeric", "bigint_01")+"}")

	out, err := lt.apply(t, false)
	if err != nil || !strings.Contains(out, "WARNING:") || !strings.Contains(out, "bigint_01") {
		t.Fatalf("apply = %v, want the bigint_01 warning printed\n%s", err, out)
	}
	var warnings string
	query := fmt.Sprintf("SELECT warnings FROM %s WHERE schema_id = 100 AND generation = 2", schemaledger.TablesFor(lt.opts.schemaTable).Generation)
	if err := lt.pool.QueryRow(context.Background(), query).Scan(&warnings); err != nil {
		t.Fatalf("read warnings: %v", err)
	}
	if !strings.Contains(warnings, "±2^53") {
		t.Fatalf("stored warnings = %q, want the rounding warning", warnings)
	}
}

func TestApplySchemaRefusesASchemaWithNoBaseline(t *testing.T) {
	lt := newLedgerTarget(t, map[string]string{"orders": "{" + boundAttr("qty", 1, "smallint", "smallint_01") + "}"})
	reset := fmt.Sprintf("UPDATE %s SET %s = NULL", schemaledger.TablesFor(lt.opts.schemaTable).Registry, schemaledger.PointerColumn)
	if _, err := lt.pool.Exec(context.Background(), reset); err != nil {
		t.Fatalf("clear the pointer: %v", err)
	}
	out, err := lt.apply(t, false)
	if err == nil || !strings.Contains(out, "run baseline-schema first") {
		t.Fatalf("apply on a schema with no baseline = %v, want a refusal naming baseline-schema\n%s", err, out)
	}
}
