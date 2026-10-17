package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/uniquekey"
)

// init-db and build-unique-keys against a real PostgreSQL (#651): init-db
// creates the key tables and builds the keys of a schema without rows, and
// build-unique-keys builds a key over existing rows or lists the duplicates
// that stop it.

// writeKeyedSchema writes name.json with one text attribute, handle, and a
// unique key over it.
func (tg *initDBTarget) writeKeyedSchema(t *testing.T, name string) {
	t.Helper()
	doc := map[string]any{
		"type":                "object",
		"properties":          map[string]any{"handle": map[string]any{"type": "string"}},
		"x-forma-unique-keys": []any{map[string]any{"id": 1, "name": "handle", "attributes": []any{"handle"}}},
	}
	attributes := map[string]any{"handle": map[string]any{"attributeID": 1, "valueType": "text"}}
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

// insertHandleRow writes a row with handle straight into the tables, as a
// server that predates the key would.
func (tg *initDBTarget) insertHandleRow(t *testing.T, schemaID int16, rowID, handle string) {
	t.Helper()
	ctx := context.Background()
	if _, err := tg.pool.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s (ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at) VALUES ($1, $2, 1, 1)",
		quoteIdentifier(tg.opts.entityMain)), schemaID, rowID); err != nil {
		t.Fatalf("insert main row: %v", err)
	}
	if _, err := tg.pool.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s (schema_id, row_id, attr_id, value_text) VALUES ($1, $2, 1, $3)",
		quoteIdentifier(tg.opts.eavTable)), schemaID, rowID, handle); err != nil {
		t.Fatalf("insert eav row: %v", err)
	}
}

func (tg *initDBTarget) count(t *testing.T, table string) int {
	t.Helper()
	var n int
	if err := tg.pool.QueryRow(context.Background(), "SELECT count(*) FROM "+quoteIdentifier(table)).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func (tg *initDBTarget) buildKeys(t *testing.T, schema string, rebuild bool) (string, error) {
	t.Helper()
	var err error
	stdout, _ := captureStdStreams(t, func() {
		err = buildUniqueKeys(context.Background(), buildUniqueKeysOptions{
			schema: schema, schemaTable: tg.opts.schemaTable, eavTable: tg.opts.eavTable,
			entityMain: tg.opts.entityMain, schemaDir: tg.opts.schemaDir, rebuild: rebuild,
		}, forma.DatabaseConfig{}, os.Stdout)
	})
	return stdout, err
}

func TestInitDBCreatesKeyTablesAndBuildsTheKeysOfAnEmptySchema(t *testing.T) {
	tg := newInitDBTarget(t)
	tg.writeKeyedSchema(t, "account")

	out, err := tg.run(t)
	if err != nil {
		t.Fatalf("init-db: %v\n%s", err, out)
	}
	for _, want := range []string{
		"Created unique key table: " + uniquekey.KeyTable(tg.opts.entityMain),
		"Created unique key state table: " + uniquekey.StateTable(tg.opts.entityMain),
		"Built unique key handle of schema account (no rows to scan)",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("init-db output lacks %q:\n%s", want, out)
		}
	}
	if n := tg.count(t, uniquekey.StateTable(tg.opts.entityMain)); n != 1 {
		t.Errorf("state table holds %d rows, want the one built key", n)
	}

	// A re-run finds the key built and reports nothing to do.
	out, err = tg.run(t)
	if err != nil || strings.Contains(out, "Built unique key") || strings.Contains(out, "needs a build") {
		t.Errorf("init-db re-run: err %v, output:\n%s", err, out)
	}
}

func TestBuildUniqueKeysCommandBuildsOrListsDuplicates(t *testing.T) {
	tg := newInitDBTarget(t)
	tg.writeKeyedSchema(t, "account")
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db: %v\n%s", err, out)
	}
	// Drop the state init-db built, as for a key added to a schema with rows.
	if _, err := tg.pool.Exec(context.Background(), "DELETE FROM "+quoteIdentifier(uniquekey.StateTable(tg.opts.entityMain))); err != nil {
		t.Fatal(err)
	}
	const first, second = "01900000-0000-7000-8000-000000000001", "01900000-0000-7000-8000-000000000002"
	tg.insertHandleRow(t, firstSchemaID, first, "dup")
	tg.insertHandleRow(t, firstSchemaID, second, "dup")

	out, err := tg.run(t)
	if err != nil || !strings.Contains(out, "Unique key needs a build") || !strings.Contains(out, "--schema account --key handle") {
		t.Errorf("init-db over a schema with rows: err %v, output:\n%s", err, out)
	}

	_, err = tg.buildKeys(t, "account", false)
	if err == nil || !strings.Contains(err.Error(), first) || !strings.Contains(err.Error(), second) || !strings.Contains(err.Error(), "unique key handle") {
		t.Fatalf("build over duplicates: err %v, want the duplicate rows listed", err)
	}
	if n := tg.count(t, uniquekey.KeyTable(tg.opts.entityMain)); n != 0 {
		t.Errorf("a refused build wrote %d entries", n)
	}

	if _, err := tg.pool.Exec(context.Background(), fmt.Sprintf("UPDATE %s SET value_text = 'unique' WHERE row_id = $1",
		quoteIdentifier(tg.opts.eavTable)), second); err != nil {
		t.Fatal(err)
	}
	out, err = tg.buildKeys(t, "account", false)
	if err != nil || !strings.Contains(out, "Built unique key handle of schema account: 2 entries") {
		t.Fatalf("build: err %v, output:\n%s", err, out)
	}
	if n := tg.count(t, uniquekey.KeyTable(tg.opts.entityMain)); n != 2 {
		t.Errorf("key table holds %d entries, want 2", n)
	}

	// A row written past the key, as by a writer that predates it, is
	// entered only by a rebuild.
	tg.insertHandleRow(t, firstSchemaID, "01900000-0000-7000-8000-000000000003", "late")
	if out, err = tg.buildKeys(t, "account", false); err != nil || !strings.Contains(out, "is already built") {
		t.Fatalf("build of a built key: err %v, output:\n%s", err, out)
	}
	out, err = tg.buildKeys(t, "account", true)
	if err != nil || !strings.Contains(out, "Rebuilt unique key handle of schema account: 3 entries") {
		t.Fatalf("rebuild: err %v, output:\n%s", err, out)
	}
	if n := tg.count(t, uniquekey.KeyTable(tg.opts.entityMain)); n != 3 {
		t.Errorf("key table holds %d entries after the rebuild, want 3", n)
	}
}

// PostgreSQL truncates an identifier past 63 bytes. A long but valid
// entity_main name still gets a key table, a state table and a row index of
// its own, whether or not a schema declares a key: a 52-byte name used to
// truncate its state table onto its key table, which failed init-db even for
// unkeyed schemas, and no name derived from a 63-byte one fits unshortened.
func TestInitDBCreatesKeyTablesOfALongEntityMainName(t *testing.T) {
	for _, length := range []int{52, 63} {
		for _, keyed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d bytes keyed=%v", length, keyed), func(t *testing.T) {
				tg := newInitDBTarget(t)
				tg.opts.entityMain += strings.Repeat("x", length-len(tg.opts.entityMain))
				if keyed {
					tg.writeKeyedSchema(t, "account")
				} else {
					tg.writeSchema(t, "account", map[string]string{"handle": "text_01"}, nil)
				}
				for run := 1; run <= 2; run++ {
					if out, err := tg.run(t); err != nil {
						t.Fatalf("init-db run %d: %v\n%s", run, err, out)
					}
				}
				tg.requireKeyRelations(t)
				if !keyed {
					return
				}
				if n := tg.count(t, uniquekey.StateTable(tg.opts.entityMain)); n != 1 {
					t.Errorf("state table holds %d rows, want the one built key", n)
				}
				tg.insertHandleRow(t, firstSchemaID, "01900000-0000-7000-8000-000000000001", "late")
				if out, err := tg.buildKeys(t, "account", true); err != nil {
					t.Fatalf("rebuild: %v\n%s", err, out)
				}
				if n := tg.count(t, uniquekey.KeyTable(tg.opts.entityMain)); n != 1 {
					t.Errorf("key table holds %d entries after the rebuild, want 1", n)
				}
			})
		}
	}
}

// requireKeyRelations fails unless the key table, the state table and the
// row index each exist as a relation of their own, the index on the key
// table.
func (tg *initDBTarget) requireKeyRelations(t *testing.T) {
	t.Helper()
	keyTable, stateTable := uniquekey.KeyTable(tg.opts.entityMain), uniquekey.StateTable(tg.opts.entityMain)
	var keyOID, stateOID, indexedOID *uint32
	if err := tg.pool.QueryRow(context.Background(), `SELECT to_regclass($1)::oid, to_regclass($2)::oid,
		(SELECT indrelid FROM pg_index WHERE indexrelid = to_regclass($3))`,
		keyTable, stateTable, uniquekey.RowIndex(tg.opts.entityMain)).Scan(&keyOID, &stateOID, &indexedOID); err != nil {
		t.Fatalf("look up the key relations: %v", err)
	}
	switch {
	case keyOID == nil || stateOID == nil:
		t.Fatalf("key table %s (%v) or state table %s (%v) is missing", keyTable, keyOID, stateTable, stateOID)
	case *keyOID == *stateOID:
		t.Fatalf("key table %s and state table %s are one relation", keyTable, stateTable)
	case indexedOID == nil || *indexedOID != *keyOID:
		t.Fatalf("row index %s is missing or not on key table %s", uniquekey.RowIndex(tg.opts.entityMain), keyTable)
	}
}
