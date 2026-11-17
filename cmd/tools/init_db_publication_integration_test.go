package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/publication"
)

// publicationTableState is what a run of init-db may change about one
// publication table: whether it exists, its storage file and its row count.
type publicationTableState struct {
	exists bool
	node   int64
	rows   int64
}

// publicationTables reads the state of the target's two publication tables.
func (tg *initDBTarget) publicationTables(t *testing.T) map[string]publicationTableState {
	t.Helper()
	ctx := context.Background()
	tables := publication.TablesFor(tg.opts.entityMain)
	states := map[string]publicationTableState{}
	for _, table := range []string{tables.Record, tables.Generation} {
		var st publicationTableState
		if err := tg.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL, COALESCE((SELECT relfilenode::oid::int8 FROM pg_class WHERE oid = to_regclass($1)), 0)`,
			table).Scan(&st.exists, &st.node); err != nil {
			t.Fatalf("read the catalog of %s: %v", table, err)
		}
		if st.exists {
			if err := tg.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s`, table)).Scan(&st.rows); err != nil {
				t.Fatalf("count the rows of %s: %v", table, err)
			}
		}
		states[table] = st
	}
	return states
}

// requirePublicationTablesEmpty fails unless both tables exist and hold no
// row: registering schemas writes no record before #896.
func (tg *initDBTarget) requirePublicationTablesEmpty(t *testing.T, when string) map[string]publicationTableState {
	t.Helper()
	states := tg.publicationTables(t)
	for table, st := range states {
		if !st.exists || st.rows != 0 {
			t.Fatalf("%s: %s exists = %v with %d rows, want present and empty", when, table, st.exists, st.rows)
		}
	}
	return states
}

// #895's acceptance: a fresh init-db creates both publication tables, a
// deployment initialized before them gains them on its next init-db, and a
// further run changes neither. No run writes a record, though each one
// registers schemas.
func TestInitDBCreatesThePublicationTablesOnceAndWritesNoRecord(t *testing.T) {
	tg := newInitDBTarget(t)
	tg.writeOutboxAndTimer(t, "due")
	out, err := tg.run(t)
	if err != nil {
		t.Fatalf("init-db on a fresh database: %v\n%s", err, out)
	}
	tables := publication.TablesFor(tg.opts.entityMain)
	for _, want := range []string{
		"Created publication record table: " + strings.Trim(tables.Record, `"`) + "\n",
		"Created publication generation table: " + strings.Trim(tables.Generation, `"`) + "\n",
		"Registered schema, name: outbox_message",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("init-db output lacks %q:\n%s", want, out)
		}
	}
	tg.requirePublicationTablesEmpty(t, "fresh init-db")

	// A deployment from before #895: every other table and a registered
	// schema, but no publication tables.
	for _, table := range []string{tables.Generation, tables.Record} {
		if _, err := tg.pool.Exec(context.Background(), "DROP TABLE "+table); err != nil {
			t.Fatalf("drop %s: %v", table, err)
		}
	}
	tg.writeSchema(t, "visit", map[string]string{"page": "text_02"}, nil)
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db on a deployment without the publication tables: %v\n%s", err, out)
	}
	upgraded := tg.requirePublicationTablesEmpty(t, "init-db over an existing deployment")

	tg.log.reset()
	if out, err := tg.run(t); err != nil {
		t.Fatalf("second init-db: %v\n%s", err, out)
	}
	if rerun := tg.requirePublicationTablesEmpty(t, "second init-db"); fmt.Sprint(rerun) != fmt.Sprint(upgraded) {
		t.Fatalf("the rerun changed the publication tables:\nbefore %v\n after %v", upgraded, rerun)
	}
	for _, sent := range tg.log.matching("_publication") {
		if !strings.HasPrefix(sent.sql, "CREATE TABLE IF NOT EXISTS ") && !strings.Contains(sent.sql, "pg_attribute") {
			t.Errorf("init-db sent a statement over the publication tables other than their DDL and the collation check: %s", sent.sql)
		}
	}
}
