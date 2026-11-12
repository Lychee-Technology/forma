package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma/internal/rowguard"
	"github.com/lychee-technology/forma/internal/schemalease"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"github.com/lychee-technology/forma/internal/sqlutil"
	"github.com/lychee-technology/forma/internal/textcollation"
	"github.com/lychee-technology/forma/internal/uniquekey"
)

// catalogColumn is one collatable column of a table init-db created, as the
// catalog describes it: the column's own collation, not the database's.
type catalogColumn struct {
	table, column, collation string
}

// collatableColumns reads every collatable column of the tables init-db
// created, independently of textcollation's query: this one filters nothing.
func (tg *initDBTarget) collatableColumns(t *testing.T) []catalogColumn {
	t.Helper()
	var found []catalogColumn
	for _, table := range ownedTables(tg.opts) {
		rows, err := tg.pool.Query(context.Background(), `
			SELECT a.attname::text, quote_ident(n.nspname) || '.' || quote_ident(c.collname)
			FROM pg_attribute a
			JOIN pg_collation c ON c.oid = a.attcollation
			JOIN pg_namespace n ON n.oid = c.collnamespace
			WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped
			ORDER BY a.attnum`, table)
		if err != nil {
			t.Fatalf("read the collatable columns of %s: %v", table, err)
		}
		col := catalogColumn{table: table}
		if _, err := pgx.ForEachRow(rows, []any{&col.column, &col.collation}, func() error {
			found = append(found, col)
			return nil
		}); err != nil {
			t.Fatalf("scan the collatable columns of %s: %v", table, err)
		}
	}
	return found
}

// wantTextColumns is every text column init-db creates, by table.
func (tg *initDBTarget) wantTextColumns() map[string][]string {
	ledger := schemaledger.TablesFor(tg.opts.schemaTable)
	return map[string][]string{
		quoteIdentifier(tg.opts.schemaTable): {"schema_name"},
		quoteIdentifier(tg.opts.entityMain): {"text_01", "text_02", "text_03", "text_04", "text_05", "text_06", "text_07",
			"text_08", "text_09", "text_10", "ltbase_created_by", "ltbase_updated_by", "ltbase_deleted_by"},
		quoteIdentifier(tg.opts.eavTable):                         {"array_indices", "value_text"},
		quoteIdentifier(uniquekey.KeyTable(tg.opts.entityMain)):   {"key_value"},
		quoteIdentifier(uniquekey.StateTable(tg.opts.entityMain)): {"attributes_hash", "storage_hash"},
		ledger.Generation: {"content_hash", "recorded_by", "warnings"},
		ledger.Attribute:  {"attr_name", "value_type", "items_type", "column_name", "encoding"},
	}
}

// databaseCollation answers the database's default collation, which every
// assertion below must hold regardless of.
func (tg *initDBTarget) databaseCollation(t *testing.T) string {
	t.Helper()
	var collate string
	if err := tg.pool.QueryRow(context.Background(),
		`SELECT datcollate::text FROM pg_database WHERE datname = current_database()`).Scan(&collate); err != nil {
		t.Fatalf("read the database collation: %v", err)
	}
	return collate
}

// #816's first acceptance criterion: a fresh init-db declares C on every text
// column of every table it creates, which the catalog shows on the column
// itself (pg_catalog."C"), not inherited from the database (pg_catalog."default",
// another OID even in a database whose default is C). The declared index over
// text_01 inherits it, so a rerun's verification still finds it present.
func TestInitDBCreatesEveryTextColumnUnderC(t *testing.T) {
	tg := newInitDBTarget(t)
	tg.writeOutboxAndTimer(t, "due")
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db: %v\n%s", err, out)
	}
	t.Logf("database default collation: %s", tg.databaseCollation(t))

	got := map[string][]string{}
	for _, c := range tg.collatableColumns(t) {
		if c.collation != `pg_catalog."C"` {
			t.Errorf("%s.%s has collation %s, want pg_catalog.\"C\"", c.table, c.column, c.collation)
		}
		got[c.table] = append(got[c.table], c.column)
	}
	want := tg.wantTextColumns()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("collatable columns:\n got %v\nwant %v", got, want)
	}
	for _, table := range []string{rowguard.Table(tg.opts.entityMain), schemalease.Table(tg.opts.entityMain), tg.opts.changeLog} {
		if cols := got[quoteIdentifier(table)]; cols != nil {
			t.Errorf("%s has collatable columns %v, which no test expects", table, cols)
		}
	}

	name, _ := tg.wantDef("text_01", "bigint_01")
	var keyCollation string
	if err := tg.pool.QueryRow(context.Background(), `
		SELECT quote_ident(n.nspname) || '.' || quote_ident(c.collname)
		FROM pg_index i JOIN pg_collation c ON c.oid = i.indcollation[1] JOIN pg_namespace n ON n.oid = c.collnamespace
		WHERE i.indexrelid = to_regclass($1)`, quoteIdentifier(name)).Scan(&keyCollation); err != nil {
		t.Fatalf("read the key collation of %s: %v", name, err)
	}
	if keyCollation != `pg_catalog."C"` {
		t.Fatalf("declared index %s keys text_01 under %s, want the column's pg_catalog.\"C\"", name, keyCollation)
	}
	out, err := tg.run(t)
	if err != nil {
		t.Fatalf("second init-db: %v\n%s", err, out)
	}
	if !strings.Contains(out, "Declared indexes: 0 created, 1 already present, 0 unreferenced\n") {
		t.Fatalf("the rerun did not verify the declared index as present:\n%s", out)
	}
}

// storageFile is the storage file of a table or index, which a table rewrite
// or an index rebuild replaces, and whether the relation is an index that
// keys a collatable column.
type storageFile struct {
	node     uint32
	keysText bool
}

// relfilenodes answers the storage file of each of tables and of every index
// on them, by relation name.
func (tg *initDBTarget) relfilenodes(t *testing.T, tables ...string) map[string]storageFile {
	t.Helper()
	files := map[string]storageFile{}
	for _, table := range tables {
		rows, err := tg.pool.Query(context.Background(), `
			SELECT c.relname::text, c.relfilenode::oid::int8, false FROM pg_class c WHERE c.oid = to_regclass($1)
			UNION ALL
			SELECT ic.relname::text, ic.relfilenode::oid::int8,
			       EXISTS (SELECT 1 FROM unnest(i.indcollation::oid[]) AS k(coll) WHERE k.coll <> 0)
			FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid
			WHERE i.indrelid = to_regclass($1)`, table)
		if err != nil {
			t.Fatalf("read the relfilenodes of %s: %v", table, err)
		}
		var name string
		var node int64
		var keysText bool
		if _, err := pgx.ForEachRow(rows, []any{&name, &node, &keysText}, func() error {
			files[name] = storageFile{node: uint32(node), keysText: keysText}
			return nil
		}); err != nil {
			t.Fatalf("scan the relfilenodes of %s: %v", table, err)
		}
	}
	return files
}

// legacyTarget is a deployment whose tables predate the text collation
// contract, holding one entity_main row and one eav_data row.
type legacyTarget struct {
	*initDBTarget
	main, eav string
	// columns is every collatable column, as it was under C before it was
	// moved back to the database default.
	columns []catalogColumn
}

// newLegacyTarget models tables created before the contract by moving every
// text column of a fresh deployment back to the database default, which
// rebuilds their indexes under it.
func newLegacyTarget(t *testing.T) *legacyTarget {
	t.Helper()
	tg := newInitDBTarget(t)
	tg.writeOutboxAndTimer(t, "due")
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db: %v\n%s", err, out)
	}
	lt := &legacyTarget{initDBTarget: tg, main: quoteIdentifier(tg.opts.entityMain), eav: quoteIdentifier(tg.opts.eavTable)}
	lt.exec(t, fmt.Sprintf(`INSERT INTO %s (ltbase_schema_id, ltbase_row_id, text_01, bigint_01, ltbase_created_at, ltbase_updated_at)
		VALUES (100, '00000000-0000-4000-8000-000000000001', 'Zebra é', 1, 1, 1)`, lt.main))
	lt.exec(t, fmt.Sprintf(`INSERT INTO %s (schema_id, row_id, attr_id, array_indices, value_text)
		VALUES (100, '00000000-0000-4000-8000-000000000001', 7, '0', 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa')`, lt.eav))
	lt.columns = tg.collatableColumns(t)
	for _, c := range lt.columns {
		lt.exec(t, fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s TYPE text COLLATE "default"`, c.table, pgx.Identifier{c.column}.Sanitize()))
	}
	return lt
}

func (lt *legacyTarget) exec(t *testing.T, sql string) {
	t.Helper()
	if _, err := lt.pool.Exec(context.Background(), sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// #816's drift criterion: tables created before the contract make a rerun of
// init-db fail, naming each column, without changing anything. Running the
// statements its error prints migrates the columns without rewriting a
// table, rebuilds their indexes, declared ones included, and the next rerun
// passes and still finds the declared index present.
func TestInitDBRefusesTextOutsideCAndItsMigrationRestoresTheContract(t *testing.T) {
	ctx := context.Background()
	lt := newLegacyTarget(t)
	tg, main, eav, legacy := lt.initDBTarget, lt.main, lt.eav, lt.columns
	before := tg.relfilenodes(t, main, eav)
	tg.log.reset()

	out, err := tg.run(t)
	var drift *textcollation.DriftError
	if !errors.As(err, &drift) {
		t.Fatalf("init-db over text outside C: err = %v, want a *textcollation.DriftError\n%s", err, out)
	}
	if len(drift.Violations) != len(legacy) {
		t.Fatalf("init-db reported %d columns, want all %d: %v", len(drift.Violations), len(legacy), drift.Violations)
	}
	for _, want := range []string{
		fmt.Sprintf(`table %s column text_01 (text) has collation pg_catalog."default", expected pg_catalog."C"`, main),
		fmt.Sprintf(`table %s column value_text (text) has collation pg_catalog."default", expected pg_catalog."C"`, eav),
		textcollation.MigrationDoc,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal does not say %q:\n%v", want, err)
		}
	}
	for _, verb := range []string{"ALTER COLUMN", "DROP", "REINDEX", "CREATE INDEX CONCURRENTLY"} {
		if sent := tg.log.matching(verb); len(sent) != 0 {
			t.Fatalf("the refused run sent %s statements: %+v", verb, sent)
		}
	}
	if after := tg.relfilenodes(t, main, eav); fmt.Sprint(after) != fmt.Sprint(before) {
		t.Fatalf("the refused run rewrote storage:\nbefore %v\n after %v", before, after)
	}
	for _, c := range tg.collatableColumns(t) {
		if c.collation != `pg_catalog."default"` {
			t.Fatalf("the refused run changed %s.%s to %s", c.table, c.column, c.collation)
		}
	}

	for _, stmt := range drift.MigrationStatements() {
		lt.exec(t, stmt)
	}
	migrated := tg.relfilenodes(t, main, eav)
	rebuilt := 0
	for name, file := range before {
		if replaced := migrated[name].node != file.node; replaced != file.keysText {
			t.Errorf("%s: storage replaced = %v, want only the indexes keying text rebuilt, tables and other indexes kept", name, replaced)
		} else if replaced {
			rebuilt++
		}
	}
	// text_01..03's covering indexes, eav_data's text index and primary key,
	// and the declared index over text_01.
	if rebuilt != 6 {
		t.Errorf("the migration rebuilt %d indexes, want 6", rebuilt)
	}
	var text, value string
	if err := tg.pool.QueryRow(ctx, fmt.Sprintf(`SELECT m.text_01, e.value_text FROM %s m JOIN %s e ON e.row_id = m.ltbase_row_id`, main, eav)).
		Scan(&text, &value); err != nil || text != "Zebra é" || value != "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" {
		t.Fatalf("the migrated rows read %q, %q (%v)", text, value, err)
	}

	out, err = tg.run(t)
	if err != nil {
		t.Fatalf("init-db after the migration: %v\n%s", err, out)
	}
	if !strings.Contains(out, "Declared indexes: 0 created, 1 already present, 0 unreferenced\n") {
		t.Fatalf("init-db no longer finds the migrated declared index present:\n%s", out)
	}
	for _, c := range tg.collatableColumns(t) {
		if c.collation != `pg_catalog."C"` {
			t.Errorf("after the migration %s.%s has collation %s", c.table, c.column, c.collation)
		}
	}
}

// A deployment from before #816 that also predates a table init-db now
// creates gets none of it from a refused run: the table transaction rolls
// back, so the rerun after the migration is the one that creates it.
func TestInitDBRefusalCreatesNothing(t *testing.T) {
	ctx := context.Background()
	tg := newInitDBTarget(t)
	legacyMain := strings.ReplaceAll(entityMainDDL(quoteIdentifier(tg.opts.entityMain)), ` COLLATE "C"`, "")
	if _, err := tg.pool.Exec(ctx, legacyMain); err != nil {
		t.Fatalf("create a pre-#816 entity_main: %v", err)
	}
	out, err := tg.run(t)
	var drift *textcollation.DriftError
	if !errors.As(err, &drift) || len(drift.Violations) != 13 {
		t.Fatalf("init-db over a pre-#816 entity_main: err = %v, want its 13 text columns refused\n%s", err, out)
	}
	ledger := sqlutil.SuffixedIdentifier(tg.opts.schemaTable, schemaledger.GenerationTableSuffix)
	for _, table := range []string{tg.opts.schemaTable, tg.opts.eavTable, ledger} {
		var exists bool
		if err := tg.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, quoteIdentifier(table)).Scan(&exists); err != nil || exists {
			t.Fatalf("the refused run left table %s behind (exists=%v, err=%v)", table, exists, err)
		}
	}
}
