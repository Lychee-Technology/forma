package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/testdb"
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

// requireRefusedBeforeDDL fails unless the last init-db run returned an
// error naming each of wants and sent nothing but the name resolution: no
// transaction, no DDL, so no relation changed.
func (tg *initDBTarget) requireRefusedBeforeDDL(t *testing.T, out string, err error, wants ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("init-db accepted a publication table name another table resolves to:\n%s", out)
	}
	for _, want := range wants {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("init-db's refusal %q lacks %q", err, want)
		}
	}
	if strings.Contains(out, "Created") {
		t.Errorf("init-db announced tables before refusing the names:\n%s", out)
	}
	for _, sent := range tg.log.matching("") {
		if sent.sql != resolveTablesSQL {
			t.Errorf("init-db sent a statement other than the name resolution before refusing the names: %s", sent.sql)
		}
	}
	tg.log.reset()
}

// createSchema creates a schema of the test's own, dropped with everything
// in it when the test ends.
func (tg *initDBTarget) createSchema(t *testing.T, suffix string) string {
	t.Helper()
	schema := strings.TrimSuffix(tg.opts.entityMain, "_entity_main") + suffix
	if _, err := tg.pool.Exec(context.Background(), "CREATE SCHEMA "+quoteIdentifier(schema)); err != nil {
		t.Fatalf("create schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		if _, err := tg.pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+quoteIdentifier(schema)+" CASCADE"); err != nil {
			t.Logf("drop schema %s: %v", schema, err)
		}
	})
	return schema
}

// useSearchPath makes init-db's connections resolve unqualified names
// through searchPath.
func (tg *initDBTarget) useSearchPath(searchPath string) {
	toolPostgresPoolFn = func(ctx context.Context, _ forma.DatabaseConfig) (*pgxpool.Pool, error) {
		cfg, err := pgxpool.ParseConfig(testdb.ResolveDSN())
		if err != nil {
			return nil, err
		}
		cfg.ConnConfig.Tracer = tg.log
		cfg.ConnConfig.RuntimeParams["search_path"] = searchPath
		return pgxpool.NewWithConfig(ctx, cfg)
	}
}

// A change log configured under the publication record table's derived name
// would make CREATE TABLE IF NOT EXISTS skip the record table, and init-db
// would announce a record table that has the change log's columns. init-db
// refuses the names before its table transaction, so nothing is created
// (R1-F2 of #897's review).
func TestInitDBRefusesAChangeLogNamedLikeThePublicationRecord(t *testing.T) {
	tg := newInitDBTarget(t)
	tg.writeOutboxAndTimer(t, "due")
	tg.opts.changeLog = strings.Trim(publication.TablesFor(tg.opts.entityMain).Record, `"`)
	out, err := tg.run(t)
	tg.requireRefusedBeforeDDL(t, out, err, "the publication record table", "the change log table, which --change-log-table names")
	for table, st := range tg.publicationTables(t) {
		if st.exists {
			t.Errorf("init-db created %s before refusing the names", table)
		}
	}
}

// The server, not the spelling, decides whether two names are one table
// (R2-F2 of #897's review): a schema-qualified change log is the
// unqualified record table when the search path resolves to its schema, and
// a name the server truncates is the table its first 63 bytes name. The
// refusal leaves an existing change log under that name, rows included, as
// it was.
func TestInitDBRefusesNamesThePublicationRecordResolvesTo(t *testing.T) {
	t.Run("a public-qualified change log", func(t *testing.T) {
		tg := newInitDBTarget(t)
		tg.writeOutboxAndTimer(t, "due")
		record := strings.Trim(publication.TablesFor(tg.opts.entityMain).Record, `"`)
		tg.opts.changeLog = "public." + record
		ctx := context.Background()
		if _, err := tg.pool.Exec(ctx, changeLogDDL(quoteIdentifier(tg.opts.changeLog))); err != nil {
			t.Fatalf("create the change log: %v", err)
		}
		if _, err := tg.pool.Exec(ctx, "INSERT INTO "+quoteIdentifier(tg.opts.changeLog)+" (schema_id, row_id, changed_at) VALUES (1, gen_random_uuid(), 1)"); err != nil {
			t.Fatalf("write a change log row: %v", err)
		}
		out, err := tg.run(t)
		tg.requireRefusedBeforeDDL(t, out, err, `"`+record+`" is the publication record table`,
			`"public"."`+record+`" is the change log table`, `PostgreSQL resolves both to "public"."`+record+`"`)
		var rows int64
		if err := tg.pool.QueryRow(ctx, "SELECT count(*) FROM "+quoteIdentifier(tg.opts.changeLog)+" WHERE flushed_at = 0").Scan(&rows); err != nil || rows != 1 {
			t.Fatalf("the change log after the refusal: %d unflushed rows, err %v; want its 1 row", rows, err)
		}
	})
	t.Run("a change log qualified by the search path's schema", func(t *testing.T) {
		tg := newInitDBTarget(t)
		tg.writeOutboxAndTimer(t, "due")
		schema := tg.createSchema(t, "_sp")
		tg.useSearchPath(schema)
		tg.opts.changeLog = schema + "." + strings.Trim(publication.TablesFor(tg.opts.entityMain).Record, `"`)
		out, err := tg.run(t)
		tg.requireRefusedBeforeDDL(t, out, err, `PostgreSQL resolves both to "`+schema+`".`)
	})
	t.Run("an EAV table the server truncates to the record table", func(t *testing.T) {
		tg := newInitDBTarget(t)
		tg.writeOutboxAndTimer(t, "due")
		tg.opts.entityMain += strings.Repeat("x", 51-len(tg.opts.entityMain))
		record := strings.Trim(publication.TablesFor(tg.opts.entityMain).Record, `"`)
		if len(record) != 63 {
			t.Fatalf("the record table %q is %d bytes, want 63", record, len(record))
		}
		tg.opts.eavTable = record + "_eav"
		out, err := tg.run(t)
		tg.requireRefusedBeforeDDL(t, out, err, "the EAV table, which --eav-table names", `PostgreSQL resolves both to "public"."`+record+`"`)
	})
}

// A change log named like the record table in a schema the search path
// does not resolve unqualified names to is another table: init-db creates
// both publication tables beside it (R2-F2 of #897's review).
func TestInitDBAcceptsTheRecordTableNameInAnotherSchema(t *testing.T) {
	tg := newInitDBTarget(t)
	tg.writeOutboxAndTimer(t, "due")
	schema := tg.createSchema(t, "_other")
	tg.opts.changeLog = schema + "." + strings.Trim(publication.TablesFor(tg.opts.entityMain).Record, `"`)
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db with the change log in %s: %v\n%s", schema, err, out)
	}
	for table, st := range tg.publicationTables(t) {
		if !st.exists {
			t.Fatalf("init-db with the change log in another schema did not create %s", table)
		}
	}
	requireStates(t, "init-db with the change log in another schema", tg.records(t),
		map[string]publication.State{"outbox_message": publication.Unpublished, "timer": publication.Unpublished})
	var columns int64
	if err := tg.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM pg_attribute WHERE attrelid = to_regclass($1) AND attname = 'flushed_at'`,
		quoteIdentifier(tg.opts.changeLog)).Scan(&columns); err != nil || columns != 1 {
		t.Fatalf("the change log in %s: %d flushed_at columns, err %v; want 1", schema, columns, err)
	}
}
