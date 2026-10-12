package main

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lychee-technology/forma/internal/declaredindex"
)

// dropStatement matches the DROP INDEX statement init-db prints as a remedy.
var dropStatement = regexp.MustCompile(`DROP INDEX CONCURRENTLY "[^"]+"(\."[^"]+")?`)

// An entity_main in a schema off the search_path keeps its declared indexes in
// that schema. Every DROP INDEX init-db prints for one, for an invalid index
// and for an unreferenced one, must work as printed from a session on the
// default search_path, where the bare index name does not resolve.
func TestInitDBRemediesDropIndexesOutsideSearchPath(t *testing.T) {
	ctx := context.Background()
	tg := newInitDBTarget(t)
	schema := strings.TrimSuffix(tg.opts.entityMain, "_entity_main") + "_ns"
	if _, err := tg.pool.Exec(ctx, "CREATE SCHEMA "+quoteIdentifier(schema)); err != nil {
		t.Fatalf("create schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		if _, err := tg.pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+quoteIdentifier(schema)+" CASCADE"); err != nil {
			t.Logf("drop schema %s: %v", schema, err)
		}
	})
	table := schema + "." + tg.opts.entityMain
	tg.opts.entityMain = table

	tg.writeOutboxAndTimer(t, "due", "lease")
	tg.opts.skipDeclaredIndexes = true
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db --skip-declared-indexes: %v\n%s", err, out)
	}
	due := declaredindex.Name(table, []string{"ltbase_schema_id", "text_01", "bigint_01", "ltbase_row_id"})
	leaveInvalidIndex(t, tg.pool, table, due)

	tg.opts.skipDeclaredIndexes = false
	out, err := tg.run(t)
	if err == nil {
		t.Fatalf("init-db exited 0 with an invalid declared index:\n%s", out)
	}
	tg.runRemedy(t, schema, due, err.Error())
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db after the printed remedy: %v\n%s", err, out)
	}
	if valid, ok := tg.indexValidity(t, schema, due); !ok || !valid {
		t.Fatalf("init-db did not rebuild %s valid after the remedy (present %v, valid %v)", due, ok, valid)
	}

	tg.writeOutboxAndTimer(t, "due")
	out, err = tg.run(t)
	if err != nil {
		t.Fatalf("init-db after removing lease: %v\n%s", err, out)
	}
	lease := declaredindex.Name(table, []string{"ltbase_schema_id", "text_01", "bigint_02", "ltbase_row_id"})
	if !strings.Contains(out, "Unreferenced declared index: "+lease+" on "+table) {
		t.Fatalf("init-db does not report %s as unreferenced:\n%s", lease, out)
	}
	tg.runRemedy(t, schema, lease, out)
	if _, ok := tg.indexValidity(t, schema, lease); ok {
		t.Fatalf("the printed remedy left %s in place", lease)
	}
}

// runRemedy checks that message prints the drop of name qualified by schema,
// that the bare name does not resolve on the pool's default search_path, and
// runs the printed statement there.
func (tg *initDBTarget) runRemedy(t *testing.T, schema, name, message string) {
	t.Helper()
	ctx := context.Background()
	printed := dropStatement.FindString(message)
	if want := declaredindex.DropSQL(schema, name); printed != want {
		t.Fatalf("init-db printed remedy %q, want %q, in:\n%s", printed, want, message)
	}
	var pgErr *pgconn.PgError
	if _, err := tg.pool.Exec(ctx, "DROP INDEX CONCURRENTLY "+quoteIdentifier(name)); !errors.As(err, &pgErr) || pgErr.Code != "42704" {
		t.Fatalf("an unqualified drop of %s returned %v, want undefined_object (42704): schema %s must be off the search_path", name, err, schema)
	}
	if _, err := tg.pool.Exec(ctx, printed); err != nil {
		t.Fatalf("the printed remedy %q failed: %v", printed, err)
	}
}

// indexValidity reports pg_index.indisvalid for schema.name, and whether the
// index exists.
func (tg *initDBTarget) indexValidity(t *testing.T, schema, name string) (valid, ok bool) {
	t.Helper()
	err := tg.pool.QueryRow(context.Background(), `SELECT i.indisvalid FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2`, schema, name).Scan(&valid)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false
	}
	if err != nil {
		t.Fatalf("read validity of %s.%s: %v", schema, name, err)
	}
	return valid, true
}
