package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// records reads every registered schema's publication record, by schema
// name; a schema without one maps to the zero Record.
func (tg *initDBTarget) records(t *testing.T) map[string]publication.Record {
	t.Helper()
	ctx := context.Background()
	registered, err := loadRegisteredSchemaIDs(ctx, tg.pool, tg.opts.schemaTable)
	if err != nil {
		t.Fatalf("read the registry: %v", err)
	}
	tx, err := tg.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	out := map[string]publication.Record{}
	for name, id := range registered {
		snap, err := publication.Read(ctx, publication.PgxTx(tx), publication.TablesFor(tg.opts.entityMain), id)
		if errors.Is(err, publication.ErrNoRecord) {
			out[name] = publication.Record{}
			continue
		}
		if err != nil {
			t.Fatalf("read the record of %s (%d): %v", name, id, err)
		}
		out[name] = snap.Record
	}
	return out
}

// requireStates fails unless each named schema's record is in the state
// wanted ("" for no record) at epoch 1, the only epoch registration and the
// cutover write.
func requireStates(t *testing.T, when string, got map[string]publication.Record, want map[string]publication.State) {
	t.Helper()
	for name, state := range want {
		r, ok := got[name]
		if !ok {
			t.Fatalf("%s: schema %s is not registered", when, name)
		}
		if r.State != state || (state != "" && r.Epoch != 1) {
			t.Errorf("%s: schema %s has a %q record at epoch %d, want %q at epoch 1", when, name, r.State, r.Epoch, state)
		}
	}
}

// emptyPublicationRecords empties the record and epoch tables: the state of
// a deployment from before V2, which never had a record. Emptying the record
// table alone is a lost record, which init-db recovers instead.
func (tg *initDBTarget) emptyPublicationRecords(t *testing.T) {
	t.Helper()
	tables := publication.TablesFor(tg.opts.entityMain)
	for _, table := range []string{tables.Record, tables.Epoch} {
		if _, err := tg.pool.Exec(context.Background(), "DELETE FROM "+table); err != nil {
			t.Fatalf("empty %s: %v", table, err)
		}
	}
}

// exec runs a statement of the test's own on the target's tables.
func (tg *initDBTarget) exec(t *testing.T, format string, args ...any) {
	t.Helper()
	sql := fmt.Sprintf(format, quoteIdentifier(tg.opts.entityMain), quoteIdentifier(tg.opts.changeLog), quoteIdentifier(tg.opts.schemaTable))
	if _, err := tg.pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// insertRow writes a live entity_main row of schema id, and, when flushedAt
// is not negative, its change_log entry flushed at flushedAt (0: unflushed).
func (tg *initDBTarget) insertRow(t *testing.T, id int16, flushedAt int64) {
	t.Helper()
	row := uuid.New()
	tg.exec(t, `INSERT INTO %[1]s (ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at) VALUES ($1, $2, 1, 1)`, id, row)
	if flushedAt >= 0 {
		tg.exec(t, `INSERT INTO %[2]s (schema_id, row_id, flushed_at, changed_at) VALUES ($1, $2, $3, 1)`, id, row, flushedAt)
	}
}

// Registration creates each schema's record in its registry transaction, and
// the V2 cutover gives a deployment registered before V2 its records: no
// history is UNPUBLISHED, a flushed entry or a live row without an entry is
// AWAITING_INIT. A rerun keeps every record as it is.
func TestInitDBWritesTheRecordsRegistrationAndTheCutoverOwe(t *testing.T) {
	tg := newInitDBTarget(t)
	tg.writeOutboxAndTimer(t, "due")
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db on a fresh database: %v\n%s", err, out)
	}
	fresh := tg.records(t)
	requireStates(t, "fresh init-db", fresh, map[string]publication.State{"outbox_message": publication.Unpublished, "timer": publication.Unpublished})

	// A deployment from before V2: its schemas carry V1 history, and its
	// publication tables are empty.
	tg.emptyPublicationRecords(t)
	tg.insertRow(t, 100, 1700000000000) // outbox_message: a flushed entry
	tg.insertRow(t, 100, 0)
	tg.insertRow(t, 101, -1) // timer: a row the V1 onboarding handoff cleared
	tg.writeSchema(t, "visit", map[string]string{"page": "text_02"}, nil)
	out, err := tg.run(t)
	if err != nil {
		t.Fatalf("init-db over a V1 deployment: %v\n%s", err, out)
	}
	cut := tg.records(t)
	requireStates(t, "cutover", cut, map[string]publication.State{
		"outbox_message": publication.AwaitingInit, "timer": publication.AwaitingInit, "visit": publication.Unpublished,
	})
	if !strings.Contains(out, "Publication record of schema 100: initialized AWAITING_INIT") {
		t.Errorf("init-db output does not report the cutover:\n%s", out)
	}

	if out, err := tg.run(t); err != nil {
		t.Fatalf("rerun: %v\n%s", err, out)
	}
	if rerun := tg.records(t); fmt.Sprint(rerun) != fmt.Sprint(cut) {
		t.Fatalf("the rerun changed the records:\nbefore %v\n after %v", cut, rerun)
	}
}

// The cutover skips a schema whose lock a V1 writer holds: it writes no
// record for it, reports it, and a later run initializes it.
func TestInitDBCutoverSkipsASchemaWhoseLockIsHeld(t *testing.T) {
	tg := newInitDBTarget(t)
	tg.writeOutboxAndTimer(t)
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db: %v\n%s", err, out)
	}
	tg.emptyPublicationRecords(t)
	ctx := context.Background()
	writer, err := tg.pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer writer.Release()
	if _, err := writer.Exec(ctx, "SELECT pg_advisory_lock(100, 100)"); err != nil {
		t.Fatalf("take schema 100's lock as cdc-flush does: %v", err)
	}
	out, err := tg.run(t)
	if err != nil {
		t.Fatalf("init-db with schema 100 locked: %v\n%s", err, out)
	}
	requireStates(t, "locked cutover", tg.records(t), map[string]publication.State{"outbox_message": "", "timer": publication.Unpublished})
	if !strings.Contains(out, "Publication record of schema 100: skipped: schema lock held") || !strings.Contains(out, "schemas [100]") {
		t.Errorf("init-db output does not report the skipped schema:\n%s", out)
	}

	if _, err := writer.Exec(ctx, "SELECT pg_advisory_unlock(100, 100)"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db after the writer stopped: %v\n%s", err, out)
	}
	requireStates(t, "retried cutover", tg.records(t), map[string]publication.State{"outbox_message": publication.Unpublished, "timer": publication.Unpublished})
}

// dropSchema is the drop transition: one transaction deletes the registry
// row and the schema's ledger, and resets the record.
func (tg *initDBTarget) dropSchema(t *testing.T, name string, id int16) {
	t.Helper()
	ctx := context.Background()
	tables := publication.TablesFor(tg.opts.entityMain)
	err := pgx.BeginFunc(ctx, tg.pool, func(tx pgx.Tx) error {
		snap, err := publication.Read(ctx, publication.PgxTx(tx), tables, id)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "DELETE FROM "+quoteIdentifier(tg.opts.schemaTable)+" WHERE schema_name = $1", name); err != nil {
			return err
		}
		for _, suffix := range []string{schemaledger.AttributeTableSuffix, schemaledger.GenerationTableSuffix} {
			ledger := sqlutil.SuffixedIdentifier(tg.opts.schemaTable, suffix)
			if _, err := tx.Exec(ctx, "DELETE FROM "+quoteIdentifier(ledger)+" WHERE schema_id = $1", id); err != nil {
				return err
			}
		}
		_, err = publication.Reset(ctx, publication.PgxTx(tx), tables, snap.Record)
		return err
	})
	if err != nil {
		t.Fatalf("drop schema %s: %v", name, err)
	}
}

// Registering a dropped schema's id again activates its record in the
// registration transaction: the next epoch, UNPUBLISHED. A row of the
// dropped schema still there refuses it, and the registration rolls back.
func TestInitDBActivatesADroppedSchemaRegisteredAgain(t *testing.T) {
	tg := newInitDBTarget(t)
	tg.writeOutboxAndTimer(t)
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db: %v\n%s", err, out)
	}
	tg.dropSchema(t, "timer", 101)
	tg.insertRow(t, 101, 0)
	out, err := tg.run(t)
	if !errors.Is(err, publication.ErrResidue) {
		t.Fatalf("init-db re-registered schema 101 over a row of the dropped schema: %v\n%s", err, out)
	}
	if _, ok := tg.records(t)["timer"]; ok {
		t.Fatalf("the refused registration of timer committed")
	}

	tg.exec(t, `DELETE FROM %[1]s WHERE ltbase_schema_id = 101`)
	tg.exec(t, `DELETE FROM %[2]s WHERE schema_id = 101`)
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db after the purge: %v\n%s", err, out)
	}
	timer := tg.records(t)["timer"]
	if timer.SchemaID != 101 || timer.State != publication.Unpublished || timer.Epoch != 3 {
		t.Fatalf("re-registered timer has record %+v, want schema 101 UNPUBLISHED at epoch 3 (dropped at 2)", timer)
	}
}
