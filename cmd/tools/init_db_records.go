package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/uniquekey"
)

// recordStep is what ensurePublicationRecords did for one registered schema.
type recordStep string

const (
	recordKept        recordStep = "kept"
	recordUnpublished recordStep = "initialized UNPUBLISHED"
	recordActivated   recordStep = "activated"
	recordLockBusy    recordStep = "skipped: schema lock held"
	recordRecovered   recordStep = "recovered"
)

// ensurePublicationRecords gives every registered schema its Protocol V2
// publication record (docs/publication-protocol-v2.md, "Lifecycle of the row"),
// on tx, the transaction that registered the schemas, so a
// registry row never commits without the record registration owes it:
//
//   - A schema with a record keeps it. Registering again never resets one.
//   - A schema whose record is DROPPED was registered again under its old id:
//     Activate checks that no row of the dropped schema remains, then starts
//     the record's next epoch, UNPUBLISHED.
//   - A schema whose registry row this transaction inserted and whose id
//     already has a record that is not DROPPED belonged to a registration
//     removed without the drop transition (drop-schema). The new
//     registration would inherit its epoch, its published generation and its
//     rows, so the run fails and rolls the registration back.
//   - A schema without a record whose id has a lifecycle epoch or
//     generation history (rows in the publication epoch or generation table)
//     lost a V2 record. A record at epoch 1 and generation 0 could carry an
//     epoch a running process still pins and would reuse numbers the history
//     committed, so under the schema lock the record is recovered after the
//     id's lifecycle epoch, the highest epoch any record of it carried, with
//     the history's highest generation as the counter
//     (publication.InitializeAfterLoss): AWAITING_INIT for a registered
//     schema with generation history, whose next cdc-init replaces its
//     parquet tiers; UNPUBLISHED for one without, once its rows are checked
//     against the UNPUBLISHED invariant as below; and, for an id this
//     transaction registered, DROPPED and then activated as above. An id whose generation history survives
//     but whose lifecycle epoch is lost too fails the run: no epoch can be
//     shown free of a running process's pin, and the operator establishes
//     one (docs/publication-protocol-v2.md, "A lost record").
//   - A schema without a record or history is a new registration. Under the
//     schema lock, its rows are checked against the UNPUBLISHED invariant
//     (every change_log entry unflushed, every entity_main row with one), and
//     it is initialized UNPUBLISHED (Initialize). Rows that break the
//     invariant are state no Protocol V2 writer leaves without a publication
//     history: a parquet tier may hold rows the hot leg would not serve, so
//     the run fails (errUnpublishedInvariantBroken) rather than write a record
//     that hides them. A lock init-db cannot take means a writer still runs:
//     a schema registered before this run is skipped, keeps no record (its
//     reads and writes fail closed on ErrNoRecord), and the next init-db
//     retries it.
//   - A schema whose registry row this transaction inserted (inserted) is
//     never skipped: its registration commits with its record or not at
//     all. A lock init-db cannot take for it fails the run, which rolls the
//     registration back, and the next init-db registers it again.
//
// The lock is pg_try_advisory_xact_lock on the key cdc-flush and cdc-init take
// as a session lock (internal/cdc TrySchemaLock): the two kinds conflict, and
// the transaction's lock is held until the record commits. init-db runs on
// PostgreSQL only (#690).
func ensurePublicationRecords(ctx context.Context, tx pgx.Tx, opts initDBOptions, inserted map[int16]bool, out io.Writer) error {
	if opts.engine.Resolved() == forma.EngineAuroraDSQL {
		return errors.New("the publication records need the PostgreSQL schema lock, and init-db cannot run on Aurora DSQL yet (#690)")
	}
	registered, err := loadRegisteredSchemaIDs(ctx, tx, opts.schemaTable)
	if err != nil {
		return fmt.Errorf("list the schemas to give a publication record: %w", err)
	}
	ids := make([]int16, 0, len(registered))
	for _, id := range registered {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	tables := publication.TablesFor(opts.entityMain)
	var skipped []int16
	for _, id := range ids {
		step, err := ensurePublicationRecord(ctx, tx, opts, tables, id, inserted[id])
		if err != nil {
			return fmt.Errorf("publication record of schema %d: %w", id, err)
		}
		if step == recordLockBusy {
			skipped = append(skipped, id)
		}
		_, _ = fmt.Fprintf(out, "Publication record of schema %d: %s\n", id, step)
	}
	if len(skipped) > 0 {
		_, _ = fmt.Fprintf(out, "Publication records not written for schemas %v: a cdc-flush or cdc-init holds their schema lock. "+
			"Stop their cdc-flush and cdc-init runs, then run init-db again; until then their reads and writes fail closed.\n", skipped)
	}
	return nil
}

// ensurePublicationRecord is ensurePublicationRecords' step for one schema;
// registered says tx inserted its registry row.
func ensurePublicationRecord(ctx context.Context, tx pgx.Tx, opts initDBOptions, tables publication.Tables, id int16, registered bool) (recordStep, error) {
	snap, err := publication.Read(ctx, publication.PgxTx(tx), tables, id)
	switch {
	case errors.Is(err, publication.ErrNoRecord):
		step, err := writeMissingRecord(ctx, tx, opts, tables, id, registered)
		if step == recordLockBusy && registered {
			return "", fmt.Errorf("schema %d is registered by this init-db, and its schema lock is held (a cdc-flush, a cdc-init, "+
				"or another session's advisory lock on (%d, %d)): its registry row cannot commit without its publication record, "+
				"so the registration is rolled back; run init-db again once the lock is free: %w", id, id, id, errRegistrationLockBusy)
		}
		return step, err
	case err != nil:
		return "", fmt.Errorf("read: %w", err)
	case snap.State != publication.Dropped && registered:
		return "", fmt.Errorf("schema %d is registered by this init-db, but its id already has a %s publication record at epoch %d "+
			"(generation %d): an earlier registration of the id was removed without retiring its record, and the new schema "+
			"would inherit its epoch, its published generation and its rows, so the registration is rolled back; "+
			"run `forma-tools drop-schema --schema-id %d` to retire the record and purge the old rows, then run init-db again: %w",
			id, snap.State, snap.Epoch, snap.Generation, id, errRegistrationInheritsRecord)
	case snap.State != publication.Dropped:
		return recordKept, nil
	}
	return activateDropped(ctx, tx, opts, tables, snap.Record)
}

// activateDropped re-registers a dropped id: Activate checks the dropped
// schema's rows are purged, then starts the record's next epoch.
func activateDropped(ctx context.Context, tx pgx.Tx, opts initDBOptions, tables publication.Tables, rec publication.Record) (recordStep, error) {
	activated, err := publication.Activate(ctx, publication.PgxTx(tx), tables, rec, schemaResidue(opts))
	if err != nil {
		return "", fmt.Errorf("activate the re-registered schema: %w", err)
	}
	return recordStep(fmt.Sprintf("%s at epoch %d", recordActivated, activated.Epoch)), nil
}

// schemaResidue names the tables Activate checks, and drop-schema purges, for
// a dropped schema's rows: its entity_main, EAV and change_log rows, and the
// unique-key state derived from them (#651). A key entry left behind would
// keep reserving a deleted row's value against the re-registered schema, and
// a build-state row would make its key look built, or, under another
// declaration, stop the server as an incompatible key.
func schemaResidue(opts initDBOptions) []publication.Residue {
	return []publication.Residue{
		{Table: opts.entityMain, SchemaIDColumn: "ltbase_schema_id"},
		{Table: opts.eavTable},
		{Table: opts.changeLog},
		{Table: uniquekey.KeyTable(opts.entityMain), SchemaIDColumn: "ltbase_schema_id"},
		{Table: uniquekey.StateTable(opts.entityMain), SchemaIDColumn: "ltbase_schema_id"},
	}
}

// errRegistrationInheritsRecord reports a schema init-db registered whose id
// still has a record that is not DROPPED, which fails the run and rolls the
// registration back.
var errRegistrationInheritsRecord = errors.New("registration would inherit a record that was never retired")

// errRegistrationLockBusy reports a schema init-db registered whose schema
// lock it could not take, which fails the run and rolls the registration back.
var errRegistrationLockBusy = errors.New("schema lock held at registration")

// trySchemaLockSQL takes the schema's lock for the rest of the transaction,
// on the key internal/cdc's TrySchemaLock takes per session.
const trySchemaLockSQL = `SELECT pg_try_advisory_xact_lock($1, $2)`

// writeMissingRecord writes the record of a schema that has none, under the
// schema lock: recovered after its lifecycle epoch when it had a record
// before, otherwise initialized UNPUBLISHED once its rows satisfy the
// UNPUBLISHED invariant.
func writeMissingRecord(ctx context.Context, tx pgx.Tx, opts initDBOptions, tables publication.Tables, id int16, registered bool) (recordStep, error) {
	var locked bool
	if err := tx.QueryRow(ctx, trySchemaLockSQL, int32(id), int32(id)).Scan(&locked); err != nil {
		return "", fmt.Errorf("take the schema lock: %w", err)
	}
	if !locked {
		return recordLockBusy, nil
	}
	history, err := publication.ReadHistory(ctx, publication.PgxTx(tx), tables, id)
	if err != nil {
		return "", fmt.Errorf("check the publication history: %w", err)
	}
	if history.Lost() {
		return recoverLostRecord(ctx, tx, opts, tables, id, history, registered)
	}
	return initializeUnpublished(ctx, tx, opts, tables, id)
}

// recoverLostRecord writes the record of a schema whose record was lost,
// after its lifecycle epoch and with its history's highest generation as the
// counter: for a schema registered before this run, AWAITING_INIT when
// generation history survives and otherwise UNPUBLISHED, once its rows
// satisfy the UNPUBLISHED invariant; for an id this run registered, DROPPED and then activated, so the
// dropped registration's rows are checked as on any re-registration.
func recoverLostRecord(ctx context.Context, tx pgx.Tx, opts initDBOptions, tables publication.Tables, id int16, h publication.History, registered bool) (recordStep, error) {
	state, err := lostRecordState(ctx, tx, opts, id, h, registered)
	if err != nil {
		return "", err
	}
	rec, wrote, err := publication.InitializeAfterLoss(ctx, publication.PgxTx(tx), tables, id, h, state)
	if errors.Is(err, publication.ErrLifecycleLost) {
		return "", fmt.Errorf("schema %d's record and its lifecycle epoch in %s are both missing while %d generation(s) through epoch %d "+
			"survive, so init-db cannot pick an epoch no running server, cdc-flush or cdc-init still pins: stop every one of them, "+
			"establish the id's lifecycle epoch as docs/publication-protocol-v2.md (\"A lost record\") describes, then run init-db again: %w",
			id, tables.Epoch, h.Generations, h.MaxEpoch, err)
	}
	if err != nil {
		return "", fmt.Errorf("recover the lost record: %w", err)
	}
	if !wrote {
		return "", errors.New("another init-db wrote the record concurrently; run init-db again")
	}
	if registered {
		return activateDropped(ctx, tx, opts, tables, rec)
	}
	return recordStep(fmt.Sprintf("%s %s at epoch %d: its record was missing while its lifecycle epoch %d and %d generation(s) "+
		"through %d survive; restart every server, cdc-flush and cdc-init that loaded it, and run cdc-init for it if it awaits one",
		recordRecovered, rec.State, rec.Epoch, h.Epoch, h.Generations, h.MaxGeneration)), nil
}

// lostRecordState is the state a lost record is recovered in: DROPPED for an
// id this run registered, AWAITING_INIT for a schema whose generation history
// survives, and otherwise UNPUBLISHED, which requires the UNPUBLISHED
// invariant to hold.
func lostRecordState(ctx context.Context, tx pgx.Tx, opts initDBOptions, id int16, h publication.History, registered bool) (publication.State, error) {
	switch {
	case registered:
		return publication.Dropped, nil
	case h.Generations > 0:
		return publication.AwaitingInit, nil
	}
	if err := requireUnpublishedInvariant(ctx, tx, opts, id); err != nil {
		return "", err
	}
	return publication.Unpublished, nil
}

// initializeUnpublished writes the UNPUBLISHED record of a schema that has
// neither a record nor publication history, under the schema lock the caller
// holds, once its rows satisfy the UNPUBLISHED invariant.
func initializeUnpublished(ctx context.Context, tx pgx.Tx, opts initDBOptions, tables publication.Tables, id int16) (recordStep, error) {
	if err := requireUnpublishedInvariant(ctx, tx, opts, id); err != nil {
		return "", err
	}
	wrote, err := publication.Initialize(ctx, publication.PgxTx(tx), tables, id)
	if err != nil {
		return "", fmt.Errorf("initialize: %w", err)
	}
	if !wrote {
		// The lock and the read above leave no writer of the record but
		// another init-db, whose insert committed after this snapshot.
		return "", errors.New("another init-db wrote the record concurrently; run init-db again")
	}
	return recordUnpublished, nil
}

// errUnpublishedInvariantBroken reports a schema without a record or
// generation history whose rows break the UNPUBLISHED invariant, which fails
// the run.
var errUnpublishedInvariantBroken = errors.New("rows break the UNPUBLISHED invariant")

// requireUnpublishedInvariant refuses, on tx, a schema whose rows break the
// UNPUBLISHED invariant (docs/publication-protocol-v2.md): a flushed
// change_log entry, or an entity_main row without any entry. Only a
// publication marks entries, in the transaction that records its generation,
// and the write path gives every row an entry, so a schema with neither a
// record nor generation history in that state was not written by Protocol V2.
// A parquet tier may hold rows no unflushed entry shadows, and an UNPUBLISHED
// record would answer from the hot leg without them, so init-db writes none.
func requireUnpublishedInvariant(ctx context.Context, tx pgx.Tx, opts initDBOptions, id int16) error {
	var broken bool
	if err := tx.QueryRow(ctx, unpublishedInvariantBrokenSQL(opts), id).Scan(&broken); err != nil {
		return fmt.Errorf("check schema %d's rows against the UNPUBLISHED invariant: %w", id, err)
	}
	if broken {
		return fmt.Errorf("schema %d has no publication record and no generation history, but %s holds a flushed entry of it "+
			"or %s a row of it without one, which no Protocol V2 writer leaves: an UNPUBLISHED record would hide whatever a "+
			"parquet tier holds of it, so init-db writes none. Restore the publication tables with the data tables they "+
			"belong to, or purge the schema's rows and register it again: %w",
			id, opts.changeLog, opts.entityMain, errUnpublishedInvariantBroken)
	}
	return nil
}

// unpublishedInvariantBrokenSQL answers whether schema $1 has a flushed
// change_log entry, or an entity_main row without any entry.
func unpublishedInvariantBrokenSQL(opts initDBOptions) string {
	return fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %[1]s WHERE schema_id = $1 AND flushed_at <> 0)
	OR EXISTS (SELECT 1 FROM %[2]s m WHERE m.ltbase_schema_id = $1 AND NOT EXISTS (
		SELECT 1 FROM %[1]s c WHERE c.schema_id = $1 AND c.row_id = m.ltbase_row_id))`,
		quoteIdentifier(opts.changeLog), quoteIdentifier(opts.entityMain))
}
