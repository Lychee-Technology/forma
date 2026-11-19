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
)

// recordStep is what ensurePublicationRecords did for one registered schema.
type recordStep string

const (
	recordKept         recordStep = "kept"
	recordUnpublished  recordStep = "initialized UNPUBLISHED"
	recordAwaitingInit recordStep = "initialized AWAITING_INIT"
	recordActivated    recordStep = "activated"
	recordLockBusy     recordStep = "skipped: schema lock held"
)

// ensurePublicationRecords gives every registered schema its Protocol V2
// publication record (docs/publication-protocol-v2.md, "Lifecycle of the row"
// and "Upgrade"), on tx, the transaction that registered the schemas, so a
// registry row never commits without the record registration owes it:
//
//   - A schema with a record keeps it. Registering again never resets one.
//   - A schema whose record is DROPPED was registered again under its old id:
//     Activate checks that no row of the dropped schema remains, then starts
//     the record's next epoch, UNPUBLISHED.
//   - A schema without a record is a new registration or one the V2 cutover
//     has not reached. Under the schema lock every V1 marker (cdc-flush and
//     the onboarding handoff) takes, the parquet-history check decides the
//     state: without history the schema satisfies the UNPUBLISHED invariant
//     (Initialize); with it the schema waits for its first V2 cdc-init
//     (InitializeAwaitingInit). A lock init-db cannot take means a writer
//     still runs: a schema registered before this run is skipped, keeps no
//     record (its reads and writes fail closed on ErrNoRecord), and the next
//     init-db retries it.
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
			"Stop every V1 writer, then run init-db again; until then their reads and writes fail closed.\n", skipped)
	}
	return nil
}

// ensurePublicationRecord is ensurePublicationRecords' step for one schema;
// registered says tx inserted its registry row.
func ensurePublicationRecord(ctx context.Context, tx pgx.Tx, opts initDBOptions, tables publication.Tables, id int16, registered bool) (recordStep, error) {
	snap, err := publication.Read(ctx, publication.PgxTx(tx), tables, id)
	switch {
	case errors.Is(err, publication.ErrNoRecord):
		step, err := cutoverSchema(ctx, tx, opts, tables, id)
		if step == recordLockBusy && registered {
			return "", fmt.Errorf("schema %d is registered by this init-db, and its schema lock is held (a cdc-flush, a cdc-init, "+
				"or another session's advisory lock on (%d, %d)): its registry row cannot commit without its publication record, "+
				"so the registration is rolled back; run init-db again once the lock is free: %w", id, id, id, errRegistrationLockBusy)
		}
		return step, err
	case err != nil:
		return "", fmt.Errorf("read: %w", err)
	case snap.State != publication.Dropped:
		return recordKept, nil
	}
	residue := []publication.Residue{
		{Table: opts.entityMain, SchemaIDColumn: "ltbase_schema_id"},
		{Table: opts.eavTable},
		{Table: opts.changeLog},
	}
	activated, err := publication.Activate(ctx, publication.PgxTx(tx), tables, snap.Record, residue)
	if err != nil {
		return "", fmt.Errorf("activate the re-registered schema: %w", err)
	}
	return recordStep(fmt.Sprintf("%s at epoch %d", recordActivated, activated.Epoch)), nil
}

// errRegistrationLockBusy reports a schema init-db registered whose schema
// lock it could not take, which fails the run and rolls the registration back.
var errRegistrationLockBusy = errors.New("schema lock held at registration")

// tryCutoverLockSQL takes the schema's lock for the rest of the transaction,
// on the key internal/cdc's TrySchemaLock takes per session.
const tryCutoverLockSQL = `SELECT pg_try_advisory_xact_lock($1, $2)`

// cutoverSchema writes the record of a schema that has none, under the
// schema lock, in the state its parquet history allows.
func cutoverSchema(ctx context.Context, tx pgx.Tx, opts initDBOptions, tables publication.Tables, id int16) (recordStep, error) {
	var locked bool
	if err := tx.QueryRow(ctx, tryCutoverLockSQL, int32(id), int32(id)).Scan(&locked); err != nil {
		return "", fmt.Errorf("take the schema lock: %w", err)
	}
	if !locked {
		return recordLockBusy, nil
	}
	var history bool
	if err := tx.QueryRow(ctx, parquetHistorySQL(opts), id).Scan(&history); err != nil {
		return "", fmt.Errorf("check the parquet history: %w", err)
	}
	initialize, step := publication.Initialize, recordUnpublished
	if history {
		initialize, step = publication.InitializeAwaitingInit, recordAwaitingInit
	}
	wrote, err := initialize(ctx, publication.PgxTx(tx), tables, id)
	if err != nil {
		return "", fmt.Errorf("initialize: %w", err)
	}
	if !wrote {
		// The lock and the read above leave no writer of the record but
		// another init-db, whose insert committed after this snapshot.
		return "", errors.New("another init-db wrote the record concurrently; run init-db again")
	}
	return step, nil
}

// parquetHistorySQL answers whether schema $1 has parquet history from V1: a
// flushed change_log entry, or an entity_main row without any entry, which
// only the V1 onboarding handoff, which cleared change_log after a cdc-init
// export, leaves behind. Either means a parquet tier may hold rows no
// unflushed entry shadows, so the schema cannot be UNPUBLISHED: with every
// entry unflushed and every row holding one, the hot leg is the whole answer.
func parquetHistorySQL(opts initDBOptions) string {
	return fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %[1]s WHERE schema_id = $1 AND flushed_at <> 0)
	OR EXISTS (SELECT 1 FROM %[2]s m WHERE m.ltbase_schema_id = $1 AND NOT EXISTS (
		SELECT 1 FROM %[1]s c WHERE c.schema_id = $1 AND c.row_id = m.ltbase_row_id))`,
		quoteIdentifier(opts.changeLog), quoteIdentifier(opts.entityMain))
}
