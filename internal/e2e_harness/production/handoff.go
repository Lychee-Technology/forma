package production

import (
	"context"

	"github.com/google/uuid"
)

// The helpers below name why a test expects rows to leave the dirty set
// (#901). Under Protocol V1 each one cleared change_log entries. Protocol V2
// never clears change_log (docs/publication-protocol-v2.md, "The cdc-init
// handoff"): the tools themselves take the rows out of the dirty set, and the
// helpers only check that they did, failing the test otherwise. No helper
// writes change_log.
//
//   - onboarded: cdc-init exported the rows to base and its handoff marked
//     their entries flushed at their export versions, in the transactions
//     that held the init's lineage. The base generation is their only source.
//   - flushed: a flush exported the rows and marked their entries in the
//     transaction that published its generation, so no hot entry is left.

// MarkOnboarded checks the cdc-init handoff of schema: the schema's latest
// RunInit published and handed off every entry it exported, so the base
// generation is the exported rows' only source.
func (e *Env) MarkOnboarded(ctx context.Context, schema SchemaRef) {
	e.T.Helper()
	e.requireHandedOff(schema)
	e.requireNoExportedRowDirty(ctx, schema)
}

// MarkRowsOnboarded is MarkOnboarded for rows: the handoff marked their
// entries, so they are served from base. The V2 handoff marks every row the
// init exported, so the schema's other exported rows leave the dirty set too.
func (e *Env) MarkRowsOnboarded(ctx context.Context, schema SchemaRef, rows []uuid.UUID) {
	e.T.Helper()
	e.requireHandedOff(schema)
	var dirty int64
	if err := e.Pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM change_log WHERE schema_id = $1 AND row_id = ANY($2) AND flushed_at = 0",
		schema.ID, rows).Scan(&dirty); err != nil {
		e.T.Fatalf("count the onboarded rows' unflushed entries of schema %d: %v", schema.ID, err)
	}
	if dirty != 0 {
		e.T.Fatalf("schema %d: %d of %d onboarded rows still have an unflushed change_log entry after the cdc-init handoff",
			schema.ID, dirty, len(rows))
	}
}

// MarkAllOnboarded is MarkOnboarded for every schema the test initialized.
func (e *Env) MarkAllOnboarded(ctx context.Context) {
	e.T.Helper()
	if len(e.inits) == 0 {
		e.T.Fatal("MarkAllOnboarded: no schema was initialized by RunInit")
	}
	for _, ref := range DefaultSchemaFixtures() {
		if _, ok := e.inits[ref.ID]; ok {
			e.MarkOnboarded(ctx, ref)
		}
	}
}

// RetireFlushed checks that a flush left schema no unflushed change_log entry,
// so the read has no hot entry of the schema to serve.
func (e *Env) RetireFlushed(ctx context.Context, schema SchemaRef) {
	e.T.Helper()
	e.requireNoDirtyEntry(ctx, "schema_id = $1", schema.ID)
}

// RetireAllFlushed is RetireFlushed for every schema of the test database:
// after a flush, the read is parquet-only.
func (e *Env) RetireAllFlushed(ctx context.Context) {
	e.T.Helper()
	e.requireNoDirtyEntry(ctx, "TRUE")
}

// requireHandedOff fails the test unless schema's latest RunInit handed off
// every entry it exported: none left unmarked, none of uncertain outcome, and
// no later init or drop replacing its lineage mid-handoff.
func (e *Env) requireHandedOff(schema SchemaRef) {
	e.T.Helper()
	in, ok := e.inits[schema.ID]
	switch {
	case !ok:
		e.T.Fatalf("schema %d (%s) was not initialized by RunInit, so there is no handoff to rely on", schema.ID, schema.Name)
	case in.HandoffStopped || in.Unconfirmed != 0 || in.Marked != in.Entries:
		e.T.Fatalf("schema %d (%s): the cdc-init handoff marked %d of %d exported entries (unconfirmed %d, stopped %t)",
			schema.ID, schema.Name, in.Marked, in.Entries, in.Unconfirmed, in.HandoffStopped)
	}
}

// requireNoExportedRowDirty fails the test when a live row of schema (one
// cdc-init exports: not soft-deleted) still has an unflushed entry: the
// handoff marks every row the init exported, and the test wrote nothing since.
func (e *Env) requireNoExportedRowDirty(ctx context.Context, schema SchemaRef) {
	e.T.Helper()
	e.requireNoDirtyEntry(ctx, `schema_id = $1 AND EXISTS (SELECT 1 FROM entity_main m
		WHERE m.ltbase_schema_id = change_log.schema_id AND m.ltbase_row_id = change_log.row_id
		AND m.ltbase_deleted_at IS NULL)`, schema.ID)
}

// requireNoDirtyEntry fails the test when a change_log entry matching where
// is still unflushed.
func (e *Env) requireNoDirtyEntry(ctx context.Context, where string, args ...any) {
	e.T.Helper()
	var dirty int64
	if err := e.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM change_log WHERE flushed_at = 0 AND "+where, args...).Scan(&dirty); err != nil {
		e.T.Fatalf("count unflushed change_log entries (%s): %v", where, err)
	}
	if dirty != 0 {
		e.T.Fatalf("%d change_log entries (%s %v) are still unflushed; the V2 tools leave none there and no helper clears change_log",
			dirty, where, args)
	}
}
