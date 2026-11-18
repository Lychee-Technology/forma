package production

import (
	"context"

	"github.com/google/uuid"
)

// The helpers below are the only place the harness clears change_log (#901).
// Under Protocol V1 every one of them is the same stand-in: deleting the
// entries takes the rows out of the dirty set, so the federated read serves
// them from parquet alone. They are split by what the test means, because
// Protocol V2 never clears change_log (docs/publication-protocol-v2.md, "The
// cdc-init handoff") and gives each intent a different replacement:
//
//   - onboarded: cdc-init exported the rows to base, and its handoff makes the
//     base object their only source. V2 replaces the delete with the real
//     handoff, which marks the entries at their export versions.
//   - flushed: a flush already exported and marked the rows, and the test
//     drops the marked entries so no hot entry is left at all.
//
// Each helper affects the same change_log rows its call sites did before
// #901, scoped the same way (one schema, some rows, or every schema). The
// statements are the same too, except that the two call sites which deleted
// a single row by scalar equality (row_id = $2) now go through
// MarkRowsOnboarded's row_id = ANY($2) with a one-element array.

// MarkOnboarded stands in for the cdc-init onboarding handoff of schema: after
// RunInit exported the schema's rows to base, the base parquet becomes their
// only source.
func (e *Env) MarkOnboarded(ctx context.Context, schema SchemaRef) {
	e.T.Helper()
	e.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", schema.ID)
}

// MarkRowsOnboarded is MarkOnboarded restricted to rows: those rows leave the
// dirty set and are served from base, while the schema's other rows stay hot.
func (e *Env) MarkRowsOnboarded(ctx context.Context, schema SchemaRef, rows []uuid.UUID) {
	e.T.Helper()
	e.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1 AND row_id = ANY($2)", schema.ID, rows)
}

// MarkAllOnboarded is MarkOnboarded for every schema of the test database, for
// tests that onboard through RunInitKeepingDelta and then read cold-only.
func (e *Env) MarkAllOnboarded(ctx context.Context) {
	e.T.Helper()
	e.ExecSQL(ctx, "DELETE FROM change_log")
}

// RetireFlushed drops schema's change_log entries after a flush exported
// them, so the read has no hot entry of the schema left to serve.
func (e *Env) RetireFlushed(ctx context.Context, schema SchemaRef) {
	e.T.Helper()
	e.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", schema.ID)
}

// RetireAllFlushed is RetireFlushed for every schema of the test database:
// after a flush, the read is cold-only and routes through DuckDB.
func (e *Env) RetireAllFlushed(ctx context.Context) {
	e.T.Helper()
	e.ExecSQL(ctx, "DELETE FROM change_log")
}
