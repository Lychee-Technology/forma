package internal

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// The repository half of a claim (#653). A claim never waits for a lock
// another Forma transaction holds: it takes each candidate's per-row
// advisory lock with pg_try_advisory_xact_lock, the key lockRowVersion uses,
// and skips a candidate whose lock is taken. Every Forma writer and
// asserter takes that lock before it touches the row, so a lock this
// transaction got means no other Forma transaction is writing the row, and
// the re-check's row lock is then free of them too. The re-check still says
// SKIP LOCKED, so a row lock taken outside Forma is skipped rather than
// waited on. A try-lock waits for nothing, so it joins no deadlock cycle.

// TryLockRows takes, without waiting, the per-row advisory lock of each of
// rowIDs, and answers the rows it got in rowIDs order. A lock another
// transaction holds is skipped; one this transaction already holds is got
// again. The locks it got are held until the transaction ends.
func (b *txBoundRepository) TryLockRows(ctx context.Context, schemaID int16, rowIDs []uuid.UUID) ([]uuid.UUID, error) {
	taken, err := tryLockRowsInTx(ctx, b.tx, schemaID, rowIDs)
	if err != nil {
		return nil, classifyStatementError(err)
	}
	return taken, nil
}

// RecheckHeld answers the rows of rowIDs that exist and satisfy condition,
// in rowIDs order, taking their entity_main row locks and skipping any row
// whose lock another transaction holds. The caller holds each row's
// advisory lock, so under READ COMMITTED the statement sees every Forma
// writer's commit to the rows (#457).
func (b *txBoundRepository) RecheckHeld(ctx context.Context, tables model.StorageTables, schemaID int16, rowIDs []uuid.UUID, condition *renderedGuard) ([]uuid.UUID, error) {
	held, err := matchingRowsInTx(ctx, b.tx, tables, schemaID, rowIDs, condition, "FOR UPDATE OF m SKIP LOCKED")
	if err != nil {
		return nil, classifyStatementError(err)
	}
	return held, nil
}

// StillMatching answers the rows of rowIDs that satisfy condition as the
// transaction now sees them, its own writes included, locking nothing.
func (b *txBoundRepository) StillMatching(ctx context.Context, tables model.StorageTables, schemaID int16, rowIDs []uuid.UUID, condition *renderedGuard) ([]uuid.UUID, error) {
	matching, err := matchingRowsInTx(ctx, b.tx, tables, schemaID, rowIDs, condition, "")
	if err != nil {
		return nil, classifyStatementError(err)
	}
	return matching, nil
}

func tryLockRowsInTx(ctx context.Context, tx pgx.Tx, schemaID int16, rowIDs []uuid.UUID) ([]uuid.UUID, error) {
	if len(rowIDs) == 0 {
		return nil, nil
	}
	keys := make([]int64, len(rowIDs))
	for i, id := range rowIDs {
		keys[i] = rowVersionLockKey(schemaID, id)
	}
	rows, err := tx.Query(ctx,
		"SELECT t.i, pg_try_advisory_xact_lock(t.k) FROM unnest($1::bigint[]) WITH ORDINALITY AS t(k, i)", keys)
	if err != nil {
		return nil, fmt.Errorf("try the row locks of %d candidates: %w", len(rowIDs), err)
	}
	got := make([]bool, len(rowIDs))
	var i int64
	var locked bool
	if _, err := pgx.ForEachRow(rows, []any{&i, &locked}, func() error {
		got[i-1] = locked
		return nil
	}); err != nil {
		return nil, fmt.Errorf("read the row locks of %d candidates: %w", len(rowIDs), err)
	}
	taken := make([]uuid.UUID, 0, len(rowIDs))
	for j, id := range rowIDs {
		if got[j] {
			taken = append(taken, id)
		}
	}
	return taken, nil
}

// matchingRowsInTx answers the rows of rowIDs that exist and satisfy
// condition, in rowIDs order. locking is the statement's locking clause, or
// empty.
func matchingRowsInTx(ctx context.Context, tx pgx.Tx, tables model.StorageTables, schemaID int16, rowIDs []uuid.UUID, condition *renderedGuard, locking string) ([]uuid.UUID, error) {
	if len(rowIDs) == 0 {
		return nil, nil
	}
	query := fmt.Sprintf("SELECT m.ltbase_row_id FROM %s m WHERE m.ltbase_schema_id = $1 AND m.ltbase_row_id = ANY($2) AND (%s) %s",
		sanitizeIdentifier(tables.EntityMain), condition.clause, locking)
	args := append([]any{schemaID, rowIDs}, condition.args...)
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("evaluate the condition on %d rows: %w", len(rowIDs), err)
	}
	matching, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	if err != nil {
		return nil, fmt.Errorf("read the rows matching the condition: %w", err)
	}
	return inOrderOf(rowIDs, matching), nil
}

// inOrderOf answers the members of subset in the order ordered lists them.
func inOrderOf(ordered, subset []uuid.UUID) []uuid.UUID {
	in := make(map[uuid.UUID]struct{}, len(subset))
	for _, id := range subset {
		in[id] = struct{}{}
	}
	out := make([]uuid.UUID, 0, len(subset))
	for _, id := range ordered {
		if _, ok := in[id]; ok {
			out = append(out, id)
		}
	}
	return out
}
