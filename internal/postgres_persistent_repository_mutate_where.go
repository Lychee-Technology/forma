package internal

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// The repository half of a predicate mutation (#652): the candidate select
// runs Query's own SQL on the transaction, and the lock-then-recheck pass
// below decides which candidates are still the call's to write. The writes
// themselves are the single-row Update and Delete paths, run by the session.

// SelectCandidates answers the row IDs of the first limit rows matching
// query.Condition in query.AttributeOrders order, row ID ascending last, and
// the number of rows matching in all. It is QueryPersistentRecords run on the
// transaction, so the candidates are exactly the rows a Tx.Query with the
// same condition and sort answers, in its order.
func (b *txBoundRepository) SelectCandidates(ctx context.Context, query *model.PersistentRecordQuery) ([]uuid.UUID, int64, error) {
	page, err := b.QueryPersistentRecords(ctx, query)
	if err != nil {
		return nil, 0, fmt.Errorf("select candidates: %w", err)
	}
	ids := make([]uuid.UUID, len(page.Records))
	for i, record := range page.Records {
		ids[i] = record.RowID
	}
	return ids, page.TotalRecords, nil
}

// RenderCondition renders a predicate mutation's condition for the re-check,
// before any statement is sent, so a malformed operand fails without SQL.
func (b *txBoundRepository) RenderCondition(tables model.StorageTables, schemaID int16, guard model.RowGuard) (*renderedGuard, error) {
	rendered, err := b.repo.renderRowGuard(tables, schemaID, guard)
	if err != nil {
		return nil, fmt.Errorf("render mutation condition: %w", err)
	}
	return rendered, nil
}

// LockAndRecheck locks every candidate as a writer would and answers the
// candidates that, once locked, still exist and satisfy condition, in
// candidate order. See lockAndRecheckInTx.
func (b *txBoundRepository) LockAndRecheck(ctx context.Context, tables model.StorageTables, schemaID int16, candidates []uuid.UUID, condition *renderedGuard) ([]uuid.UUID, error) {
	survivors, err := lockAndRecheckInTx(ctx, b.tx, tables, schemaID, candidates, condition)
	if err != nil {
		return nil, classifyStatementError(err)
	}
	return survivors, nil
}

// lockAndRecheckInTx takes every candidate's per-row advisory lock in
// (schema_id, row_id) order, the order every batch writer takes them, so two
// predicate mutations never deadlock with each other whatever their Sort.
// It then re-evaluates condition on the locked rows in one statement, which
// also takes their entity_main row locks. Under READ COMMITTED that
// statement's snapshot postdates every lock grant, so it sees each previous
// holder's commit (#457): a candidate a concurrent writer changed so that it
// no longer matches, or deleted, is dropped. A row the candidate select did
// not see is never locked, so it is never written.
func lockAndRecheckInTx(ctx context.Context, tx pgx.Tx, tables model.StorageTables, schemaID int16, candidates []uuid.UUID, condition *renderedGuard) ([]uuid.UUID, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	keys := make([]model.PersistentRecordKey, len(candidates))
	for i, id := range candidates {
		keys[i] = model.PersistentRecordKey{SchemaID: schemaID, RowID: id}
	}
	if err := lockRowKeys(ctx, tx, keys); err != nil {
		return nil, fmt.Errorf("lock %d candidates: %w", len(candidates), err)
	}

	survivors, err := matchingRowsInTx(ctx, tx, tables, schemaID, candidates, condition, "FOR UPDATE OF m")
	if err != nil {
		return nil, fmt.Errorf("re-check %d locked candidates: %w", len(candidates), err)
	}
	return survivors, nil
}
