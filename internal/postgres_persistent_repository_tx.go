package internal

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// inWriteTx runs body in one transaction and commits it. Every repository
// write runs through here, which is what makes #645's first invariant
// structural: no 40001, 40P01 or 55P03 raised by any statement of a write
// path reaches a caller unclassified, including a statement added later.
// Classifying at each statement could not promise that. A 55P03 under
// lock_timeout comes from the advisory-lock statement, and a deadlock can
// surface on any statement.
//
// A body error goes through classifyStatementError and a COMMIT error
// through classifyCommitError (storage_outcome.go). The deferred rollback is a
// no-op once COMMIT has been attempted, and is what releases the
// transaction's locks on every early return.
func (r *DBPersistentRecordRepository) inWriteTx(ctx context.Context, opts pgx.TxOptions, body func(tx pgx.Tx) error) error {
	tx, err := r.pool.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", classifyStatementError(err))
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op if committed

	if err := body(tx); err != nil {
		return classifyStatementError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", classifyCommitError(err))
	}
	return nil
}
