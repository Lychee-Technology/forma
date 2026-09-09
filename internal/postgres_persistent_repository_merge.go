package internal

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// MergePersistentRecord performs a row's read-modify-write with the merge
// base read inside the write transaction, under the same (schema_id, row_id)
// advisory lock create and delete take (#457).
//
// The update path used to read its merge base on the pool, outside any
// transaction, and write back with no lock and no version assert: two
// concurrent updates to one row both merged onto the same pre-write snapshot,
// and because the EAV write is delete-all-then-reinsert, the second committer
// replaced the first committer's whole document. Locking first and reading
// second makes last-committer-wins operate per MERGED document — a waiter is
// released only after the previous holder commits, and re-reads that commit
// before merging.
//
// The transaction deliberately stays at the default READ COMMITTED. A
// snapshot isolation level would freeze the waiter's view at BEGIN — before
// the lock was granted — and hand the merge the very stale base this exists
// to prevent.
//
// The optimistic ltbase_updated_at predicate stays deferred: a failed assert
// needs a caller-facing conflict protocol (409 mapping, retry) that does not
// exist yet.
func (r *DBPersistentRecordRepository) MergePersistentRecord(
	ctx context.Context,
	tables model.StorageTables,
	schemaID int16,
	rowID uuid.UUID,
	merge model.PersistentRecordMerge,
) (*model.PersistentRecord, error) {
	if merge == nil {
		return nil, fmt.Errorf("merge function cannot be nil")
	}
	if err := validateWriteTables(tables); err != nil {
		return nil, fmt.Errorf("validate tables for update: %w", err)
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op if committed

	if err := lockRowVersion(ctx, tx, schemaID, rowID); err != nil {
		return nil, fmt.Errorf("lock row version for update: %w", err)
	}

	existing, err := r.loadRecordWithAttributes(ctx, tx, tables, schemaID, rowID)
	if err != nil {
		return nil, fmt.Errorf("load merge base for %s: %w", rowID, err)
	}

	// Deliberately a bare return rather than the usual wrap: merge is the
	// caller's own body, which has already attached its context, and its error
	// is frequently a published forma carrier whose message the API answers
	// with. A prefix here would land in that body.
	record, err := merge(ctx, existing)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, fmt.Errorf("merge returned no record for row %s", rowID)
	}
	if record.SchemaID != schemaID || record.RowID != rowID {
		return nil, fmt.Errorf("merge returned record for %d/%s, expected %d/%s",
			record.SchemaID, record.RowID, schemaID, rowID)
	}

	record.UpdatedAt = r.nowMillis()

	if err := r.updateMainRow(ctx, tx, tables.EntityMain, record); err != nil {
		return nil, fmt.Errorf("update main row for %s: %w", rowID, err)
	}

	if err := r.replaceEAVAttributes(ctx, tx, tables.EAVData, schemaID, rowID, record.OtherAttributes); err != nil {
		return nil, fmt.Errorf("replace eav attributes for %s: %w", rowID, err)
	}

	if tables.ChangeLog != "" {
		if err := r.upsertChangeLog(ctx, tx, tables.ChangeLog, schemaID, rowID, record.UpdatedAt, record.DeletedAt); err != nil {
			return nil, fmt.Errorf("upsert change log for %s: %w", rowID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	return record, nil
}
