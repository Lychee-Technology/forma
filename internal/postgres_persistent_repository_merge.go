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
// The transaction pins READ COMMITTED explicitly rather than inheriting the
// cluster default, because here the level is load-bearing: a snapshot
// isolation level would freeze the waiter's view at BEGIN — before the lock
// was granted — and hand the merge the very stale base this exists to
// prevent, silently. READ COMMITTED takes a fresh snapshot per statement, so
// the post-lock read sees the previous holder's commit.
//
// Optimistic concurrency is the caller's attribute precondition (#649):
// guard is evaluated against the locked row before the merge base is read,
// and a row that fails it writes nothing and answers
// forma.ErrPreconditionFailed (412). The system ltbase_updated_at stamp is
// not a precondition: preconditions name schema attributes only.
func (r *DBPersistentRecordRepository) MergePersistentRecord(
	ctx context.Context,
	tables model.StorageTables,
	schemaID int16,
	rowID uuid.UUID,
	guard model.RowGuard,
	merge model.PersistentRecordMerge,
) (*model.PersistentRecord, error) {
	if merge == nil {
		return nil, fmt.Errorf("merge function cannot be nil")
	}
	if err := validateWriteTables(tables); err != nil {
		return nil, fmt.Errorf("validate tables for update: %w", err)
	}
	rendered, err := r.renderRowGuard(tables, schemaID, guard)
	if err != nil {
		return nil, fmt.Errorf("prepare update of %s: %w", rowID, err)
	}

	var stored *model.PersistentRecord
	err = r.inWriteTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		var err error
		stored, err = r.mergeRecordInTx(ctx, tx, tables, schemaID, rowID, rendered, merge)
		return err
	})
	if err != nil {
		return nil, err
	}
	return stored, nil
}

// mergeRecordInTx is MergePersistentRecord's body: lock, check the guard,
// read the merge base, merge and write, all on tx. guard was rendered before
// the write sent any statement; nil requires nothing. It answers the stored
// record.
func (r *DBPersistentRecordRepository) mergeRecordInTx(
	ctx context.Context,
	tx pgx.Tx,
	tables model.StorageTables,
	schemaID int16,
	rowID uuid.UUID,
	guard *renderedGuard,
	merge model.PersistentRecordMerge,
) (*model.PersistentRecord, error) {
	if err := lockRowVersion(ctx, tx, schemaID, rowID); err != nil {
		return nil, fmt.Errorf("lock row version for update: %w", err)
	}

	existing, err := r.loadGuardedMergeBase(ctx, tx, tables, schemaID, rowID, guard)
	if err != nil {
		return nil, fmt.Errorf("read merge base for %s: %w", rowID, err)
	}

	// A plain wrap, never WrapPublicf: merge's error is frequently a published
	// forma carrier (NotFoundf/InvalidInputf), and ResolvePublicMessage walks
	// past a plain wrapper to that inner publication, so the row context lands
	// in Error() and the log while the caller's message stays the body (#313).
	record, err := merge(ctx, existing, txClock(tx))
	if err != nil {
		return nil, fmt.Errorf("merge record for %s: %w", rowID, err)
	}
	if record == nil {
		return nil, fmt.Errorf("merge returned no record for row %s", rowID)
	}
	if record.SchemaID != schemaID || record.RowID != rowID {
		return nil, fmt.Errorf("merge returned record for %d/%s, expected %d/%s",
			record.SchemaID, record.RowID, schemaID, rowID)
	}

	// A key the update moves is reserved before the first entity write, so a
	// value another row holds leaves nothing written (#651).
	claim, err := r.prepareUpdate(ctx, tx, tables, record, existing)
	if err != nil {
		return nil, fmt.Errorf("prepare update of %s: %w", rowID, err)
	}
	if _, err := r.reserveUniqueKeys(ctx, tx, tables, claim); err != nil {
		return nil, fmt.Errorf("reserve unique keys of %s: %w", rowID, err)
	}
	if err := r.updateMainRow(ctx, tx, tables.EntityMain, record); err != nil {
		return nil, fmt.Errorf("update main row for %s: %w", rowID, err)
	}
	if err := r.applyUniqueKeys(ctx, tx, tables, claim); err != nil {
		return nil, fmt.Errorf("write unique keys of %s: %w", rowID, err)
	}

	if err := r.replaceEAVAttributes(ctx, tx, tables.EAVData, schemaID, rowID, record.OtherAttributes); err != nil {
		return nil, fmt.Errorf("replace eav attributes for %s: %w", rowID, err)
	}

	if tables.ChangeLog != "" {
		if err := r.upsertChangeLog(ctx, tx, tables.ChangeLog, schemaID, rowID, record.UpdatedAt, record.DeletedAt); err != nil {
			return nil, fmt.Errorf("upsert change log for %s: %w", rowID, err)
		}
	}

	return record, nil
}

// prepareUpdate makes merged the row its update stores and answers its
// unique-key claim; the caller holds the row's version lock and read
// existing, the merge base, under it. updateMainRow assigns only the main
// columns merged sets, so a column the merged document dropped, a shape
// report-only validation admits, keeps its stored value. merged takes that
// value from existing, so the claim keeps the entry the row still holds
// instead of releasing it (#651), and the update answers the value too.
// UpdatedAt is the clock read now; updateMainRow advances it past the row's
// previous version. Both merge cores call it, so they cannot drift.
func (r *DBPersistentRecordRepository) prepareUpdate(
	ctx context.Context, tx pgx.Tx, tables model.StorageTables, merged, existing *model.PersistentRecord,
) (*uniqueKeyClaim, error) {
	retainStoredColumns(merged, existing)
	merged.UpdatedAt = r.nowMillis()
	claim, err := r.planUniqueKeys(ctx, tx, tables, merged, true)
	if err != nil {
		return nil, fmt.Errorf("plan unique keys: %w", err)
	}
	return claim, nil
}

// loadGuardedMergeBase reads the merge base once guard holds. A row the
// guard finds missing answers nil without a second read, so the merge
// authors the caller's 404 as it does for an unguarded update.
func (r *DBPersistentRecordRepository) loadGuardedMergeBase(
	ctx context.Context,
	tx pgx.Tx,
	tables model.StorageTables,
	schemaID int16,
	rowID uuid.UUID,
	guard *renderedGuard,
) (*model.PersistentRecord, error) {
	if guard != nil {
		exists, err := checkRowGuard(ctx, tx, tables, schemaID, rowID, guard)
		if err != nil {
			return nil, fmt.Errorf("check precondition: %w", err)
		}
		if !exists {
			return nil, nil
		}
	}
	existing, err := r.loadRecordWithAttributes(ctx, tx, tables, schemaID, rowID)
	if err != nil {
		return nil, fmt.Errorf("load row: %w", err)
	}
	return existing, nil
}
