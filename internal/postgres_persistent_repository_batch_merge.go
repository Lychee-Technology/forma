package internal

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/jackc/pgx/v5"
)

// BatchMergePersistentRecords is MergePersistentRecord over a batch (#554):
// the same guarded read-modify-write, with every row's merge base read
// inside the one write transaction after every row's advisory lock has been
// granted. The lock pass runs in sorted (schemaID, rowID) order so two
// overlapping batches cannot invert (see sortedRowKeys); reads, merges and
// writes then follow input order so the answer is positional.
//
// Before this the batch path read each base on the pool, outside any
// transaction, and wrote back with no lock: two overlapping batches, or a
// batch racing a guarded single-row Update, merged onto the same pre-write
// snapshot and the second committer's delete-all-then-reinsert replaced the
// first's document. READ COMMITTED is load-bearing here for the reason given
// on MergePersistentRecord: the post-lock read must see the previous holder's
// commit, which a snapshot taken at BEGIN would not.
func (r *DBPersistentRecordRepository) BatchMergePersistentRecords(
	ctx context.Context,
	tables model.StorageTables,
	keys []model.PersistentRecordKey,
	merge model.PersistentRecordBatchMerge,
) ([]*model.PersistentRecord, error) {
	if merge == nil {
		return nil, fmt.Errorf("merge function cannot be nil")
	}
	if len(keys) == 0 {
		return nil, nil
	}
	if err := validateWriteTables(tables); err != nil {
		return nil, fmt.Errorf("validate tables for batch update: %w", err)
	}
	if err := validateRecordKeys(keys); err != nil {
		return nil, err
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op if committed

	if err := lockRowKeys(ctx, tx, keys); err != nil {
		return nil, fmt.Errorf("lock row versions for batch update: %w", err)
	}

	// Records in a batch usually share one schema; resolve the #294 delete
	// scope once per schemaID instead of once per record.
	scopeBySchema := make(map[int16][]int16)
	stored := make([]*model.PersistentRecord, len(keys))
	for i, key := range keys {
		record, err := r.mergeOneOfBatch(ctx, tx, tables, i, key, merge)
		if err != nil {
			return nil, err
		}
		knownIDs, ok := scopeBySchema[key.SchemaID]
		if !ok {
			knownIDs, err = r.knownAttrIDs(key.SchemaID)
			if err != nil {
				return nil, fmt.Errorf("resolve replace scope for record[%d]: %w", i, err)
			}
			scopeBySchema[key.SchemaID] = knownIDs
		}
		if err := r.replaceEAVAttributesScoped(ctx, tx, tables.EAVData, key.SchemaID, key.RowID, record.OtherAttributes, knownIDs); err != nil {
			return nil, fmt.Errorf("replace eav attributes for record[%d]: %w", i, err)
		}
		if tables.ChangeLog != "" {
			if err := r.upsertChangeLog(ctx, tx, tables.ChangeLog, key.SchemaID, key.RowID, record.UpdatedAt, record.DeletedAt); err != nil {
				return nil, fmt.Errorf("upsert change log for record[%d]: %w", i, err)
			}
		}
		stored[i] = record
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}
	return stored, nil
}

// mergeOneOfBatch reads one row's merge base inside tx, runs merge on it and
// applies the main-table update, which stamps the effective version back
// into the returned record (see updateMainRow).
func (r *DBPersistentRecordRepository) mergeOneOfBatch(
	ctx context.Context,
	tx pgx.Tx,
	tables model.StorageTables,
	i int,
	key model.PersistentRecordKey,
	merge model.PersistentRecordBatchMerge,
) (*model.PersistentRecord, error) {
	existing, err := r.loadRecordWithAttributes(ctx, tx, tables, key.SchemaID, key.RowID)
	if err != nil {
		return nil, fmt.Errorf("load merge base for record[%d] (%s): %w", i, key.RowID, err)
	}

	// A plain wrap, never WrapPublicf, for the reason given on
	// MergePersistentRecord: merge's error usually carries the published
	// message, and this wrap must stay out of the body (#313).
	record, err := merge(ctx, i, existing)
	if err != nil {
		return nil, fmt.Errorf("merge record for %s: %w", key.RowID, err)
	}
	if record == nil {
		return nil, fmt.Errorf("merge returned no record for record[%d] (%s)", i, key.RowID)
	}
	if record.SchemaID != key.SchemaID || record.RowID != key.RowID {
		return nil, fmt.Errorf("merge returned record for %d/%s, expected %d/%s at record[%d]",
			record.SchemaID, record.RowID, key.SchemaID, key.RowID, i)
	}

	record.UpdatedAt = r.nowMillis()
	if err := r.updateMainRow(ctx, tx, tables.EntityMain, record); err != nil {
		return nil, fmt.Errorf("update main row for record[%d] (%s): %w", i, key.RowID, err)
	}
	return record, nil
}
