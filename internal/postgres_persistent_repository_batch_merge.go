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

	var stored []*model.PersistentRecord
	err := r.inWriteTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		var err error
		stored, err = r.batchMergeInTx(ctx, tx, tables, keys, merge)
		return err
	})
	if err != nil {
		return nil, err
	}
	return stored, nil
}

// batchMergeInTx is BatchMergePersistentRecords' body, run on tx: the sorted
// lock pass, every row's read, merge and unique-key plan in input order, one
// reservation over all of them, then every row's write in input order.
// Merging every row before writing any lets the keys be reserved in one
// sorted statement after the row locks, and a conflict found before the first
// write (#651). It answers the stored records positionally.
func (r *DBPersistentRecordRepository) batchMergeInTx(
	ctx context.Context,
	tx pgx.Tx,
	tables model.StorageTables,
	keys []model.PersistentRecordKey,
	merge model.PersistentRecordBatchMerge,
) ([]*model.PersistentRecord, error) {
	if err := lockRowKeys(ctx, tx, keys); err != nil {
		return nil, fmt.Errorf("lock row versions for batch update: %w", err)
	}

	merged := make([]*model.PersistentRecord, len(keys))
	claims := make([]*uniqueKeyClaim, len(keys))
	for i, key := range keys {
		record, claim, err := r.mergeOneOfBatch(ctx, tx, tables, i, key, merge)
		if err != nil {
			return nil, err
		}
		merged[i], claims[i] = record, claim
	}
	if _, err := r.reserveUniqueKeys(ctx, tx, tables, claims...); err != nil {
		return nil, fmt.Errorf("reserve unique keys for batch update: %w", err)
	}

	// Records in a batch usually share one schema; resolve the #294 delete
	// scope once per schemaID instead of once per record.
	scopeBySchema := make(map[int16][]int16)
	for i, key := range keys {
		record := merged[i]
		if err := r.updateMainRow(ctx, tx, tables.EntityMain, record); err != nil {
			return nil, fmt.Errorf("update main row for record[%d] (%s): %w", i, key.RowID, err)
		}
		if err := r.applyUniqueKeys(ctx, tx, tables, claims[i]); err != nil {
			return nil, fmt.Errorf("write unique keys for record[%d]: %w", i, err)
		}
		knownIDs, ok := scopeBySchema[key.SchemaID]
		if !ok {
			var err error
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
	}

	return merged, nil
}

// mergeOneOfBatch reads one row's merge base inside tx, runs merge on it and
// prepares the merged record's write (prepareUpdate), writing nothing. It
// answers the record and its unique-key claim; updateMainRow later stamps
// the effective version back into the record.
func (r *DBPersistentRecordRepository) mergeOneOfBatch(
	ctx context.Context,
	tx pgx.Tx,
	tables model.StorageTables,
	i int,
	key model.PersistentRecordKey,
	merge model.PersistentRecordBatchMerge,
) (*model.PersistentRecord, *uniqueKeyClaim, error) {
	existing, err := r.loadRecordWithAttributes(ctx, tx, tables, key.SchemaID, key.RowID)
	if err != nil {
		return nil, nil, fmt.Errorf("load merge base for record[%d] (%s): %w", i, key.RowID, err)
	}

	// A plain wrap, never WrapPublicf, for the reason given on
	// MergePersistentRecord: merge's error usually carries the published
	// message, and this wrap must stay out of the body (#313).
	record, err := merge(ctx, i, existing)
	if err != nil {
		return nil, nil, fmt.Errorf("merge record for %s: %w", key.RowID, err)
	}
	if record == nil {
		return nil, nil, fmt.Errorf("merge returned no record for record[%d] (%s)", i, key.RowID)
	}
	if record.SchemaID != key.SchemaID || record.RowID != key.RowID {
		return nil, nil, fmt.Errorf("merge returned record for %d/%s, expected %d/%s at record[%d]",
			record.SchemaID, record.RowID, key.SchemaID, key.RowID, i)
	}
	claim, err := r.prepareUpdate(ctx, tx, tables, record, existing)
	if err != nil {
		return nil, nil, fmt.Errorf("prepare update of record[%d] (%s): %w", i, key.RowID, err)
	}
	return record, claim, nil
}
