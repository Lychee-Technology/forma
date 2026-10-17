package internal

import (
	"context"
	"errors"
	"fmt"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
)

func (r *DBPersistentRecordRepository) BatchInsertPersistentRecords(ctx context.Context, tables model.StorageTables, records []*model.PersistentRecord) error {
	if len(records) == 0 {
		return nil
	}
	if err := validateWriteTables(tables); err != nil {
		return fmt.Errorf("validate tables for batch insert: %w", err)
	}

	// READ COMMITTED for the unique-key reservation; see
	// InsertPersistentRecord.
	now := r.nowMillis()
	return r.inWriteTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		return r.batchInsertInTx(ctx, tx, tables, records, now)
	})
}

// batchInsertInTx is BatchInsertPersistentRecords' body, run on tx.
func (r *DBPersistentRecordRepository) batchInsertInTx(ctx context.Context, tx pgx.Tx, tables model.StorageTables, records []*model.PersistentRecord, now int64) error {
	// Recreates must outrank their retained tombstone (#274); see
	// prepareCreate. The lock pass runs in sorted order before any write,
	// the stamp and write loops below in input order (#554).
	keys, err := recordKeys(records)
	if err != nil {
		return fmt.Errorf("validate batch insert: %w", err)
	}
	if err := lockRowKeys(ctx, tx, keys); err != nil {
		return fmt.Errorf("lock row versions for batch insert: %w", err)
	}
	// Every record is stamped and its unique keys planned before any is
	// reserved, so each entry holds the stamp its row is stored with. The
	// keys are then reserved in one sorted statement after every row lock
	// and before the first entity write (#651), so two records of the batch
	// taking one value conflict here too. A failure later rolls the batch's
	// own transaction back, reservation included.
	claims := make([]*uniqueKeyClaim, len(records))
	for i, record := range records {
		if claims[i], err = r.prepareCreate(ctx, tx, tables, record, now); err != nil {
			return fmt.Errorf("prepare create of record[%d]: %w", i, err)
		}
	}
	if _, err := r.reserveUniqueKeys(ctx, tx, tables, claims...); err != nil {
		return fmt.Errorf("reserve unique keys for batch insert: %w", err)
	}

	for i, record := range records {
		inserted, err := r.insertMainRow(ctx, tx, tables.EntityMain, record)
		if err != nil {
			return fmt.Errorf("insert main row for record[%d]: %w", i, err)
		}
		if !inserted {
			return fmt.Errorf("record[%d]: %w", i, r.rowIDConflict(record.SchemaID, record.RowID))
		}
		if err := r.applyUniqueKeys(ctx, tx, tables, claims[i]); err != nil {
			return fmt.Errorf("write unique keys for record[%d]: %w", i, err)
		}
		if err := r.insertEAVAttributes(ctx, tx, tables.EAVData, record.OtherAttributes); err != nil {
			return fmt.Errorf("insert eav attributes for record[%d]: %w", i, err)
		}
		if tables.ChangeLog != "" {
			if err := r.upsertChangeLog(ctx, tx, tables.ChangeLog, record.SchemaID, record.RowID, record.UpdatedAt, record.DeletedAt); err != nil {
				return fmt.Errorf("upsert change log for record[%d]: %w", i, err)
			}
		}
	}
	return nil
}

func (r *DBPersistentRecordRepository) BatchDeletePersistentRecords(ctx context.Context, tables model.StorageTables, keys []model.PersistentRecordKey) error {
	if len(keys) == 0 {
		return nil
	}
	if err := validateWriteTables(tables); err != nil {
		return fmt.Errorf("validate tables for batch delete: %w", err)
	}
	return r.inWriteTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		return r.batchDeleteInTx(ctx, tx, tables, keys)
	})
}

// batchDeleteInTx is BatchDeletePersistentRecords' body, run on tx.
func (r *DBPersistentRecordRepository) batchDeleteInTx(ctx context.Context, tx pgx.Tx, tables model.StorageTables, keys []model.PersistentRecordKey) error {
	// RETURNING + computeTombstoneStamp: see DeletePersistentRecord — the tombstone
	// must rank strictly after the row's (possibly clock-ahead) last update
	// or the delete loses the LWW tie (#274).
	deleteMain := fmt.Sprintf("DELETE FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2 RETURNING ltbase_updated_at", sanitizeIdentifier(tables.EntityMain))
	deleteEAV := fmt.Sprintf("DELETE FROM %s WHERE schema_id = $1 AND row_id = $2", sanitizeIdentifier(tables.EAVData))

	if err := validateRecordKeys(keys); err != nil {
		return err
	}
	// Sorted lock pass first, input-order writes after (#554).
	if err := lockRowKeys(ctx, tx, keys); err != nil {
		return fmt.Errorf("lock row versions for batch delete: %w", err)
	}

	now := r.nowMillis()
	for i, key := range keys {
		var prevUpdatedAt int64
		if err := tx.QueryRow(ctx, deleteMain, key.SchemaID, key.RowID).Scan(&prevUpdatedAt); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return forma.WithOperatorDetail(forma.NotFoundf("entity not found for key[%d] (row=%s)", i, key.RowID),
					fmt.Errorf("schema=%d", key.SchemaID))
			}
			return fmt.Errorf("delete entity_main row for key[%d]: %w", i, err)
		}
		if _, err := tx.Exec(ctx, deleteEAV, key.SchemaID, key.RowID); err != nil {
			return fmt.Errorf("delete eav row for key[%d]: %w", i, err)
		}
		if err := r.releaseRowUniqueKeys(ctx, tx, tables, key.SchemaID, key.RowID); err != nil {
			return fmt.Errorf("delete unique keys for key[%d]: %w", i, err)
		}
		if tables.ChangeLog != "" {
			stamp := computeTombstoneStamp(now, prevUpdatedAt)
			if err := r.upsertChangeLog(ctx, tx, tables.ChangeLog, key.SchemaID, key.RowID, stamp, &stamp); err != nil {
				return fmt.Errorf("upsert change log for key[%d]: %w", i, err)
			}
		}
	}
	return nil
}

// recordKeys projects a batch's records onto their (schemaID, rowID) keys for
// the lock pass, refusing a nil record before any lock is taken.
func recordKeys(records []*model.PersistentRecord) ([]model.PersistentRecordKey, error) {
	keys := make([]model.PersistentRecordKey, len(records))
	for i, record := range records {
		if record == nil {
			return nil, fmt.Errorf("record[%d] cannot be nil", i)
		}
		keys[i] = model.PersistentRecordKey{SchemaID: record.SchemaID, RowID: record.RowID}
	}
	return keys, nil
}

func validateRecordKeys(keys []model.PersistentRecordKey) error {
	for i, key := range keys {
		if key.SchemaID <= 0 {
			return fmt.Errorf("key[%d] has invalid schema id %d", i, key.SchemaID)
		}
		if key.RowID == uuid.Nil {
			return fmt.Errorf("key[%d] has empty row id", i)
		}
	}
	return nil
}
