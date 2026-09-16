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

	now := r.nowMillis()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op if committed

	// Recreates must outrank their retained tombstone (#274); see
	// InsertPersistentRecord. The lock pass runs in sorted order before any
	// write, the write loop below in input order (#554).
	keys, err := recordKeys(records)
	if err != nil {
		return err
	}
	if err := lockRowKeys(ctx, tx, keys); err != nil {
		return fmt.Errorf("lock row versions for batch insert: %w", err)
	}

	for i, record := range records {
		effective, err := nextRowVersion(ctx, tx, tables.ChangeLog, record.SchemaID, record.RowID, now)
		if err != nil {
			return fmt.Errorf("stamp create version for record[%d]: %w", i, err)
		}
		record.CreatedAt = now
		record.UpdatedAt = effective

		if err := r.insertMainRow(ctx, tx, tables.EntityMain, record); err != nil {
			return fmt.Errorf("insert main row for record[%d]: %w", i, err)
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

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
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

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op if committed

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
		if tables.ChangeLog != "" {
			stamp := computeTombstoneStamp(now, prevUpdatedAt)
			if err := r.upsertChangeLog(ctx, tx, tables.ChangeLog, key.SchemaID, key.RowID, stamp, &stamp); err != nil {
				return fmt.Errorf("upsert change log for key[%d]: %w", i, err)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
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
