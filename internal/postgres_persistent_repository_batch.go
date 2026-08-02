package internal

import (
	"context"
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
		return err
	}

	now := r.nowMillis()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op if committed

	for i, record := range records {
		if record == nil {
			return fmt.Errorf("record[%d] cannot be nil", i)
		}

		record.CreatedAt = now
		record.UpdatedAt = now

		if err := r.insertMainRow(ctx, tx, tables.EntityMain, record); err != nil {
			return fmt.Errorf("insert main row for record[%d]: %w", i, err)
		}
		if err := r.insertEAVAttributes(ctx, tx, tables.EAVData, record.OtherAttributes); err != nil {
			return fmt.Errorf("insert eav attributes for record[%d]: %w", i, err)
		}
		if tables.ChangeLog != "" {
			if err := r.upsertChangeLog(ctx, tx, tables.ChangeLog, record.SchemaID, record.RowID, record.CreatedAt, record.DeletedAt); err != nil {
				return fmt.Errorf("upsert change log for record[%d]: %w", i, err)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func (r *DBPersistentRecordRepository) BatchUpdatePersistentRecords(ctx context.Context, tables model.StorageTables, records []*model.PersistentRecord) error {
	if len(records) == 0 {
		return nil
	}
	if err := validateWriteTables(tables); err != nil {
		return err
	}

	now := r.nowMillis()
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op if committed

	// Records in a batch usually share one schema; resolve the #294 delete
	// scope once per schemaID instead of once per record.
	scopeBySchema := make(map[int16][]int16)

	for i, record := range records {
		if record == nil {
			return fmt.Errorf("record[%d] cannot be nil", i)
		}

		record.UpdatedAt = now

		if err := r.updateMainRow(ctx, tx, tables.EntityMain, record); err != nil {
			return fmt.Errorf("update main row for record[%d]: %w", i, err)
		}
		knownIDs, ok := scopeBySchema[record.SchemaID]
		if !ok {
			var err error
			knownIDs, err = r.knownAttrIDs(record.SchemaID)
			if err != nil {
				return fmt.Errorf("resolve replace scope for record[%d]: %w", i, err)
			}
			scopeBySchema[record.SchemaID] = knownIDs
		}
		if err := r.replaceEAVAttributesScoped(ctx, tx, tables.EAVData, record.SchemaID, record.RowID, record.OtherAttributes, knownIDs); err != nil {
			return fmt.Errorf("replace eav attributes for record[%d]: %w", i, err)
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
		return err
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op if committed

	deleteMain := fmt.Sprintf("DELETE FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2", sanitizeIdentifier(tables.EntityMain))
	deleteEAV := fmt.Sprintf("DELETE FROM %s WHERE schema_id = $1 AND row_id = $2", sanitizeIdentifier(tables.EAVData))

	now := r.nowMillis()
	for i, key := range keys {
		if key.SchemaID <= 0 {
			return fmt.Errorf("key[%d] has invalid schema id %d", i, key.SchemaID)
		}
		if key.RowID == uuid.Nil {
			return fmt.Errorf("key[%d] has empty row id", i)
		}

		tag, err := tx.Exec(ctx, deleteMain, key.SchemaID, key.RowID)
		if err != nil {
			return fmt.Errorf("delete entity_main row for key[%d]: %w", i, err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("entity not found for key[%d] (schema=%d row=%s): %w", i, key.SchemaID, key.RowID, forma.ErrNotFound)
		}
		if _, err := tx.Exec(ctx, deleteEAV, key.SchemaID, key.RowID); err != nil {
			return fmt.Errorf("delete eav row for key[%d]: %w", i, err)
		}
		if tables.ChangeLog != "" {
			deletedAt := now
			if err := r.upsertChangeLog(ctx, tx, tables.ChangeLog, key.SchemaID, key.RowID, now, &deletedAt); err != nil {
				return fmt.Errorf("upsert change log for key[%d]: %w", i, err)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}
