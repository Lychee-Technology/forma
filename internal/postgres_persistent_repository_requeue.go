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

// buildRequeueMainStatement advances a row's version without touching any
// value column. It is the version half of buildUpdateMainStatement: the same
// GREATEST rule (#274), so the requeue's stamp is strictly above every copy
// already exported, and RETURNING hands change_log the identical stamp (#210)
// plus the row's delete state.
func buildRequeueMainStatement(table string) string {
	return fmt.Sprintf(
		"UPDATE %s SET ltbase_updated_at = GREATEST($1, ltbase_updated_at + 1) WHERE ltbase_schema_id = $2 AND ltbase_row_id = $3 RETURNING ltbase_updated_at, ltbase_deleted_at",
		sanitizeIdentifier(table),
	)
}

// RequeueForFlush marks an unchanged row dirty so the next CDC flush
// re-exports it from Postgres (#501). It is a version-only write: the row's
// values stay as they are, its version advances, and change_log gets a slot-0
// entry at that version.
//
// The version must advance, because touching change_log alone is not enough.
// Parquet ver_ts is change_log.changed_at. On an equal-ver_ts tie between two
// live parquet copies the merge picks an unspecified winner, since it assumes
// both copies hold the same values (compaction.mergeLWWOrderBy). A re-export
// at the old version could therefore lose to the stale copy it exists to
// replace. Once committed, the dirty set routes reads of the row to the hot
// tier until the flush lands. The one visible side effect is that the row's
// updatedAt advances.
//
// A missing entity_main row answers forma.NotFoundf, so a caller sweeping
// many rows can tell a vanished row from a storage failure.
func (r *DBPersistentRecordRepository) RequeueForFlush(ctx context.Context, tables model.StorageTables, schemaID int16, rowID uuid.UUID) error {
	if err := validateTables(tables); err != nil {
		return fmt.Errorf("validate tables for requeue: %w", err)
	}
	if tables.ChangeLog == "" {
		return fmt.Errorf("requeue row %s: change log table name is required: a requeue is a change_log entry", rowID)
	}

	// READ COMMITTED, as in MergePersistentRecord: the version read below
	// must see the commit of whichever writer held the lock before us.
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op if committed

	if err := lockRowVersion(ctx, tx, schemaID, rowID); err != nil {
		return fmt.Errorf("lock row version for requeue: %w", err)
	}

	var version int64
	var deletedAt *int64
	err = tx.QueryRow(ctx, buildRequeueMainStatement(tables.EntityMain), r.nowMillis(), schemaID, rowID).Scan(&version, &deletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return forma.WithOperatorDetail(forma.NotFoundf("entity not found (row=%s)", rowID),
			fmt.Errorf("schema=%d", schemaID))
	}
	if err != nil {
		return fmt.Errorf("advance version of %s: %w", rowID, classifyPgError(err))
	}

	if err := r.upsertChangeLog(ctx, tx, tables.ChangeLog, schemaID, rowID, version, deletedAt); err != nil {
		return fmt.Errorf("upsert requeue change log for %s: %w", rowID, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit requeue of %s: %w", rowID, err)
	}
	return nil
}
