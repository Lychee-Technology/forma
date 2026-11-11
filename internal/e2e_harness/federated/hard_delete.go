package federated

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// HardDeleteRecord transitions a LIVE hot row to a hard-delete tombstone, the
// way the production repository's delete does (#508): in one transaction it
// removes the row's entity_main and eav_data state and upserts an unflushed
// change_log tombstone whose changed_at and deleted_at are the same stamp.
//
// Seeding TestRecord.NoCreationStamp produces only the tombstone's final
// SHAPE for a fresh row id; it cannot express that an existing row was
// deleted, because it leaves that row's main and EAV state behind. This is
// the transition.
//
// The stamp is strictly after the deleted row's last update and no earlier
// than the clock read (computeTombstoneStamp in the production repository):
// per-row versions may run ahead of the wall clock (#274), and a tombstone
// stamped below the version it deletes would lose the LWW fold to it once
// both reach parquet. The stamp is returned so a caller can assert on it.
func (h *FederatedTestHarness) HardDeleteRecord(ctx context.Context, rowID uuid.UUID) (int64, error) {
	tx, err := h.PGDB.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin hard delete of row %s in schema %d: %w", rowID, h.SchemaID, err)
	}
	defer func() { _ = tx.Rollback() }()

	stamp, err := h.hardDeleteInTx(ctx, tx, rowID, time.Now().UnixMilli())
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit hard delete of row %s in schema %d: %w", rowID, h.SchemaID, err)
	}
	return stamp, nil
}

func (h *FederatedTestHarness) hardDeleteInTx(ctx context.Context, tx *sql.Tx, rowID uuid.UUID, nowMillis int64) (int64, error) {
	// RETURNING captures the live version the tombstone must outrank, and an
	// absent row is an error: hard-deleting a row that is not live would
	// silently mint a tombstone for nothing, which is the shape-only case
	// NoCreationStamp already covers.
	var prevUpdatedAt int64
	err := tx.QueryRowContext(ctx, `
		DELETE FROM entity_main
		WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2
		RETURNING ltbase_updated_at
	`, h.SchemaID, rowID).Scan(&prevUpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("hard delete row %s in schema %d: no live entity_main row", rowID, h.SchemaID)
	}
	if err != nil {
		return 0, fmt.Errorf("delete entity_main row %s in schema %d: %w", rowID, h.SchemaID, err)
	}

	if _, err := tx.ExecContext(ctx, `
		DELETE FROM eav_data WHERE schema_id = $1 AND row_id = $2
	`, h.SchemaID, rowID); err != nil {
		return 0, fmt.Errorf("delete eav_data rows of %s in schema %d: %w", rowID, h.SchemaID, err)
	}

	stamp := tombstoneStamp(nowMillis, prevUpdatedAt)
	tombstone := TestRecord{
		RowID:           rowID,
		SchemaID:        h.SchemaID,
		NoCreationStamp: true,
		ChangedAt:       stamp,
		DeletedAt:       stamp,
	}
	if err := h.insertChangeLog(ctx, tx, tombstone); err != nil {
		return 0, fmt.Errorf("write tombstone for %s in schema %d: %w", rowID, h.SchemaID, err)
	}
	return stamp, nil
}

// tombstoneStamp mirrors the production computeTombstoneStamp: strictly after
// the deleted row's last update, and no earlier than the clock read.
func tombstoneStamp(nowMillis, prevUpdatedAt int64) int64 {
	if prevUpdatedAt+1 > nowMillis {
		return prevUpdatedAt + 1
	}
	return nowMillis
}
