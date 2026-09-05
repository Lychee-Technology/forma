package cdc

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
)

// getEntityMainCount returns the total number of non-deleted rows for a schema.
func getEntityMainCount(ctx context.Context, db *sql.DB, tableName string, schemaID int16) (int64, error) {
	if tableName == "" {
		tableName = "entity_main"
	}
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE ltbase_schema_id = $1 AND ltbase_deleted_at IS NULL",
		sanitizeIdentifier(tableName))
	row := db.QueryRowContext(ctx, query, schemaID)
	var cnt int64
	if err := row.Scan(&cnt); err != nil {
		return 0, fmt.Errorf("count rows: %w", err)
	}
	return cnt, nil
}

// selectEntityMainBatch returns a batch of row IDs ordered by ltbase_row_id,
// strictly after the `after` cursor (nil starts from the beginning). Keyset
// pagination, not LIMIT/OFFSET: init runs against a live table (TrySchemaLock
// excludes only flusher/init/reconcile, not CRUD), and with OFFSET a row
// deleted below the cursor shifts the window left and silently drops one live
// row from the wholesale-replaced base tier (#462). The row-id cursor is
// immune — membership changes below it cannot move the window.
func selectEntityMainBatch(ctx context.Context, db *sql.DB, tableName string, schemaID int16, after *uuid.UUID, limit int) ([]uuid.UUID, error) {
	if tableName == "" {
		tableName = "entity_main"
	}
	query := fmt.Sprintf(`
		SELECT ltbase_row_id
		FROM %s
		WHERE ltbase_schema_id = $1 AND ltbase_deleted_at IS NULL AND ($3::uuid IS NULL OR ltbase_row_id > $3)
		ORDER BY ltbase_row_id
		LIMIT $2`,
		sanitizeIdentifier(tableName))

	rows, err := db.QueryContext(ctx, query, schemaID, limit, after)
	if err != nil {
		return nil, fmt.Errorf("select batch: %w", err)
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan row id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate row ids: %w", err)
	}
	return ids, nil
}
