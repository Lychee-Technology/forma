package cdc

import (
	"context"
	"database/sql"
	"fmt"
)

// getEntityMainCount returns the total number of non-deleted rows for a
// schema: what a cdc-init dry run reports it would export.
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
