package cdc

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lychee-technology/forma/internal/sqlutil"
)

// GetChangeLogStats returns count and oldest changed_at for unflushed rows.
func GetChangeLogStats(ctx context.Context, db *sql.DB, table string, schemaID int16) (int64, int64, error) {
	if table == "" {
		table = "change_log"
	}
	query := fmt.Sprintf("SELECT COUNT(*), COALESCE(MIN(changed_at),0) FROM %s WHERE schema_id = $1 AND flushed_at = 0", sanitizeIdentifier(table))
	row := db.QueryRowContext(ctx, query, schemaID)
	var cnt int64
	var oldest int64
	if err := row.Scan(&cnt, &oldest); err != nil {
		return 0, 0, fmt.Errorf("get stats: %w", err)
	}
	return cnt, oldest, nil
}

// sanitizeIdentifier performs a minimal whitelist for table names.
func sanitizeIdentifier(name string) string {
	return sqlutil.SanitizeIdentifier(name)
}
