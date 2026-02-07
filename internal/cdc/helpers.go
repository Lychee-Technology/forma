package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// AcquireSchemaLock tries to grab an advisory lock for the schema to avoid
// concurrent flush/compaction on the same schema.
func AcquireSchemaLock(ctx context.Context, db *sql.DB, schemaID int16) (bool, error) {
	row := db.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1, $2)", int32(schemaID), int32(schemaID))
	var locked bool
	if err := row.Scan(&locked); err != nil {
		return false, fmt.Errorf("acquire lock: %w", err)
	}
	return locked, nil
}

// ReleaseSchemaLock releases the advisory lock for the schema.
func ReleaseSchemaLock(ctx context.Context, db *sql.DB, schemaID int16) error {
	if _, err := db.ExecContext(ctx, "SELECT pg_advisory_unlock($1, $2)", int32(schemaID), int32(schemaID)); err != nil {
		return fmt.Errorf("release lock: %w", err)
	}
	return nil
}

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

// SelectBatchRowIDs picks up to batchSize row_ids for flushing and returns a
// snapshot cutoff (ms) used for exporting.
func SelectBatchRowIDs(ctx context.Context, db *sql.DB, table string, schemaID int16, batchSize int) ([]uuid.UUID, int64, error) {
	if table == "" {
		table = "change_log"
	}
	if batchSize <= 0 {
		batchSize = 10000
	}
	query := fmt.Sprintf("SELECT row_id FROM %s WHERE schema_id = $1 AND flushed_at = 0 ORDER BY changed_at ASC LIMIT $2", sanitizeIdentifier(table))
	rows, err := db.QueryContext(ctx, query, schemaID, batchSize)
	if err != nil {
		return nil, 0, fmt.Errorf("select batch row ids: %w", err)
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, 0, fmt.Errorf("scan row id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate row ids: %w", err)
	}
	snapshot := time.Now().UnixMilli()
	return ids, snapshot, nil
}

// MarkFlushed updates flushed_at for rows up to the snapshot.
func MarkFlushed(ctx context.Context, db *sql.DB, table string, schemaID int16, snapshot int64, flushedAt int64) (int64, error) {
	if table == "" {
		table = "change_log"
	}
	query := fmt.Sprintf("UPDATE %s SET flushed_at = $1 WHERE schema_id = $2 AND flushed_at = 0 AND changed_at <= $3", sanitizeIdentifier(table))
	res, err := db.ExecContext(ctx, query, flushedAt, schemaID, snapshot)
	if err != nil {
		return 0, fmt.Errorf("mark flushed: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}
	return affected, nil
}

// MarkFlushedIDsAtSnapshot updates flushed_at for specific row_ids only if their changed_at
// is at or before the snapshot. It returns the row_ids actually marked flushed.
func MarkFlushedIDsAtSnapshot(ctx context.Context, db *sql.DB, table string, schemaID int16, rowIDs []uuid.UUID, snapshot int64, flushedAt int64) ([]uuid.UUID, error) {
	if table == "" {
		table = "change_log"
	}
	if len(rowIDs) == 0 {
		return nil, nil
	}
	placeholders := make([]string, 0, len(rowIDs))
	args := make([]any, 0, len(rowIDs)+3)
	args = append(args, flushedAt, schemaID, snapshot)
	for i, id := range rowIDs {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+4))
		args = append(args, id)
	}
	query := fmt.Sprintf(
		"UPDATE %s SET flushed_at = $1 WHERE schema_id = $2 AND flushed_at = 0 AND changed_at <= $3 AND row_id IN (%s) RETURNING row_id",
		sanitizeIdentifier(table),
		strings.Join(placeholders, ","),
	)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("mark flushed by ids with snapshot: %w", err)
	}
	defer rows.Close()

	var updated []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan flushed row id: %w", err)
		}
		updated = append(updated, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate flushed row ids: %w", err)
	}
	return updated, nil
}

// CopyTmpToFinal copies a parquet file from tmp key to final key and deletes tmp.
func CopyTmpToFinal(ctx context.Context, client S3ObjectClient, bucket, tmpKey, finalKey string, logger *zap.Logger) error {
	if client == nil {
		return fmt.Errorf("s3 client is nil")
	}
	src := fmt.Sprintf("%s/%s", bucket, strings.TrimPrefix(tmpKey, "/"))
	if _, err := client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     &bucket,
		CopySource: &src,
		Key:        &finalKey,
	}); err != nil {
		return fmt.Errorf("copy tmp->final: %w", err)
	}
	if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &tmpKey}); err != nil {
		logger.Sugar().Warnw("failed to delete tmp object", "err", err)
	}
	return nil
}

// sanitizeIdentifier performs a minimal whitelist for table names.
func sanitizeIdentifier(name string) string {
	if name == "" {
		return ""
	}
	return strings.ReplaceAll(name, "`", "")
}
