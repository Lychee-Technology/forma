package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/redact"
	"github.com/lychee-technology/forma/internal/sqlutil"
	"go.uber.org/zap"
)

// redactConnStr replaces any password value in a connection string (or an SQL
// string that embeds one) with redact.Placeholder. It is safe to call on any
// string that may or may not contain a password.
//
// The matcher itself now lives in internal/redact, shared with the HTTP error
// boundary, which needs exactly the same scrub (#301) on driver text that quotes
// the postgres_scan connection string back at us. The #290 regression tests in
// redact_test.go exercise it through this wrapper and remain the contract.
func redactConnStr(s string) string {
	return redact.ConnStringPassword(s)
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

// SelectBatchRowIDs picks up to batchSize row_ids for flushing, recording
// each listed row's changed_at version, and returns a snapshot cutoff (ms)
// for exporting. The snapshot is the MAX of the wall clock and the listed
// versions: per-row versions are strictly monotonic and may run ahead of the
// wall clock (#274 GREATEST ordering), so a wall-clock-only cutoff would
// exclude a clock-ahead row from both export and mark — shipping an empty
// delta and leaving the row dirty until the clock caught up, indefinitely
// under sustained lead (review round 2 P1). The versions feed
// MarkFlushedVersions, which marks exactly what was listed.
func SelectBatchRowIDs(ctx context.Context, db *sql.DB, table string, schemaID int16, batchSize int) ([]uuid.UUID, map[uuid.UUID]int64, int64, error) {
	if table == "" {
		table = "change_log"
	}
	if batchSize <= 0 {
		batchSize = 10000
	}
	query := fmt.Sprintf("SELECT row_id, changed_at FROM %s WHERE schema_id = $1 AND flushed_at = 0 ORDER BY changed_at ASC LIMIT $2", sanitizeIdentifier(table))
	rows, err := db.QueryContext(ctx, query, schemaID, batchSize)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("select batch row ids: %w", err)
	}
	defer rows.Close()

	var ids []uuid.UUID
	versions := make(map[uuid.UUID]int64)
	snapshot := time.Now().UnixMilli()
	for rows.Next() {
		var id uuid.UUID
		var changedAt int64
		if err := rows.Scan(&id, &changedAt); err != nil {
			return nil, nil, 0, fmt.Errorf("scan row id: %w", err)
		}
		ids = append(ids, id)
		versions[id] = changedAt
		if changedAt > snapshot {
			snapshot = changedAt
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, 0, fmt.Errorf("iterate row ids: %w", err)
	}
	return ids, versions, snapshot, nil
}

// MarkFlushedVersions updates flushed_at for the given rows only where the
// slot-0 changed_at still EQUALS the version LISTED for that row at batch
// selection. Exact equality is what keeps export and mark consistent without
// a global wall-clock cutoff: any slot whose version differs from its
// listing was concurrently rewritten — advanced by an update/delete, or
// replaced by a delete→recreate — and its exported copy (if any) no longer
// matches slot-0, so the row must stay dirty for the next run. (`<=` is NOT
// safe here: a recreate overwrites the slot, and before #274's create
// ordering it could even land BELOW a clock-ahead tombstone's listing — a
// `<=` mark would clear the dirty barrier for a payload no parquet holds.)
// A clock-ahead listed version matches its own listing and marks normally
// (review round 2 P1). Returns the row_ids actually marked flushed.
func MarkFlushedVersions(ctx context.Context, db *sql.DB, table string, schemaID int16, rowIDs []uuid.UUID, versions map[uuid.UUID]int64, flushedAt int64) ([]uuid.UUID, error) {
	if table == "" {
		table = "change_log"
	}
	if len(rowIDs) == 0 {
		return nil, nil
	}
	valueRows := make([]string, 0, len(rowIDs))
	args := make([]any, 0, len(rowIDs)*2+2)
	args = append(args, flushedAt, schemaID)
	for _, id := range rowIDs {
		version, ok := versions[id]
		if !ok {
			return nil, fmt.Errorf("mark flushed: row %s has no listed version", id)
		}
		valueRows = append(valueRows, fmt.Sprintf("($%d::uuid, $%d::bigint)", len(args)+1, len(args)+2))
		args = append(args, id, version)
	}
	query := fmt.Sprintf(
		`UPDATE %s cl SET flushed_at = $1
		 FROM (VALUES %s) AS v(row_id, version)
		 WHERE cl.schema_id = $2 AND cl.flushed_at = 0
		   AND cl.row_id = v.row_id AND cl.changed_at = v.version
		 RETURNING cl.row_id`,
		sanitizeIdentifier(table),
		strings.Join(valueRows, ","),
	)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("mark flushed by listed versions: %w", err)
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
		// #226: the retry uses fresh UUIDv7 keys, so this tmp would be
		// unreachable garbage — best-effort delete before surfacing the copy
		// error. A failed cleanup is reclaimed by manifest-reconcile --gc.
		if _, delErr := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &tmpKey}); delErr != nil {
			logger.Sugar().Warnw("failed to delete tmp object after copy failure", "tmp_key", tmpKey, "err", delErr)
		}
		return fmt.Errorf("copy tmp->final: %w", err)
	}
	if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &tmpKey}); err != nil {
		logger.Sugar().Warnw("failed to delete tmp object", "err", err)
	}
	return nil
}

// DeleteObjectKey deletes one S3 object.
func DeleteObjectKey(ctx context.Context, client S3ObjectClient, bucket, key string) error {
	if client == nil {
		return fmt.Errorf("s3 client is nil")
	}
	if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &key}); err != nil {
		return fmt.Errorf("delete object %s: %w", key, err)
	}
	return nil
}

// HeadObjectSize returns the byte size of an S3 object. Callers use it to
// populate manifest FileEntry.SizeBytes after a tmp->final copy; the size
// feeds compaction's promotion heuristic only, so callers should treat a
// failure as best-effort (log and keep 0) rather than failing the pipeline.
func HeadObjectSize(ctx context.Context, client S3ObjectClient, bucket, key string) (int64, error) {
	if client == nil {
		return 0, fmt.Errorf("s3 client is nil")
	}
	out, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return 0, fmt.Errorf("head object %s: %w", key, err)
	}
	if out.ContentLength == nil {
		return 0, nil
	}
	return *out.ContentLength, nil
}

// sanitizeIdentifier performs a minimal whitelist for table names.
func sanitizeIdentifier(name string) string {
	return sqlutil.SanitizeIdentifier(name)
}
