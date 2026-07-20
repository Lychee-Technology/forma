package federated

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/cdc"
)

// RunCDCFlush triggers a CDC flush operation.
func (h *FederatedTestHarness) RunCDCFlush(ctx context.Context) (*FlushResult, error) {
	start := time.Now()

	locked, unlock, err := h.tryAcquireSchemaLock(ctx)
	if err != nil {
		return nil, err
	}
	if !locked {
		return &FlushResult{Flushed: false, Duration: time.Since(start)}, nil
	}
	defer unlock()

	// Get unflushed count before
	countBefore, _, err := h.GetChangeLogStats(ctx)
	if err != nil {
		return nil, err
	}

	if countBefore == 0 {
		return &FlushResult{Flushed: false, Duration: time.Since(start)}, nil
	}

	// Get batch of unflushed row IDs
	rowIDs, err := h.getUnflushedRowIDs(ctx)
	if err != nil {
		return nil, err
	}

	if len(rowIDs) == 0 {
		return &FlushResult{Flushed: false, Duration: time.Since(start)}, nil
	}

	// Create test records from the row IDs
	records := make([]TestRecord, len(rowIDs))
	for i, id := range rowIDs {
		records[i] = TestRecord{
			RowID:     id,
			SchemaID:  h.SchemaID,
			ChangedAt: time.Now().UnixMilli(),
		}
	}

	// Write to delta parquet
	filename := fmt.Sprintf("delta_%s.parquet", uuid.NewString()[:8])
	if err := h.WriteParquet(ctx, "delta", filename, records); err != nil {
		return nil, fmt.Errorf("write delta parquet: %w", err)
	}

	// Mark as flushed
	rowsFlushed, err := h.markRowsFlushed(ctx, rowIDs)
	if err != nil {
		return nil, err
	}

	return &FlushResult{
		Flushed:      rowsFlushed > 0,
		RowsFlushed:  rowsFlushed,
		FilesCreated: []string{filename},
		Duration:     time.Since(start),
	}, nil
}

func (h *FederatedTestHarness) tryAcquireSchemaLock(ctx context.Context) (bool, func(), error) {
	return cdc.TrySchemaLock(ctx, h.PGDB, h.SchemaID)
}

// getUnflushedRowIDs fetches unflushed row IDs from change_log up to batch size.
func (h *FederatedTestHarness) getUnflushedRowIDs(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := h.PGDB.QueryContext(ctx, `
		SELECT row_id FROM change_log 
		WHERE schema_id = $1 AND flushed_at = 0
		LIMIT $2
	`, h.SchemaID, h.CDCConfig.BatchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rowIDs []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan row id: %w", err)
		}
		rowIDs = append(rowIDs, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate unflushed row ids: %w", err)
	}
	return rowIDs, nil
}

// markRowsFlushed marks rows as flushed in the change_log.
func (h *FederatedTestHarness) markRowsFlushed(ctx context.Context, rowIDs []uuid.UUID) (int64, error) {
	flushedAt := time.Now().UnixMilli()
	var updated int64
	for _, id := range rowIDs {
		res, err := h.PGDB.ExecContext(ctx, `
				UPDATE change_log SET flushed_at = $1 
				WHERE schema_id = $2 AND row_id = $3 AND flushed_at = 0
			`, flushedAt, h.SchemaID, id)
		if err != nil {
			return 0, fmt.Errorf("mark row flushed: %w", err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("read rows affected: %w", err)
		}
		updated += affected
	}
	return updated, nil
}

// RunCompaction triggers a compaction operation.
func (h *FederatedTestHarness) RunCompaction(ctx context.Context) (*CompactionResult, error) {
	start := time.Now()

	// List delta files
	deltaFiles, err := h.ListParquetFiles(ctx, "delta")
	if err != nil {
		return nil, err
	}

	if len(deltaFiles) == 0 {
		return &CompactionResult{Duration: time.Since(start)}, nil
	}

	// Read all delta files and merge
	allRecords, err := h.readDeltaFiles(ctx, deltaFiles)
	if err != nil {
		return nil, err
	}

	// Write merged base file and delete delta files
	if len(allRecords) > 0 {
		if err := h.WriteParquet(ctx, "base", "compacted_base.parquet", allRecords); err != nil {
			return nil, err
		}

		h.deleteDeltaFiles(ctx, deltaFiles)
	}

	return &CompactionResult{
		FilesCompacted: len(deltaFiles),
		FilesCreated:   1,
		RowsMerged:     int64(len(allRecords)),
		Duration:       time.Since(start),
	}, nil
}

// readDeltaFiles reads all records from delta parquet files.
func (h *FederatedTestHarness) readDeltaFiles(ctx context.Context, deltaFiles []string) ([]TestRecord, error) {
	var allRecords []TestRecord
	for _, f := range deltaFiles {
		if err := func(file string) error {
			s3Path := fmt.Sprintf("s3://%s/%s", h.S3Bucket, file)
			rows, err := h.Duck.DB.QueryContext(ctx, fmt.Sprintf(`
				SELECT row_id, schema_id, changed_at, deleted_at, name, version
				FROM read_parquet('%s')
			`, s3Path))
			if err != nil {
				return nil
			}
			defer rows.Close()

			for rows.Next() {
				var rowID string
				var schemaID int16
				var changedAt, deletedAt int64
				var name sql.NullString
				var version sql.NullInt64

				if err := rows.Scan(&rowID, &schemaID, &changedAt, &deletedAt, &name, &version); err != nil {
					return fmt.Errorf("scan row: %w", err)
				}

				rec := TestRecord{
					RowID:     uuid.MustParse(rowID),
					SchemaID:  schemaID,
					ChangedAt: changedAt,
					DeletedAt: deletedAt,
					Attributes: map[string]any{
						"name":    name.String,
						"version": int(version.Int64),
					},
				}
				allRecords = append(allRecords, rec)
			}
			if err := rows.Err(); err != nil {
				return fmt.Errorf("iterate cdc records: %w", err)
			}
			return nil
		}(f); err != nil {
			return nil, err
		}
	}
	return allRecords, nil
}

// deleteDeltaFiles deletes the specified delta files from S3.
func (h *FederatedTestHarness) deleteDeltaFiles(ctx context.Context, deltaFiles []string) {
	for _, f := range deltaFiles {
		_, _ = h.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(h.S3Bucket),
			Key:    aws.String(f),
		})
	}
}

// GetChangeLogStats returns change_log statistics.
func (h *FederatedTestHarness) GetChangeLogStats(ctx context.Context) (count int64, oldestTs int64, err error) {
	err = h.PGDB.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(MIN(changed_at), 0)
		FROM change_log
		WHERE schema_id = $1 AND flushed_at = 0
	`, h.SchemaID).Scan(&count, &oldestTs)
	return
}

// CountUnflushedRecords returns the count of unflushed records.
func (h *FederatedTestHarness) CountUnflushedRecords(ctx context.Context) int {
	var count int
	_ = h.PGDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM change_log WHERE schema_id = $1 AND flushed_at = 0
	`, h.SchemaID).Scan(&count)
	return count
}
