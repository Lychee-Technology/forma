package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/manifest"
	"go.uber.org/zap"
)

type flushBatchExecutor struct {
	db               *sql.DB
	duck             *DuckExporter
	s3Client         S3ObjectClient
	cfg              CDCConfig
	tableName        string
	schemaID         int16
	snapshot         int64
	pgConnForDuck    string
	attrCache        forma.SchemaAttributeCache
	dryRun           bool
	logger           *zap.Logger
	manifestStore    manifest.Store
	manifestResolver manifest.PathResolver
	executeSingle    func(*flushBatchExecutor, []uuid.UUID) error
	executeInChunks  func(*flushBatchExecutor, []uuid.UUID, int) error
	exportSnapshot   func(*DuckExporter, context.Context, CDCConfig, string, string, int16, int64, []uuid.UUID, forma.SchemaAttributeCache) error
}

func (e *flushBatchExecutor) executeBatch(ctx context.Context, batchIDs []uuid.UUID, tmpKey string, finalKey string, batchKind string) error {
	if e.dryRun {
		// Decided before the first side effect: no DuckDB export (which
		// writes a _tmp S3 object), no tmp->final copy, no mark-flushed,
		// and no manifest update (#180). Log the work a real run would do.
		e.logger.Sugar().Infow("dry-run: skipping flush batch",
			"schema_id", e.schemaID, "batch_kind", batchKind,
			"batch_size", len(batchIDs), "would_create_key", finalKey)
		return nil
	}

	s3TmpPath := fmt.Sprintf("s3://%s/%s", e.cfg.S3Bucket, tmpKey)
	exportSnapshot := e.exportSnapshot
	if exportSnapshot == nil {
		exportSnapshot = func(duck *DuckExporter, ctx context.Context, cfg CDCConfig, pgConnStr string, s3TmpPath string, schemaID int16, snapshotTS int64, rowIDs []uuid.UUID, attrCache forma.SchemaAttributeCache) error {
			return duck.ExportSnapshotToTmp(ctx, cfg, pgConnStr, s3TmpPath, schemaID, snapshotTS, rowIDs, attrCache)
		}
	}

	if err := exportSnapshot(e.duck, ctx, e.cfg, e.pgConnForDuck, s3TmpPath, e.schemaID, e.snapshot, batchIDs, e.attrCache); err != nil {
		return fmt.Errorf("duck export snapshot (%s): %w", batchKind, err)
	}

	if err := CopyTmpToFinal(ctx, e.s3Client, e.cfg.S3Bucket, tmpKey, finalKey, e.logger); err != nil {
		return fmt.Errorf("copy tmp to final (%s): %w", batchKind, err)
	}

	flushedAt := time.Now().UnixMilli()
	updatedIDs, err := MarkFlushedIDsAtSnapshot(ctx, e.db, e.tableName, e.schemaID, batchIDs, e.snapshot, flushedAt)
	if err != nil {
		return fmt.Errorf("mark flushed at snapshot (%s): %w", batchKind, err)
	}

	if len(updatedIDs) == 0 {
		e.logger.Sugar().Infow("flush batch marked zero rows; possible concurrent updates", "schema_id", e.schemaID, "batch_kind", batchKind, "batch_size", len(batchIDs))
		return nil
	}

	if e.manifestStore != nil {
		// Rows are already marked flushed, so a re-run will not re-export
		// them: a delta file missing from the manifest stays invisible to
		// manifest consumers (e.g. compaction) forever. Propagate so the run
		// reports failure; the final key in the error is the operator's
		// pointer to the orphaned file for manual reconciliation (#197).
		if err := updateManifest(ctx, e.manifestStore, e.manifestResolver, e.schemaID, finalKey, "delta", updatedIDs, flushedAt, e.logger); err != nil {
			return fmt.Errorf("manifest update (%s) for %s: %w", batchKind, finalKey, err)
		}
	}

	if len(updatedIDs) < len(batchIDs) {
		e.logger.Sugar().Infow("flush batch marked fewer rows than requested; possible concurrent updates", "schema_id", e.schemaID, "batch_kind", batchKind, "rows_flushed", len(updatedIDs), "batch_size", len(batchIDs))
	}

	e.logger.Sugar().Infow("flush batch completed", "schema_id", e.schemaID, "batch_kind", batchKind, "rows_flushed", len(updatedIDs), "batch_size", len(batchIDs), "final_key", finalKey)
	return nil
}

func buildFlushS3Keys(cfg CDCConfig, schemaID int16) (string, string) {
	tmpUUID := uuid.Must(uuid.NewV7()).String()
	finalUUID := uuid.Must(uuid.NewV7()).String()

	tmpKey := BuildTempPath(cfg.S3Prefix, schemaID, tmpUUID)
	finalKey := BuildDeltaPath(cfg.S3Prefix, schemaID, finalUUID)
	return tmpKey, finalKey
}

func executeFlushInChunks(
	ctx context.Context,
	executor *flushBatchExecutor,
	batchIDs []uuid.UUID,
	maxRows int,
) error {
	for start := 0; start < len(batchIDs); start += maxRows {
		end := min(start+maxRows, len(batchIDs))
		sub := batchIDs[start:end]

		chunkTmpKey, chunkFinalKey := buildFlushS3Keys(executor.cfg, executor.schemaID)
		if err := executor.executeBatch(ctx, sub, chunkTmpKey, chunkFinalKey, "chunk"); err != nil {
			return err
		}
	}
	return nil
}

func executeFlushSingle(
	ctx context.Context,
	executor *flushBatchExecutor,
	batchIDs []uuid.UUID,
) error {
	tmpKey, finalKey := buildFlushS3Keys(executor.cfg, executor.schemaID)
	return executor.executeBatch(ctx, batchIDs, tmpKey, finalKey, "batch")
}

// updateManifest appends a file entry to the schema's manifest.
func updateManifest(
	ctx context.Context,
	store manifest.Store,
	resolver manifest.PathResolver,
	schemaID int16,
	filePath string,
	tier string,
	rowIDs []uuid.UUID,
	createdAt int64,
	logger *zap.Logger,
) error {
	if store == nil {
		return nil
	}

	manifestPath, err := resolver.Resolve(schemaID)
	if err != nil {
		return fmt.Errorf("resolve manifest path: %w", err)
	}

	// Build file entry
	entry := manifest.FileEntry{
		Tier:       tier,
		Path:       filePath,
		RowCount:   int64(len(rowIDs)),
		CreatedMin: createdAt,
		CreatedMax: createdAt,
	}

	// Set row ID bounds if available
	if len(rowIDs) > 0 {
		rowIDMin, rowIDMax := minMaxRowID(rowIDs)
		entry.RowIDMin = rowIDMin.String()
		entry.RowIDMax = rowIDMax.String()
	}

	if err := manifest.AppendFile(ctx, store, manifestPath, schemaID, entry); err != nil {
		return fmt.Errorf("append to manifest: %w", err)
	}

	logger.Sugar().Infow("manifest updated", "schema_id", schemaID, "manifest_path", manifestPath, "file_path", filePath, "tier", tier)
	return nil
}

func minMaxRowID(rowIDs []uuid.UUID) (uuid.UUID, uuid.UUID) {
	rowIDsSize := len(rowIDs)
	if rowIDsSize == 0 {
		return uuid.Nil, uuid.Nil
	}
	if rowIDsSize == 1 {
		return rowIDs[0], rowIDs[0]
	}
	minID := rowIDs[0]
	minIDTime := minID.Time()
	maxID := minID
	maxIDTime := minIDTime
	for _, id := range rowIDs[1:] {
		idTime := id.Time()
		if idTime < minIDTime {
			minID = id
			minIDTime = idTime
		}
		if idTime > maxIDTime {
			maxID = id
			maxIDTime = idTime
		}
	}
	return minID, maxID
}
