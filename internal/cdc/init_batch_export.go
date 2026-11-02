package cdc

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"go.uber.org/zap"
)

type schemaBatchExport struct {
	rowIDs    []uuid.UUID
	minRowID  string
	maxRowID  string
	tmpKey    string
	finalKey  string
	s3TmpPath string
}

func buildSchemaBatchExport(runCtx *initRunContext, state *schemaInitState, rowIDs []uuid.UUID) schemaBatchExport {
	minRowID := rowIDs[0].String()
	maxRowID := rowIDs[len(rowIDs)-1].String()

	tmpUUID := uuid.Must(uuid.NewV7()).String()
	tmpKey := BuildBaseTempPath(runCtx.cfg.S3Prefix, state.schemaID, tmpUUID)
	// tmp and final share the batch's UUID: the final key is write-once
	// (#416), and a shared id lets an operator pair a stranded _tmp object
	// with the final it was meant to become.
	finalKey := BuildBasePath(runCtx.cfg.S3Prefix, state.schemaID, minRowID, maxRowID, tmpUUID)
	s3TmpPath := fmt.Sprintf("s3://%s/%s", runCtx.cfg.S3Bucket, tmpKey)

	return schemaBatchExport{
		rowIDs:    rowIDs,
		minRowID:  minRowID,
		maxRowID:  maxRowID,
		tmpKey:    tmpKey,
		finalKey:  finalKey,
		s3TmpPath: s3TmpPath,
	}
}

func recordSchemaBatchResult(state *schemaInitState, batch schemaBatchExport, createdAt int64, sizeBytes int64, columns map[string]string, checksum string) {
	state.fileEntries = append(state.fileEntries, manifest.FileEntry{
		Tier:       "base",
		Path:       batch.finalKey,
		RowIDMin:   batch.minRowID,
		RowIDMax:   batch.maxRowID,
		RowCount:   int64(len(batch.rowIDs)),
		CreatedMin: createdAt,
		CreatedMax: createdAt,
		SizeBytes:  sizeBytes,
		Columns:    columns,
		Checksum:   checksum,
	})
	state.rowsExported += int64(len(batch.rowIDs))
	state.filesCreated++
}

// exportBaseFileForBatch writes the batch's rows to the tmp object, through the
// run's export seam when one is installed (tests) and the DuckDB exporter
// otherwise.
func exportBaseFileForBatch(ctx context.Context, runCtx *initRunContext, state *schemaInitState, batch schemaBatchExport) error {
	if runCtx.exportBaseFile != nil {
		return runCtx.exportBaseFile(ctx, state, batch)
	}
	return runCtx.duck.ExportBaseFileToTmp(ctx, runCtx.cfg, runCtx.db, batch.s3TmpPath, state.schemaID, batch.rowIDs, state.attrCache)
}

func exportSchemaBatch(ctx context.Context, runCtx *initRunContext, state *schemaInitState, rowIDs []uuid.UUID) error {
	return exportBatch(ctx, runCtx, state, buildSchemaBatchExport(runCtx, state, rowIDs))
}

// exportBatch runs one prepared batch end to end: the tmp export, the
// tmp->final copy, the size stat, and the manifest entry record. Dry-run
// counts the batch and writes nothing. The zero-row base of an emptied
// schema (finishEmptySchema, #519) goes through the same steps.
func exportBatch(ctx context.Context, runCtx *initRunContext, state *schemaInitState, batch schemaBatchExport) error {
	runCtx.logger.Info("exporting batch",
		zap.Int16("schema_id", state.schemaID),
		zap.Int("batch_size", len(batch.rowIDs)),
		zap.String("min_row_id", batch.minRowID),
		zap.String("max_row_id", batch.maxRowID),
		zap.String("tmp_path", batch.s3TmpPath),
		zap.String("final_key", batch.finalKey))

	if runCtx.dryRun {
		runCtx.logger.Info("dry-run: skipping export", zap.Int16("schema_id", state.schemaID), zap.Int("batch_size", len(batch.rowIDs)))
		state.rowsExported += int64(len(batch.rowIDs))
		state.filesCreated++
		return nil
	}

	if err := exportBaseFileForBatch(ctx, runCtx, state, batch); err != nil {
		return fmt.Errorf("export batch: %w", err)
	}
	if err := schemaledger.RequireUnchanged(ctx, runCtx.schemaRegistry, state.schemaID); err != nil {
		return fmt.Errorf("publish batch: %w", err) // the export used this run's declared types (#733)
	}
	if err := CopyTmpToFinal(ctx, runCtx.s3Client, runCtx.cfg.S3Bucket, batch.tmpKey, batch.finalKey, runCtx.logger); err != nil {
		return fmt.Errorf("copy tmp->final: %w", err)
	}

	sizeBytes, err := HeadObjectSize(ctx, runCtx.s3Client, runCtx.cfg.S3Bucket, batch.finalKey)
	if err != nil {
		// Best-effort: SizeBytes only feeds compaction's promotion heuristic.
		runCtx.logger.Warn("failed to stat final base object; manifest SizeBytes stays 0",
			zap.Int16("schema_id", state.schemaID),
			zap.String("final_key", batch.finalKey),
			zap.Error(err))
	}

	recordSchemaBatchResult(state, batch, time.Now().UnixMilli(), sizeBytes,
		initStampColumns(ctx, runCtx, state.schemaID, batch),
		initStampChecksum(ctx, runCtx, state.schemaID, batch))

	runCtx.logger.Info("batch completed",
		zap.Int16("schema_id", state.schemaID),
		zap.Int64("rows_exported", state.rowsExported),
		zap.Int("files_created", state.filesCreated),
		zap.String("final_key", batch.finalKey))
	return nil
}
