package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/lychee-technology/forma/internal/parquetcheck"
	"go.uber.org/zap"
)

type flushBatchExecutor struct {
	db        *sql.DB
	duck      *DuckExporter
	s3Client  S3ObjectClient
	cfg       CDCConfig
	tableName string
	schemaID  int16
	snapshot  int64
	// versions holds the changed_at listed per batch row at selection time;
	// marking is exact against it (MarkFlushedVersions), so a clock-ahead
	// version flushes normally and a concurrently advanced row stays dirty.
	versions         map[uuid.UUID]int64
	pgConnForDuck    string
	attrCache        forma.SchemaAttributeCache
	dryRun           bool
	logger           *zap.Logger
	manifestStore    manifest.Store
	manifestResolver manifest.PathResolver
	executeSingle    func(*flushBatchExecutor, []uuid.UUID) error
	executeInChunks  func(*flushBatchExecutor, []uuid.UUID, int) error
	exportSnapshot   func(*DuckExporter, context.Context, CDCConfig, string, string, int16, int64, []uuid.UUID, forma.SchemaAttributeCache) error
	markFlushed      func(context.Context, *sql.DB, string, int16, []uuid.UUID, map[uuid.UUID]int64, int64) ([]uuid.UUID, error)
	// describeColumns is a test seam for the write-time footer probe that
	// stamps manifest entries (#256); nil uses the exporter's DuckDB session.
	describeColumns func(ctx context.Context, uri string) (map[string]string, error)
	// checksumObject is a test seam for the post-publish content hash that
	// stamps manifest entries (#347); nil skips stamping. The real
	// implementation is ObjectSHA256 over the full S3 client.
	checksumObject func(ctx context.Context, key string) (string, error)
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

	if e.cfg.BeforeExportHook != nil {
		if err := e.cfg.BeforeExportHook(ctx, e.schemaID, batchIDs, e.snapshot); err != nil {
			return fmt.Errorf("before-export hook (%s): %w", batchKind, err)
		}
	}

	if err := e.exportBatchToTmp(ctx, batchIDs, tmpKey, batchKind); err != nil {
		return fmt.Errorf("export flush batch to tmp: %w", err)
	}

	if err := CopyTmpToFinal(ctx, e.s3Client, e.cfg.S3Bucket, tmpKey, finalKey, e.logger); err != nil {
		return fmt.Errorf("copy tmp to final (%s): %w", batchKind, err)
	}

	sizeBytes, err := HeadObjectSize(ctx, e.s3Client, e.cfg.S3Bucket, finalKey)
	if err != nil {
		// Best-effort: SizeBytes only feeds compaction's promotion heuristic.
		e.logger.Sugar().Warnw("failed to stat final delta object; manifest SizeBytes stays 0",
			"schema_id", e.schemaID, "final_key", finalKey, "err", err)
	}

	stampCols := e.stampColumns(ctx, finalKey)
	checksum := e.stampChecksum(ctx, finalKey)

	// Order (#252): manifest-append precedes mark-flushed, so no query ever
	// lands in a state where the batch's rows are in neither tier. The middle
	// state — delta listed but rows still flushed_at = 0 — is safe by
	// construction: the dirty anti-join (advanced_query_template_duckdb.go)
	// discards the S3 copies of unflushed rows and the hot pg_source serves
	// them. Failure contract (supersedes #197):
	//   - Append fails -> rows stay dirty (mark never ran); the retry
	//     re-exports to a fresh UUIDv7 key and self-heals. The old copied
	//     final is an unlisted orphan reclaimed by manifest-reconcile --gc
	//     (ClassDelta). The final key in the error is observability, not a
	//     --repair pointer.
	//   - Mark fails after append -> the delta is LISTED and rows stay dirty;
	//     the retry yields a second listed delta with identical
	//     (row_id, ver_ts) rows, which LWW (rn=1 + #183 tie-break) dedups and
	//     compaction later collapses.
	// The entry is built from batchIDs (mark has not run, so the marked subset
	// is unknown): RowCount/RowIDMin/Max may overcount rows that concurrently
	// changed after the snapshot. That feeds only compaction's promotion
	// heuristic; reconcile recomputes stats from parquet contents. A batch
	// whose rows ALL changed concurrently leaves an LWW-inert junk delta
	// listed — accepted, never rolled back.
	entryCreatedAt := time.Now().UnixMilli()
	if e.manifestStore != nil {
		if err := updateManifest(ctx, e.manifestStore, e.manifestResolver, e.schemaID, finalKey, "delta", batchIDs, entryCreatedAt, sizeBytes, stampCols, checksum, e.logger); err != nil {
			return fmt.Errorf("manifest update (%s) for %s: %w", batchKind, finalKey, err)
		}
	}

	// The mark timestamp is sampled AFTER the append completes, separately
	// from the manifest entry's CreatedMin/Max stamp: a reader that anchored
	// its dirty-barrier cutoff and resolved its path set while the append was
	// still in flight (its set lacks this delta) must observe
	// flushed_at >= cutoff so the widened barrier keeps the rows hot-readable
	// (#252 review P1). CreatedMin/Max only feed compaction ordering, so the
	// two stamps diverging by the append latency is harmless.
	flushedAt := time.Now().UnixMilli()
	markFlushed := e.markFlushed
	if markFlushed == nil {
		markFlushed = MarkFlushedVersions
	}
	updatedIDs, err := markFlushed(ctx, e.db, e.tableName, e.schemaID, batchIDs, e.versions, flushedAt)
	if err != nil {
		return fmt.Errorf("mark flushed at listed versions (%s): %w", batchKind, err)
	}

	if len(updatedIDs) == 0 {
		e.logger.Sugar().Infow("flush batch marked zero rows; possible concurrent updates", "schema_id", e.schemaID, "batch_kind", batchKind, "batch_size", len(batchIDs))
		return nil
	}

	if len(updatedIDs) < len(batchIDs) {
		e.logger.Sugar().Infow("flush batch marked fewer rows than requested; possible concurrent updates", "schema_id", e.schemaID, "batch_kind", batchKind, "rows_flushed", len(updatedIDs), "batch_size", len(batchIDs))
	}

	e.logger.Sugar().Infow("flush batch completed", "schema_id", e.schemaID, "batch_kind", batchKind, "rows_flushed", len(updatedIDs), "batch_size", len(batchIDs), "final_key", finalKey)
	return nil
}

// exportBatchToTmp runs the DuckDB snapshot export into the batch's tmp
// object. On failure it deletes that object in band (#226): DuckDB may have
// written it before failing, and the retry mints a fresh UUID, so the orphan
// would otherwise wait for manifest-reconcile --gc. An S3 delete of a missing
// key is a no-op, so this is safe even when nothing was written.
func (e *flushBatchExecutor) exportBatchToTmp(ctx context.Context, batchIDs []uuid.UUID, tmpKey, batchKind string) error {
	s3TmpPath := fmt.Sprintf("s3://%s/%s", e.cfg.S3Bucket, tmpKey)
	exportSnapshot := e.exportSnapshot
	if exportSnapshot == nil {
		exportSnapshot = func(duck *DuckExporter, ctx context.Context, cfg CDCConfig, pgConnStr string, s3TmpPath string, schemaID int16, snapshotTS int64, rowIDs []uuid.UUID, attrCache forma.SchemaAttributeCache) error {
			return duck.ExportSnapshotToTmp(ctx, cfg, pgConnStr, s3TmpPath, schemaID, snapshotTS, rowIDs, attrCache)
		}
	}

	if err := exportSnapshot(e.duck, ctx, e.cfg, e.pgConnForDuck, s3TmpPath, e.schemaID, e.snapshot, batchIDs, e.attrCache); err != nil {
		if delErr := DeleteObjectKey(ctx, e.s3Client, e.cfg.S3Bucket, tmpKey); delErr != nil {
			e.logger.Sugar().Warnw("failed to delete tmp object after export failure",
				"schema_id", e.schemaID, "tmp_key", tmpKey, "err", delErr)
		}
		return fmt.Errorf("duck export snapshot (%s): %w", batchKind, err)
	}
	return nil
}

// stampColumns best-effort probes the just-published final object's footer
// for the manifest column stamp (#256). tmp→final is a byte-identical
// CopyObject, so the final's footer is the export's footer. Failure leaves
// the entry unstamped — readers fall back to probing — so stamping never
// fails a flush.
func (e *flushBatchExecutor) stampColumns(ctx context.Context, finalKey string) map[string]string {
	if e.manifestStore == nil {
		// No manifest to stamp: the entry is never persisted (executeBatch
		// skips updateManifest), so a footer probe would be a discarded S3
		// read per batch.
		return nil
	}

	describe := e.describeColumns
	if describe == nil {
		if e.duck == nil || e.duck.DB == nil {
			return nil
		}
		describe = func(ctx context.Context, uri string) (map[string]string, error) {
			return parquetcheck.DescribeColumns(ctx, e.duck.DB, uri)
		}
	}
	uri := fmt.Sprintf("s3://%s/%s", e.cfg.S3Bucket, finalKey)
	cols, err := describe(ctx, uri)
	if err != nil {
		e.logger.Sugar().Warnw("failed to describe final delta object; manifest entry stays unstamped",
			"schema_id", e.schemaID, "final_key", finalKey, "err", err)
		return nil
	}
	return cols
}

// stampChecksum best-effort hashes the just-published final object for the
// manifest content checksum (#347). tmp→final is a byte-identical CopyObject,
// so the final's bytes are the export's bytes. Failure leaves the entry
// unstamped — verification passes skip it — so stamping never fails a flush.
func (e *flushBatchExecutor) stampChecksum(ctx context.Context, finalKey string) string {
	if e.manifestStore == nil || e.checksumObject == nil {
		return ""
	}
	sum, err := e.checksumObject(ctx, finalKey)
	if err != nil {
		e.logger.Sugar().Warnw("failed to checksum final delta object; manifest entry stays unstamped",
			"schema_id", e.schemaID, "final_key", finalKey, "err", err)
		return ""
	}
	return sum
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
	sizeBytes int64,
	columns map[string]string,
	checksum string,
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
		SizeBytes:  sizeBytes,
		Columns:    columns,
		Checksum:   checksum,
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
