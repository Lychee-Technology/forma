package cdc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/cdcv2"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"go.uber.org/zap"
)

// errNothingExported reports a flush generation whose export wrote no row:
// every listed entry was rewritten past the export's snapshot cutoff between
// the selection and the export. A generation lists at least one file, so
// nothing is published, and the entries, which no longer carry their listed
// versions, stay dirty for the next flush.
var errNothingExported = errors.New("the flush export wrote no row")

// deltaStager is cdc-flush's cdcv2.DeltaStager. It exports a generation's
// entries with the DuckDB exporter, each object from the change_log versions
// and the entity_main and eav_data values of one PostgreSQL REPEATABLE READ
// snapshot, and stages every object under a fresh write-once key of the
// request's layout and epoch. An object that holds no row is not staged.
type deltaStager struct {
	duck      *DuckExporter
	db        *sql.DB
	store     manifestv2.Store
	cfg       CDCConfig
	registry  forma.SchemaRegistry
	attrCache forma.SchemaAttributeCache
	logger    *zap.Logger
}

// StageDelta exports req.Entries in objects of at most the configured byte
// target's rows (MaxBatchBytes / EstimatedRowBytes) and answers their
// manifest entries.
//
// The export's cutoff is the later of the clock and the greatest listed
// version: versions are per-row and monotonic and may run ahead of the clock
// (#274), so a clock-only cutoff would leave a clock-ahead row out of every
// export. An entry rewritten after the selection exports at its new version
// when that is at or below the cutoff, and not at all otherwise; either way
// its mark, exact against the listed version, skips it, so a generation never
// marks an entry its files do not hold at that version.
func (s *deltaStager) StageDelta(ctx context.Context, req cdcv2.DeltaRequest) ([]manifestv2.File, error) {
	rowIDs, snapshot := deltaBatch(req.Entries)
	s.logger.Sugar().Infow("export snapshot", "schema_id", req.SchemaID, "epoch", req.Epoch, "snapshot_ts", snapshot, "rows", len(rowIDs))
	if hook := s.cfg.BeforeExportHook; hook != nil {
		if err := hook(ctx, req.SchemaID, rowIDs, snapshot); err != nil {
			return nil, fmt.Errorf("before-export hook: %w", err)
		}
	}
	chunkRows := s.chunkRows(len(rowIDs))
	if chunkRows < len(rowIDs) {
		s.logger.Sugar().Infow("splitting batch to meet byte target", "schema_id", req.SchemaID, "from_rows", len(rowIDs), "chunk_rows", chunkRows)
	}
	var files []manifestv2.File
	start := 0
	for chunk := range slices.Chunk(rowIDs, chunkRows) {
		file, ok, err := s.stageChunk(ctx, req, chunk, snapshot)
		if err != nil {
			return nil, fmt.Errorf("stage delta rows %d-%d of %d: %w", start+1, start+len(chunk), len(rowIDs), err)
		}
		if ok {
			files = append(files, file)
		}
		start += len(chunk)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("schema %d, %d entries: %w", req.SchemaID, len(rowIDs), errNothingExported)
	}
	return files, nil
}

// deltaBatch answers the entries' row ids and the export's cutoff.
func deltaBatch(entries []cdcv2.Entry) ([]uuid.UUID, int64) {
	rowIDs := make([]uuid.UUID, len(entries))
	snapshot := time.Now().UnixMilli()
	for i, e := range entries {
		rowIDs[i] = e.RowID
		snapshot = max(snapshot, e.Version)
	}
	return rowIDs, snapshot
}

// chunkRows is the most rows one delta object holds: the byte target over
// the estimated row size, or the whole batch without a target.
func (s *deltaStager) chunkRows(total int) int {
	if s.cfg.MaxBatchBytes > 0 && s.cfg.EstimatedRowBytes > 0 {
		if rows := int(s.cfg.MaxBatchBytes / int64(s.cfg.EstimatedRowBytes)); rows > 0 && rows < total {
			return rows
		}
	}
	return max(total, 1)
}

// stageChunk exports rowIDs to a scratch file and, when it holds a row,
// stages it under a fresh delta key. ok is false for an export of no row.
func (s *deltaStager) stageChunk(ctx context.Context, req cdcv2.DeltaRequest, rowIDs []uuid.UUID, snapshot int64) (manifestv2.File, bool, error) {
	key, err := req.Layout.NewParquetKey(req.SchemaID, req.Epoch, manifestv2.TierDelta)
	if err != nil {
		return manifestv2.File{}, false, fmt.Errorf("mint a delta key: %w", err)
	}
	scratch, err := newScratchFile()
	if err != nil {
		return manifestv2.File{}, false, err
	}
	defer scratch.remove()
	if err := s.duck.ExportDelta(ctx, s.cfg, s.db, scratch.path, req.SchemaID, snapshot, rowIDs, s.attrCache); err != nil {
		return manifestv2.File{}, false, fmt.Errorf("duck export snapshot: %w", err)
	}
	// The export wrote through this run's declared types; an object exported
	// across a newly recorded generation is never staged (#733).
	if err := schemaledger.RequireUnchanged(ctx, s.registry, req.SchemaID); err != nil {
		return manifestv2.File{}, false, fmt.Errorf("stage delta object %s: %w", key, err)
	}
	obj, err := s.duck.describeExport(ctx, scratch.path)
	if err != nil {
		return manifestv2.File{}, false, err
	}
	if obj.rowCount == 0 {
		s.logger.Sugar().Infow("delta export wrote no row: its entries were rewritten since the selection and stay dirty",
			"schema_id", req.SchemaID, "rows", len(rowIDs))
		return manifestv2.File{}, false, nil
	}
	file, err := stageObject(ctx, s.store, key, manifestv2.TierDelta, obj)
	if err != nil {
		return manifestv2.File{}, false, err
	}
	s.logger.Sugar().Infow("delta object staged", "schema_id", req.SchemaID, "epoch", req.Epoch, "key", key,
		"rows", obj.rowCount, "size_bytes", obj.sizeBytes)
	return file, true, nil
}
