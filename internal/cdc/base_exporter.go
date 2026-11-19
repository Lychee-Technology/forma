package cdc

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/cdcv2"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"go.uber.org/zap"
)

// baseExporter is cdc-init's cdcv2.BaseExporter. It exports one export
// snapshot's live rows with the DuckDB exporter, reading their content on
// the snapshot the writer hands it, and stages the object at the base key
// the writer minted, once.
type baseExporter struct {
	duck      *DuckExporter
	store     manifestv2.Store
	cfg       CDCConfig
	registry  forma.SchemaRegistry
	attrCache forma.SchemaAttributeCache
	logger    *zap.Logger
}

// ExportBase writes req.RowIDs to req.Key. snap listed them as live rows as
// its first statement, and the export reads them as live rows in the same
// snapshot, so the object holds every one of them: one it does not is
// refused before it is staged.
func (x *baseExporter) ExportBase(ctx context.Context, snap *sql.Tx, req cdcv2.BaseRequest) (manifestv2.File, error) {
	x.logger.Info("exporting base object",
		zap.Int16("schema_id", req.SchemaID), zap.Int64("epoch", req.Epoch),
		zap.Int("rows", len(req.RowIDs)), zap.String("key", req.Key))
	// Selection->export test seam, mirroring the flusher's (#182). Init has
	// no snapshot clock, so the hook receives 0.
	if hook := x.cfg.BeforeExportHook; hook != nil {
		if err := hook(ctx, req.SchemaID, req.RowIDs, 0); err != nil {
			return manifestv2.File{}, fmt.Errorf("before-export hook: %w", err)
		}
	}
	scratch, err := newScratchFile()
	if err != nil {
		return manifestv2.File{}, err
	}
	defer scratch.remove()
	if err := x.duck.ExportBase(ctx, x.cfg, snap, scratch.path, req.SchemaID, req.RowIDs, x.attrCache); err != nil {
		return manifestv2.File{}, fmt.Errorf("export base object %s: %w", req.Key, err)
	}
	// The export used this run's declared types (#733).
	if err := schemaledger.RequireUnchanged(ctx, x.registry, req.SchemaID); err != nil {
		return manifestv2.File{}, fmt.Errorf("stage base object %s: %w", req.Key, err)
	}
	obj, err := x.duck.describeExport(ctx, scratch.path)
	if err != nil {
		return manifestv2.File{}, err
	}
	if obj.rowCount != int64(len(req.RowIDs)) {
		return manifestv2.File{}, fmt.Errorf("base object %s holds %d rows, but its export snapshot listed %d live rows", req.Key, obj.rowCount, len(req.RowIDs))
	}
	file, err := stageObject(ctx, x.store, req.Key, manifestv2.TierBase, obj)
	if err != nil {
		return manifestv2.File{}, err
	}
	x.logger.Info("base object staged",
		zap.Int16("schema_id", req.SchemaID), zap.String("key", req.Key),
		zap.Int64("rows", obj.rowCount), zap.Int64("size_bytes", obj.sizeBytes))
	return file, nil
}
