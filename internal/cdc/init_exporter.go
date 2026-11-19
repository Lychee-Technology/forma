package cdc

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// ExportBase exports the live entity_main + eav_data rows rowIDs to the
// parquet file dest. Unlike a delta export it does not read change_log. It
// reads PostgreSQL on snap, the REPEATABLE READ snapshot whose first statement
// listed the rows and their unflushed versions (cdcv2.BaseExporter), so each
// row's exported content and its listed version come from one snapshot. It
// neither commits nor rolls snap back.
func (e *DuckExporter) ExportBase(ctx context.Context, cfg CDCConfig, snap *sql.Tx, dest string, schemaID int16, rowIDs []uuid.UUID, attrCache forma.SchemaAttributeCache) error {
	plan, err := buildBaseExportPlan(stagedTransport, dest, cfg, schemaID, rowIDs, attrCache)
	if err != nil {
		return fmt.Errorf("export base of schema %d: %w", schemaID, err)
	}
	stage := func(ctx context.Context, duckConn *sql.Conn) (int64, error) {
		return stageRowsOnTx(ctx, snap, duckConn, plan.stages, rowIDs)
	}
	return e.runExport(ctx, cfg, plan, rowIDs, stage, "duckdb base export sql", "duckdb base copy exec")
}

// buildBaseExportPlan constructs the export plan for base files.
// Key differences from delta export:
//  1. No change_log table involved
//  2. Uses ltbase_updated_at as changed_at (the reader's version timestamp):
//     the base row IS the state as of its last update, and every write path
//     stamps ltbase_updated_at and change_log.changed_at from the same clock
//     read, so the base copy ranks exactly at its newest flushed delta — never
//     below a previously flushed older version (#210)
//  3. Queries entity_main directly for row metadata
func buildBaseExportPlan(transport exportTransport, dest string, cfg CDCConfig, schemaID int16, rowIDs []uuid.UUID, attrCache forma.SchemaAttributeCache) (exportSQLPlan, error) {
	plan, err := buildExportSQLPlan(exportModeSpec{
		defaultMemoryLimit: "8GB",
		activeOnly:         true,
		useChangeLog:       false,
		schemaIDSelect:     "m.ltbase_schema_id AS schema_id",
		rowIDSelect:        "m.ltbase_row_id AS row_id",
		timeSlotSelect:     "m.ltbase_updated_at AS changed_at",
		deletedAtSelect:    "COALESCE(m.ltbase_deleted_at, 0) AS deleted_at",
	}, transport, dest, cfg, schemaID, 0, rowIDs, attrCache)
	if err != nil {
		return exportSQLPlan{}, fmt.Errorf("build base export SQL plan: %w", err)
	}

	return plan, nil
}
