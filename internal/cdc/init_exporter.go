package cdc

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// ExportBaseFileToTmp exports existing entity_main + eav_data rows, read from
// pg (a pgx database/sql pool), directly to a base parquet file. Unlike delta
// export, this does NOT use change_log - it reads directly from entity_main.
// s3TmpPath is the destination like 's3://bucket/base/<schema_id>/_tmp/<tmp_uuid>.parquet'
func (e *DuckExporter) ExportBaseFileToTmp(ctx context.Context, cfg CDCConfig, pg *sql.DB, s3TmpPath string, schemaID int16, rowIDs []uuid.UUID, attrCache forma.SchemaAttributeCache) error {
	plan, err := buildBaseExportPlan(stagedTransport, s3TmpPath, cfg, schemaID, rowIDs, attrCache)
	if err != nil {
		return fmt.Errorf("export base file to tmp for schema %d: %w", schemaID, err)
	}

	return e.runExport(ctx, cfg, pg, plan, rowIDs, "duckdb base export sql", "duckdb base copy exec")
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
func buildBaseExportPlan(transport exportTransport, s3TmpPath string, cfg CDCConfig, schemaID int16, rowIDs []uuid.UUID, attrCache forma.SchemaAttributeCache) (exportSQLPlan, error) {
	plan, err := buildExportSQLPlan(exportModeSpec{
		defaultMemoryLimit: "8GB",
		activeOnly:         true,
		useChangeLog:       false,
		schemaIDSelect:     "m.ltbase_schema_id AS schema_id",
		rowIDSelect:        "m.ltbase_row_id AS row_id",
		timeSlotSelect:     "m.ltbase_updated_at AS changed_at",
		deletedAtSelect:    "COALESCE(m.ltbase_deleted_at, 0) AS deleted_at",
	}, transport, s3TmpPath, cfg, schemaID, 0, rowIDs, attrCache)
	if err != nil {
		return exportSQLPlan{}, fmt.Errorf("build base export SQL plan: %w", err)
	}

	return plan, nil
}
