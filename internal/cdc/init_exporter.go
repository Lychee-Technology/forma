package cdc

import (
	"context"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// ExportBaseFileToTmp exports existing entity_main + eav_data rows directly to a base parquet file.
// Unlike delta export, this does NOT use change_log - it reads directly from entity_main.
// s3TmpPath is the destination like 's3://bucket/base/<schema_id>/_tmp/<tmp_uuid>.parquet'
func (e *DuckExporter) ExportBaseFileToTmp(ctx context.Context, cfg CDCConfig, pgConnStr string, s3TmpPath string, schemaID int16, rowIDs []uuid.UUID, attrCache forma.SchemaAttributeCache) error {
	sql, mQuery, eQuery, err := buildBaseExportSQL(pgConnStr, s3TmpPath, cfg, schemaID, rowIDs, attrCache)
	if err != nil {
		return err
	}

	return e.executeExportSQL(ctx, cfg, "duckdb base export sql", "duckdb base copy exec", sql, "", mQuery, eQuery)
}

// buildBaseExportSQL constructs the DuckDB SQL for exporting base files.
// Key differences from delta export:
// 1. No change_log table involved
// 2. Uses ltbase_created_at as changed_at (the reader's version timestamp)
// 3. Queries entity_main directly for row metadata
func buildBaseExportSQL(pgConnStr string, s3TmpPath string, cfg CDCConfig, schemaID int16, rowIDs []uuid.UUID, attrCache forma.SchemaAttributeCache) (string, string, string, error) {
	plan, err := buildExportSQLPlan(exportModeSpec{
		defaultMemoryLimit: "8GB",
		defaultMainColumns: defaultBaseMainColumns,
		activeOnly:         true,
		useChangeLog:       false,
		schemaIDSelect:     "m.ltbase_schema_id AS schema_id",
		rowIDSelect:        "m.ltbase_row_id AS row_id",
		timeSlotSelect:     "m.ltbase_created_at AS changed_at",
		deletedAtSelect:    "COALESCE(m.ltbase_deleted_at, 0) AS deleted_at",
	}, pgConnStr, s3TmpPath, cfg, schemaID, 0, rowIDs, attrCache)
	if err != nil {
		return "", "", "", err
	}

	return plan.sql, plan.mainQuery, plan.eavQuery, nil
}
