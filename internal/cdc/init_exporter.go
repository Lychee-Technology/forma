package cdc

import (
	"context"
	"fmt"
	"strings"
	"time"

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

	e.Logger.Sugar().Infow("duckdb base export sql", "sql_preview", redactConnStr(sql), "m_query", mQuery, "e_query", eQuery)
	timeout := cfg.QueryTimeout
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}
	ctx2, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if _, err := e.DB.ExecContext(ctx2, sql); err != nil {
		return fmt.Errorf("duckdb base copy exec: %w", err)
	}
	return nil
}

// buildBaseExportSQL constructs the DuckDB SQL for exporting base files.
// Key differences from delta export:
// 1. No change_log table involved
// 2. Uses ltbase_created_at as time_slot
// 3. Queries entity_main directly for row metadata
func buildBaseExportSQL(pgConnStr string, s3TmpPath string, cfg CDCConfig, schemaID int16, rowIDs []uuid.UUID, attrCache forma.SchemaAttributeCache) (string, string, string, error) {
	if len(rowIDs) == 0 {
		return "", "", "", fmt.Errorf("export base: no row ids provided")
	}

	pgEsc := escapeLiteral(pgConnStr)
	s3Esc := escapeLiteral(s3TmpPath)

	entityMain, eavData := resolveMainAndEAVTableNames(cfg)

	rowList := quoteUUIDList(rowIDs)
	mFilter := fmt.Sprintf("ltbase_row_id IN (%s)", rowList)
	eFilter := fmt.Sprintf("row_id IN (%s)", rowList)

	opts := resolveExportSQLOptions(cfg, "8GB") // Larger default for base file export

	// Fallback to generic projection when no schema metadata is provided.
	if len(attrCache) == 0 {
		mColumns := defaultBaseMainColumns()
		mQuery := buildMainEntityQuery(entityMain, schemaID, mColumns, mFilter, true)
		eQuery := buildEAVQuery(eavData, schemaID, eFilter, nil)

		mQueryEsc := escapeLiteral(mQuery)
		eQueryEsc := escapeLiteral(eQuery)

		// For base files, we use ltbase_created_at as time_slot
		mainSelectCols := []string{
			"m.ltbase_schema_id AS schema_id",
			"m.ltbase_row_id AS row_id",
			"m.ltbase_created_at AS time_slot",
			"m.ltbase_deleted_at AS deleted_at",
			"m.ltbase_created_at",
			"m.ltbase_updated_at",
			"m.ltbase_deleted_at",
		}
		mainSelectCols = append(mainSelectCols, prefixColumns("m.", mColumns[5:])...)

		sql := fmt.Sprintf(`PRAGMA memory_limit='%s';
ATTACH IF NOT EXISTS '%s' AS pg_db (TYPE postgres, READ_ONLY);

COPY (
SELECT
  %s,
  e.attributes
FROM postgres_query('pg_db', '%s') m
LEFT JOIN (
  SELECT row_id, list(struct_pack(attr_id := attr_id, value_text := value_text)) AS attributes
  FROM postgres_query('pg_db', '%s')
  GROUP BY row_id
) e ON m.ltbase_row_id = e.row_id
) TO '%s' (FORMAT PARQUET, COMPRESSION '%s', COMPRESSION_LEVEL %d);
`, opts.memoryLimit, pgEsc, strings.Join(mainSelectCols, ",\n  "), mQueryEsc, eQueryEsc, s3Esc,
			strings.ToUpper(opts.compression), opts.compressionLevel)

		return sql, mQuery, eQuery, nil
	}

	// Schema-driven projection path
	projection := buildSchemaDrivenProjection(attrCache)
	mQuery := buildMainEntityQuery(entityMain, schemaID, projection.mainColumns, mFilter, true)
	eQuery := buildEAVQuery(eavData, schemaID, eFilter, projection.eavAttrIDs)

	mQueryEsc := escapeLiteral(mQuery)
	eQueryEsc := escapeLiteral(eQuery)

	// For base files, we use ltbase_created_at as time_slot
	mainSelectCols := []string{
		"m.ltbase_schema_id AS schema_id",
		"m.ltbase_row_id AS row_id",
		"m.ltbase_created_at AS time_slot",
		"m.ltbase_deleted_at AS deleted_at",
		"m.ltbase_created_at",
		"m.ltbase_updated_at",
		"m.ltbase_deleted_at",
	}
	mainSelectCols = append(mainSelectCols, projection.mainProjections...)
	mainSelectCols = append(mainSelectCols, projection.eavSelect...)

	eAggSQL := buildEAVAggregationSQL(eQueryEsc, projection.eavAgg)

	sql := fmt.Sprintf(`PRAGMA memory_limit='%s';
ATTACH IF NOT EXISTS '%s' AS pg_db (TYPE postgres, READ_ONLY);

COPY (
SELECT
  %s
FROM postgres_query('pg_db', '%s') m
LEFT JOIN (
  %s
) e ON m.ltbase_row_id = e.row_id
) TO '%s' (FORMAT PARQUET, COMPRESSION '%s', COMPRESSION_LEVEL %d);
`, opts.memoryLimit, pgEsc, strings.Join(mainSelectCols, ",\n  "), mQueryEsc, eAggSQL, s3Esc,
		strings.ToUpper(opts.compression), opts.compressionLevel)

	return sql, mQuery, eQuery, nil
}
