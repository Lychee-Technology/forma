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
func (e *DuckExporter) ExportBaseFileToTmp(ctx context.Context, pgConnStr string, s3TmpPath string, schemaID int16, rowIDs []uuid.UUID, attrCache forma.SchemaAttributeCache) error {
	sql, mQuery, eQuery, err := buildBaseExportSQL(pgConnStr, s3TmpPath, e.Config, schemaID, rowIDs, attrCache)
	if err != nil {
		return err
	}

	e.Logger.Sugar().Infow("duckdb base export sql", "sql_preview", sql, "m_query", mQuery, "e_query", eQuery)
	timeout := e.Config.QueryTimeout
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

	entityMain := sanitizeIdentifier(cfg.EntityMainTable)
	if entityMain == "" {
		entityMain = "entity_main"
	}
	eavData := sanitizeIdentifier(cfg.EAVDataTable)
	if eavData == "" {
		eavData = "eav_data"
	}

	rowList := quoteUUIDList(rowIDs)
	mFilter := fmt.Sprintf("ltbase_row_id IN (%s)", rowList)
	eFilter := fmt.Sprintf("row_id IN (%s)", rowList)

	compression := cfg.ParquetCompression
	if compression == "" {
		compression = DefaultParquetCompression
	}
	compressionLevel := cfg.ParquetCompressionLevel
	if compressionLevel <= 0 {
		compressionLevel = DefaultParquetCompressionLevel
	}
	memoryLimit := cfg.DuckMemLimit
	if memoryLimit == "" {
		memoryLimit = "8GB" // Larger default for base file export
	}

	// Fallback to generic projection when no schema metadata is provided.
	if len(attrCache) == 0 {
		mColumns := []string{
			"ltbase_row_id",
			"ltbase_schema_id",
			"ltbase_created_at",
			"ltbase_updated_at",
			"ltbase_deleted_at",
			"text_01", "text_02", "text_03", "text_04", "text_05",
			"text_06", "text_07", "text_08", "text_09", "text_10",
			"smallint_01", "smallint_02", "smallint_03",
			"integer_01", "integer_02", "integer_03",
			"bigint_01", "bigint_02", "bigint_03", "bigint_04", "bigint_05",
			"double_01", "double_02", "double_03", "double_04", "double_05",
			"uuid_01", "uuid_02",
		}
		mQuery := fmt.Sprintf(
			"SELECT %s FROM %s WHERE ltbase_schema_id = %d AND ltbase_deleted_at IS NULL AND %s",
			strings.Join(mColumns, ", "), entityMain, schemaID, mFilter,
		)
		eQuery := fmt.Sprintf(
			"SELECT schema_id, row_id, attr_id, value_text FROM %s WHERE schema_id = %d AND %s",
			eavData, schemaID, eFilter,
		)

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
`, memoryLimit, pgEsc, strings.Join(mainSelectCols, ",\n  "), mQueryEsc, eQueryEsc, s3Esc,
			strings.ToUpper(compression), compressionLevel)

		return sql, mQuery, eQuery, nil
	}

	// Schema-driven projection path
	mainColumns := []string{
		"ltbase_row_id",
		"ltbase_schema_id",
		"ltbase_created_at",
		"ltbase_updated_at",
		"ltbase_deleted_at",
	}
	mainColSet := map[string]struct{}{
		"ltbase_row_id":     {},
		"ltbase_schema_id":  {},
		"ltbase_created_at": {},
		"ltbase_updated_at": {},
		"ltbase_deleted_at": {},
	}

	mainProjections := []string{}
	eavAgg := []string{}
	eavSelect := []string{}
	eavAttrIDs := []int16{}

	for _, attrName := range sortedAttrKeys(attrCache) {
		meta := attrCache[attrName]
		alias := safeColumnAlias(attrName)
		if meta.ColumnBinding != nil {
			colName := string(meta.ColumnBinding.ColumnName)
			if _, ok := mainColSet[colName]; !ok {
				mainColumns = append(mainColumns, colName)
				mainColSet[colName] = struct{}{}
			}
			expr := castMainValue("m."+colName, meta)
			mainProjections = append(mainProjections, fmt.Sprintf("%s AS %s", expr, alias))
			continue
		}

		castExpr := castEAVValue(meta)
		eavAgg = append(eavAgg, fmt.Sprintf("MAX(CASE WHEN attr_id = %d THEN %s END) AS %s", meta.AttributeID, castExpr, alias))
		eavSelect = append(eavSelect, fmt.Sprintf("e.%s", alias))
		eavAttrIDs = append(eavAttrIDs, meta.AttributeID)
	}

	mQuery := fmt.Sprintf(
		"SELECT %s FROM %s WHERE ltbase_schema_id = %d AND ltbase_deleted_at IS NULL AND %s",
		strings.Join(mainColumns, ", "), entityMain, schemaID, mFilter,
	)

	attrFilter := ""
	if len(eavAttrIDs) > 0 {
		attrFilter = fmt.Sprintf(" AND attr_id IN (%s)", joinInt16(eavAttrIDs))
	}
	eQuery := fmt.Sprintf(
		"SELECT schema_id, row_id, attr_id, value_text FROM %s WHERE schema_id = %d AND %s%s",
		eavData, schemaID, eFilter, attrFilter,
	)

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
	mainSelectCols = append(mainSelectCols, mainProjections...)
	mainSelectCols = append(mainSelectCols, eavSelect...)

	eAggCols := []string{"row_id"}
	if len(eavAgg) > 0 {
		eAggCols = append(eAggCols, eavAgg...)
	}
	eAggSQL := fmt.Sprintf("SELECT %s FROM postgres_query('pg_db', '%s') GROUP BY row_id", strings.Join(eAggCols, ",\n    "), eQueryEsc)

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
`, memoryLimit, pgEsc, strings.Join(mainSelectCols, ",\n  "), mQueryEsc, eAggSQL, s3Esc,
		strings.ToUpper(compression), compressionLevel)

	return sql, mQuery, eQuery, nil
}
