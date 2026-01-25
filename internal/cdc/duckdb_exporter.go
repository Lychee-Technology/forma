package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

// DuckExporter handles DuckDB interactions for exporting snapshots to S3 temp path.
type DuckExporter struct {
	DB     *sql.DB
	Logger *zap.Logger
	Config CDCConfig // keep config for compression settings
}

// NewDuckExporter opens a DuckDB connection and configures pragmas and extensions.
func NewDuckExporter(ctx context.Context, cfg CDCConfig, s3AccessKey, s3Secret string, logger *zap.Logger) (*DuckExporter, error) {
	// Build DSN
	dsn := cfg.DuckDBPath
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	// configure pragmas and extensions
	ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if cfg.DuckMemLimit != "" {
		if _, err := db.ExecContext(ctx2, fmt.Sprintf("PRAGMA memory_limit='%s';", cfg.DuckMemLimit)); err != nil {
			logger.Sugar().Warnw("duckdb pragma failed", "pragma", "memory_limit", "err", err)
		}
	}
	if cfg.DuckThreads > 0 {
		if _, err := db.ExecContext(ctx2, fmt.Sprintf("PRAGMA threads=%d;", cfg.DuckThreads)); err != nil {
			logger.Sugar().Warnw("duckdb pragma failed", "pragma", "threads", "err", err)
		}
	}
	// attempt to install/load extensions (postgres_scanner first for postgres_query)
	exts := []string{"postgres_scanner", "httpfs", "parquet"}
	for _, e := range exts {
		if _, err := db.ExecContext(ctx2, "INSTALL "+e+";"); err != nil {
			logger.Sugar().Warnw("duckdb install extension failed", "ext", e, "err", err)
		} else {
			if _, err := db.ExecContext(ctx2, "LOAD "+e+";"); err != nil {
				logger.Sugar().Warnw("duckdb load extension failed", "ext", e, "err", err)
			}
		}
	}
	// set S3 pragmas if provided
	if s3AccessKey != "" {
		if _, err := db.ExecContext(ctx2, fmt.Sprintf("SET s3_access_key_id='%s';", s3AccessKey)); err != nil {
			logger.Sugar().Warnw("duckdb set s3_access_key_id failed", "err", err)
		}
	}
	if s3Secret != "" {
		if _, err := db.ExecContext(ctx2, fmt.Sprintf("SET s3_secret_access_key='%s';", s3Secret)); err != nil {
			logger.Sugar().Warnw("duckdb set s3_secret_access_key failed", "err", err)
		}
	}
	if cfg.S3Region != "" {
		if _, err := db.ExecContext(ctx2, fmt.Sprintf("SET s3_region='%s';", cfg.S3Region)); err != nil {
			logger.Sugar().Warnw("duckdb set s3_region failed", "err", err)
		}
	}
	if cfg.S3Endpoint != "" {
		ep := strings.TrimPrefix(strings.TrimPrefix(cfg.S3Endpoint, "https://"), "http://")
		if _, err := db.ExecContext(ctx2, fmt.Sprintf("SET s3_endpoint='%s';", ep)); err != nil {
			logger.Sugar().Warnw("duckdb set s3_endpoint failed", "err", err)
		}
	}
	// Configure SSL
	sslVal := "true"
	if !cfg.S3UseSSL {
		sslVal = "false"
	}
	if _, err := db.ExecContext(ctx2, fmt.Sprintf("SET s3_use_ssl=%s;", sslVal)); err != nil {
		logger.Sugar().Warnw("duckdb set s3_use_ssl failed", "err", err)
	}
	// Configure URL style (path vs virtual-hosted)
	if cfg.S3UsePath {
		if _, err := db.ExecContext(ctx2, "SET s3_url_style='path';"); err != nil {
			logger.Sugar().Warnw("duckdb set s3_url_style failed", "err", err)
		}
	}

	return &DuckExporter{DB: db, Logger: logger, Config: cfg}, nil
}

// ExportSnapshotToTmp builds an export SQL and runs COPY to the provided s3TmpPath.
// s3TmpPath is the destination like 's3://bucket/prefix/<schema_id>/_tmp/<tmp_uuid>.parquet'
func (e *DuckExporter) ExportSnapshotToTmp(ctx context.Context, pgConnStr string, s3TmpPath string, schemaID int16, snapshotTS int64, rowIDs []uuid.UUID, attrCache forma.SchemaAttributeCache) error {
	sql, clQuery, mQuery, eQuery, err := buildExportSQL(pgConnStr, s3TmpPath, e.Config, schemaID, snapshotTS, rowIDs, attrCache)
	if err != nil {
		return err
	}

	e.Logger.Sugar().Infow("duckdb export sql", "sql_preview", sql, "cl_query", clQuery, "m_query", mQuery, "e_query", eQuery)
	timeout := e.Config.QueryTimeout
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}
	ctx2, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if _, err := e.DB.ExecContext(ctx2, sql); err != nil {
		return fmt.Errorf("duckdb copy exec: %w", err)
	}
	return nil
}

// escapeLiteral doubles single quotes for safe embedding in SQL string literals.
func escapeLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func buildExportSQL(pgConnStr string, s3TmpPath string, cfg CDCConfig, schemaID int16, snapshotTS int64, rowIDs []uuid.UUID, attrCache forma.SchemaAttributeCache) (string, string, string, string, error) {
	if len(rowIDs) == 0 {
		return "", "", "", "", fmt.Errorf("export snapshot: no row ids provided")
	}

	pgEsc := escapeLiteral(pgConnStr)
	s3Esc := escapeLiteral(s3TmpPath)

	changeLog := sanitizeIdentifier(cfg.ChangeLogTable)
	if changeLog == "" {
		changeLog = "change_log"
	}
	entityMain := "entity_main"
	eavData := "eav_data"

	rowList := quoteUUIDList(rowIDs)
	clFilter := fmt.Sprintf("row_id IN (%s)", rowList)
	mFilter := fmt.Sprintf("ltbase_row_id IN (%s)", rowList)
	eFilter := fmt.Sprintf("row_id IN (%s)", rowList)

	clQuery := fmt.Sprintf(
		"SELECT schema_id, row_id, changed_at, deleted_at FROM %s WHERE schema_id = %d AND flushed_at = 0 AND changed_at <= %d AND %s",
		changeLog, schemaID, snapshotTS, clFilter,
	)

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
		memoryLimit = "4GB"
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
			"smallint_01", "smallint_02",
			"integer_01", "integer_02", "integer_03",
			"bigint_01", "bigint_02", "bigint_03", "bigint_04", "bigint_05",
			"double_01", "double_02", "double_03", "double_04", "double_05",
			"uuid_01", "uuid_02",
		}
		mQuery := fmt.Sprintf(
			"SELECT %s FROM %s WHERE ltbase_schema_id = %d AND %s",
			strings.Join(mColumns, ", "), entityMain, schemaID, mFilter,
		)
		eQuery := fmt.Sprintf(
			"SELECT schema_id, row_id, attr_id, value_text FROM %s WHERE schema_id = %d AND %s",
			eavData, schemaID, eFilter,
		)

		clQueryEsc := escapeLiteral(clQuery)
		mQueryEsc := escapeLiteral(mQuery)
		eQueryEsc := escapeLiteral(eQuery)

		mainSelectCols := append([]string{
			"cl.schema_id",
			"cl.row_id",
			"cl.changed_at AS time_slot",
			"cl.deleted_at",
			"m.ltbase_created_at",
			"m.ltbase_updated_at",
			"m.ltbase_deleted_at",
		}, prefixColumns("m.", mColumns[5:])...)

		sql := fmt.Sprintf(`PRAGMA memory_limit='%s';
ATTACH IF NOT EXISTS '%s' AS pg_db (TYPE postgres, READ_ONLY);

COPY (
SELECT
  %s,
  e.attributes
FROM postgres_query('pg_db', '%s') cl
JOIN postgres_query('pg_db', '%s') m
  ON cl.row_id = m.ltbase_row_id
LEFT JOIN (
  SELECT row_id, list(struct_pack(attr_id := attr_id, value_text := value_text)) AS attributes
  FROM postgres_query('pg_db', '%s')
  GROUP BY row_id
) e ON cl.row_id = e.row_id
) TO '%s' (FORMAT PARQUET, COMPRESSION '%s', COMPRESSION_LEVEL %d);
`, memoryLimit, pgEsc, strings.Join(mainSelectCols, ",\n  "), clQueryEsc, mQueryEsc, eQueryEsc, s3Esc,
			strings.ToUpper(compression), compressionLevel)

		return sql, clQuery, mQuery, eQuery, nil
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
		"SELECT %s FROM %s WHERE ltbase_schema_id = %d AND %s",
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

	clQueryEsc := escapeLiteral(clQuery)
	mQueryEsc := escapeLiteral(mQuery)
	eQueryEsc := escapeLiteral(eQuery)

	mainSelectCols := []string{
		"cl.schema_id",
		"cl.row_id",
		"cl.changed_at AS time_slot",
		"cl.deleted_at",
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
FROM postgres_query('pg_db', '%s') cl
JOIN postgres_query('pg_db', '%s') m
  ON cl.row_id = m.ltbase_row_id
LEFT JOIN (
  %s
) e ON cl.row_id = e.row_id
) TO '%s' (FORMAT PARQUET, COMPRESSION '%s', COMPRESSION_LEVEL %d);
`, memoryLimit, pgEsc, strings.Join(mainSelectCols, ",\n  "), clQueryEsc, mQueryEsc, eAggSQL, s3Esc,
		strings.ToUpper(compression), compressionLevel)

	return sql, clQuery, mQuery, eQuery, nil
}

func quoteUUIDList(ids []uuid.UUID) string {
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("UUID '%s'", id.String()))
	}
	return strings.Join(parts, ", ")
}

func prefixColumns(prefix string, cols []string) []string {
	res := make([]string, 0, len(cols))
	for _, c := range cols {
		res = append(res, prefix+c)
	}
	return res
}

func sortedAttrKeys(cache forma.SchemaAttributeCache) []string {
	keys := make([]string, 0, len(cache))
	for k := range cache {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func safeColumnAlias(name string) string {
	replacer := strings.NewReplacer("`", "", ".", "_", " ", "_", "[", "", "]", "")
	alias := replacer.Replace(name)
	if alias == "" {
		alias = "attr"
	}
	return alias
}

func duckTypeForValue(v forma.ValueType) string {
	switch v {
	case forma.ValueTypeText:
		return "VARCHAR"
	case forma.ValueTypeSmallInt:
		return "SMALLINT"
	case forma.ValueTypeInteger:
		return "INTEGER"
	case forma.ValueTypeBigInt:
		return "BIGINT"
	case forma.ValueTypeNumeric:
		return "DOUBLE"
	case forma.ValueTypeDate:
		return "DATE"
	case forma.ValueTypeDateTime:
		return "TIMESTAMP"
	case forma.ValueTypeUUID:
		return "UUID"
	case forma.ValueTypeBool:
		return "BOOLEAN"
	default:
		return "VARCHAR"
	}
}

func castMainValue(col string, meta forma.AttributeMetadata) string {
	switch meta.ValueType {
	case forma.ValueTypeBool:
		switch meta.ColumnBinding.Encoding {
		case forma.MainColumnEncodingBoolInt:
			return fmt.Sprintf("CASE WHEN %s <> 0 THEN TRUE ELSE FALSE END", col)
		case forma.MainColumnEncodingBoolText:
			return fmt.Sprintf("CASE WHEN LOWER(%s) IN ('true','1') THEN TRUE ELSE FALSE END", col)
		default:
			return fmt.Sprintf("TRY_CAST(%s AS BOOLEAN)", col)
		}
	case forma.ValueTypeDate:
		if meta.ColumnBinding != nil && meta.ColumnBinding.Encoding == forma.MainColumnEncodingUnixMs {
			return fmt.Sprintf("CAST(to_timestamp(CAST(%s AS DOUBLE)/1000) AS DATE)", col)
		}
		return fmt.Sprintf("TRY_CAST(%s AS DATE)", col)
	case forma.ValueTypeDateTime:
		if meta.ColumnBinding != nil && meta.ColumnBinding.Encoding == forma.MainColumnEncodingUnixMs {
			return fmt.Sprintf("to_timestamp(CAST(%s AS DOUBLE)/1000)", col)
		}
		return fmt.Sprintf("TRY_CAST(%s AS TIMESTAMP)", col)
	case forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeNumeric:
		return fmt.Sprintf("TRY_CAST(%s AS %s)", col, duckTypeForValue(meta.ValueType))
	case forma.ValueTypeUUID:
		return fmt.Sprintf("TRY_CAST(%s AS UUID)", col)
	default:
		return fmt.Sprintf("CAST(%s AS VARCHAR)", col)
	}
}

func castEAVValue(meta forma.AttributeMetadata) string {
	switch meta.ValueType {
	case forma.ValueTypeBool:
		return "CASE WHEN lower(value_text) IN ('true','1','t','yes','y') THEN TRUE WHEN lower(value_text) IN ('false','0','f','no','n') THEN FALSE ELSE NULL END"
	case forma.ValueTypeDate:
		return "TRY_CAST(value_text AS DATE)"
	case forma.ValueTypeDateTime:
		return "TRY_CAST(value_text AS TIMESTAMP)"
	case forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeNumeric:
		return fmt.Sprintf("TRY_CAST(value_text AS %s)", duckTypeForValue(meta.ValueType))
	case forma.ValueTypeUUID:
		return "TRY_CAST(value_text AS UUID)"
	default:
		return "CAST(value_text AS VARCHAR)"
	}
}

func joinInt16(vals []int16) string {
	parts := make([]string, 0, len(vals))
	for _, v := range vals {
		parts = append(parts, fmt.Sprintf("%d", v))
	}
	return strings.Join(parts, ", ")
}
