package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"go.uber.org/zap"
)

// DuckExporter handles DuckDB interactions for exporting snapshots to S3 temp path.
type DuckExporter struct {
	DB     *sql.DB
	Logger *zap.Logger
}

type exportModeSpec struct {
	defaultMemoryLimit string
	activeOnly         bool
	useChangeLog       bool
	schemaIDSelect     string
	rowIDSelect        string
	timeSlotSelect     string
	deletedAtSelect    string
}

type exportSQLPlan struct {
	sql            string
	changeLogQuery string
	mainQuery      string
	eavQuery       string
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
		if err := validateS3Credential("s3_access_key_id", s3AccessKey); err != nil {
			db.Close()
			return nil, err
		}
		if _, err := db.ExecContext(ctx2, fmt.Sprintf("SET s3_access_key_id='%s';", s3AccessKey)); err != nil {
			logger.Sugar().Warnw("duckdb set s3_access_key_id failed", "err", err)
		}
	}
	if s3Secret != "" {
		if err := validateS3Credential("s3_secret_access_key", s3Secret); err != nil {
			db.Close()
			return nil, err
		}
		if _, err := db.ExecContext(ctx2, fmt.Sprintf("SET s3_secret_access_key='%s';", s3Secret)); err != nil {
			logger.Sugar().Warnw("duckdb set s3_secret_access_key failed", "err", err)
		}
	}
	// Temporary credentials (STS/assumed roles) are a key+secret+token
	// triple; without the token httpfs signs requests the store rejects even
	// though the SDK client on the same credentials works.
	s3Token := cfg.S3SessionToken
	if s3Token == "" {
		s3Token = os.Getenv("AWS_SESSION_TOKEN")
	}
	if s3Token != "" {
		if err := validateS3Credential("s3_session_token", s3Token); err != nil {
			db.Close()
			return nil, err
		}
		if _, err := db.ExecContext(ctx2, fmt.Sprintf("SET s3_session_token='%s';", s3Token)); err != nil {
			logger.Sugar().Warnw("duckdb set s3_session_token failed", "err", err)
		}
	}
	if cfg.S3Region != "" {
		if err := validateS3Credential("s3_region", cfg.S3Region); err != nil {
			db.Close()
			return nil, err
		}
		if _, err := db.ExecContext(ctx2, fmt.Sprintf("SET s3_region='%s';", cfg.S3Region)); err != nil {
			logger.Sugar().Warnw("duckdb set s3_region failed", "err", err)
		}
	}
	if cfg.S3Endpoint != "" {
		ep := strings.TrimPrefix(strings.TrimPrefix(cfg.S3Endpoint, "https://"), "http://")
		if err := validateS3Credential("s3_endpoint", ep); err != nil {
			db.Close()
			return nil, err
		}
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

	return &DuckExporter{DB: db, Logger: logger}, nil
}

// ExportSnapshotToTmp builds an export SQL and runs COPY to the provided s3TmpPath.
// s3TmpPath is the destination like 's3://bucket/prefix/<schema_id>/_tmp/<tmp_uuid>.parquet'
func (e *DuckExporter) ExportSnapshotToTmp(ctx context.Context, cfg CDCConfig, pgConnStr string, s3TmpPath string, schemaID int16, snapshotTS int64, rowIDs []uuid.UUID, attrCache forma.SchemaAttributeCache) error {
	sql, clQuery, mQuery, eQuery, err := buildExportSQL(pgConnStr, s3TmpPath, cfg, schemaID, snapshotTS, rowIDs, attrCache)
	if err != nil {
		return err
	}

	return e.executeExportSQL(ctx, cfg, "duckdb export sql", "duckdb copy exec", sql, clQuery, mQuery, eQuery)
}

// escapeLiteral doubles single quotes for safe embedding in SQL string literals.
func escapeLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func buildExportSQL(pgConnStr string, s3TmpPath string, cfg CDCConfig, schemaID int16, snapshotTS int64, rowIDs []uuid.UUID, attrCache forma.SchemaAttributeCache) (string, string, string, string, error) {
	plan, err := buildExportSQLPlan(exportModeSpec{
		defaultMemoryLimit: "4GB",
		activeOnly:         false,
		useChangeLog:       true,
		schemaIDSelect:     "cl.schema_id",
		rowIDSelect:        "cl.row_id",
		timeSlotSelect:     "cl.changed_at AS changed_at",
		deletedAtSelect:    "cl.deleted_at",
	}, pgConnStr, s3TmpPath, cfg, schemaID, snapshotTS, rowIDs, attrCache)
	if err != nil {
		return "", "", "", "", err
	}

	return plan.sql, plan.changeLogQuery, plan.mainQuery, plan.eavQuery, nil
}

func (e *DuckExporter) executeExportSQL(ctx context.Context, cfg CDCConfig, logMessage, execErrPrefix, sqlText, clQuery, mQuery, eQuery string) error {
	e.Logger.Sugar().Infow(logMessage, "sql_preview", redactConnStr(sqlText), "cl_query", clQuery, "m_query", mQuery, "e_query", eQuery)
	timeout := cfg.QueryTimeout
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}
	ctx2, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if _, err := e.DB.ExecContext(ctx2, sqlText); err != nil {
		return fmt.Errorf("%s: %w", execErrPrefix, err)
	}
	return nil
}

func buildExportSQLPlan(spec exportModeSpec, pgConnStr string, s3TmpPath string, cfg CDCConfig, schemaID int16, snapshotTS int64, rowIDs []uuid.UUID, attrCache forma.SchemaAttributeCache) (exportSQLPlan, error) {
	if len(rowIDs) == 0 {
		modeName := "base"
		if spec.useChangeLog {
			modeName = "snapshot"
		}
		return exportSQLPlan{}, fmt.Errorf("export %s: no row ids provided", modeName)
	}

	pgEsc := escapeLiteral(pgConnStr)
	s3Esc := escapeLiteral(s3TmpPath)
	entityMain, eavData := resolveMainAndEAVTableNames(cfg)
	rowList := quoteUUIDList(rowIDs)
	mFilter := fmt.Sprintf("ltbase_row_id IN (%s)", rowList)
	eFilter := fmt.Sprintf("row_id IN (%s)", rowList)

	plan := exportSQLPlan{}
	if spec.useChangeLog {
		changeLog := sanitizeIdentifier(cfg.ChangeLogTable)
		if changeLog == "" {
			changeLog = "change_log"
		}
		clFilter := fmt.Sprintf("row_id IN (%s)", rowList)
		plan.changeLogQuery = fmt.Sprintf(
			"SELECT schema_id, row_id, changed_at, deleted_at FROM %s WHERE schema_id = %d AND flushed_at = 0 AND changed_at <= %d AND %s",
			changeLog, schemaID, snapshotTS, clFilter,
		)
	}

	opts := resolveExportSQLOptions(cfg, spec.defaultMemoryLimit)
	copyOptions := buildParquetCopyOptions(opts)

	if len(attrCache) == 0 {
		// A resolvable attribute cache is mandatory: without it we cannot derive
		// the numeric-family typing (bool as 1/0, dates as epoch ms) the
		// federated reader expects, and the reader itself fails fast for no-cache
		// schemas (ErrSchemaMetadataCacheRequired). Pre-flight validation in the
		// flush/init loops should already have aborted; this is defense in depth
		// for any caller that bypasses it (#193).
		return exportSQLPlan{}, fmt.Errorf("build export SQL for schema %d: attribute metadata cache is required but empty: %w", schemaID, ErrSchemaAttrCacheUnavailable)
	}

	projection, err := buildSchemaDrivenProjection(attrCache)
	if err != nil {
		return exportSQLPlan{}, fmt.Errorf("build export projection for schema %d: %w", schemaID, err)
	}
	plan.mainQuery = buildMainEntityQuery(entityMain, schemaID, projection.mainColumns, mFilter, spec.activeOnly)
	plan.eavQuery = buildEAVQuery(eavData, schemaID, eFilter, projection.eavAttrIDs)
	mainSelectCols := append(spec.baseSelectColumns(), projection.mainProjections...)
	mainSelectCols = append(mainSelectCols, projection.eavSelect...)
	plan.sql = buildProjectedExportSQL(spec, opts, copyOptions, pgEsc, s3Esc, plan.changeLogQuery, plan.mainQuery, plan.eavQuery, mainSelectCols, projection.eavAgg)

	return plan, nil
}

func (spec exportModeSpec) baseSelectColumns() []string {
	return []string{
		spec.schemaIDSelect,
		spec.rowIDSelect,
		spec.timeSlotSelect,
		spec.deletedAtSelect,
		"m.ltbase_created_at",
		"m.ltbase_updated_at",
		"m.ltbase_deleted_at",
	}
}

func buildProjectedExportSQL(spec exportModeSpec, opts exportSQLOptions, copyOptions, pgEsc, s3Esc, clQuery, mQuery, eQuery string, mainSelectCols, eavAgg []string) string {
	mQueryEsc := escapeLiteral(mQuery)
	eQueryEsc := escapeLiteral(eQuery)
	eAggSQL := buildEAVAggregationSQL(eQueryEsc, eavAgg)

	if spec.useChangeLog {
		// LEFT JOIN to entity_main so change_log tombstones of hard-deleted
		// rows are exported instead of silently dropped (#173).
		clQueryEsc := escapeLiteral(clQuery)
		return fmt.Sprintf(`PRAGMA memory_limit='%s';
ATTACH IF NOT EXISTS '%s' AS pg_db (TYPE postgres, READ_ONLY);

COPY (
SELECT
  %s
FROM postgres_query('pg_db', '%s') cl
LEFT JOIN postgres_query('pg_db', '%s') m
  ON cl.row_id = m.ltbase_row_id
LEFT JOIN (
  %s
) e ON cl.row_id = e.row_id
) TO '%s' (%s);
`, opts.memoryLimit, pgEsc, strings.Join(mainSelectCols, ",\n  "), clQueryEsc, mQueryEsc, eAggSQL, s3Esc, copyOptions)
	}

	return fmt.Sprintf(`PRAGMA memory_limit='%s';
ATTACH IF NOT EXISTS '%s' AS pg_db (TYPE postgres, READ_ONLY);

COPY (
SELECT
  %s
FROM postgres_query('pg_db', '%s') m
LEFT JOIN (
  %s
) e ON m.ltbase_row_id = e.row_id
) TO '%s' (%s);
`, opts.memoryLimit, pgEsc, strings.Join(mainSelectCols, ",\n  "), mQueryEsc, eAggSQL, s3Esc, copyOptions)
}

func quoteUUIDList(ids []uuid.UUID) string {
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("UUID '%s'", id.String()))
	}
	return strings.Join(parts, ", ")
}

func sortedAttrKeys(cache forma.SchemaAttributeCache) []string {
	keys := make([]string, 0, len(cache))
	for k := range cache {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// safeColumnAlias derives the parquet column name for an attribute. It
// delegates to the shared writer/reader contract in sqlgen: the federated
// reader projects the same names back out of parquet (#260).
func safeColumnAlias(name string) string {
	return sqlgen.ParquetAttrColumn(name)
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

// castMainValue projects a main-column-bound attribute into its parquet
// column. The output type must match what the federated reader consumes:
// its projection casts date/datetime attrs with CAST(attr AS BIGINT)
// (sqlgen.duckDBAttrCast) and scans them as epoch-ms int64. So every
// date/datetime encoding must export epoch-ms BIGINT here, matching
// castEAVValue's date handling (#194, #219).
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
	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		return castDateMainValue(col, meta)
	case forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeNumeric:
		return fmt.Sprintf("TRY_CAST(%s AS %s)", col, duckTypeForValue(meta.ValueType))
	case forma.ValueTypeUUID:
		return fmt.Sprintf("TRY_CAST(%s AS UUID)", col)
	default:
		return fmt.Sprintf("CAST(%s AS VARCHAR)", col)
	}
}

// castDateMainValue normalizes a date/datetime main column to epoch-ms BIGINT
// regardless of its declared storage encoding, so the federated reader's
// CAST(attr AS BIGINT) projection and epoch-ms BIGINT predicate binds (#200)
// receive a consistent value on every tier (#219). The two storable shapes:
//   - iso8601: an RFC3339 string in a text column -> parse to TIMESTAMP, then
//     epoch_ms() yields BIGINT milliseconds.
//   - unix_ms / default: epoch millis already in a bigint column -> pass
//     through as BIGINT.
//
// Before #219 the non-unix_ms branch exported a native DATE/TIMESTAMP column,
// which the reader's CAST(... AS BIGINT) reinterpreted as days- or
// microseconds-since-epoch — silently off from the epoch-ms convention by a
// factor of 10^3-10^8 on the warm/cold tiers.
func castDateMainValue(col string, meta forma.AttributeMetadata) string {
	if meta.ColumnBinding != nil && meta.ColumnBinding.Encoding == forma.MainColumnEncodingISO8601 {
		return fmt.Sprintf("epoch_ms(TRY_CAST(%s AS TIMESTAMP))", col)
	}
	return fmt.Sprintf("TRY_CAST(%s AS BIGINT)", col)
}

// castEAVValue projects an eav_data value into the parquet column for its
// value type. It must read the column the mainline write path populates
// (transform.populateTypedValue): numeric-family types (including bool as
// 1/0 and date/datetime as epoch millis) live in value_numeric; text and
// uuid live in value_text. The output types mirror the federated reader's
// EAV pivot (sqlgen.BuildSchemaProjection): BOOLEAN for bool, epoch-ms
// BIGINT for dates, native numeric types otherwise (#173).
// 热端 pivot(sqlgen.buildEAVPivotExpr)必须与本映射保持一致(#205)。
func castEAVValue(meta forma.AttributeMetadata) string {
	switch meta.ValueType {
	case forma.ValueTypeBool:
		return "(value_numeric <> 0)"
	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		return "TRY_CAST(value_numeric AS BIGINT)"
	case forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeNumeric:
		return fmt.Sprintf("TRY_CAST(value_numeric AS %s)", duckTypeForValue(meta.ValueType))
	case forma.ValueTypeUUID:
		return "CAST(value_text AS VARCHAR)"
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
