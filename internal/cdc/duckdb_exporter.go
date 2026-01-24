package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
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
// s3TmpPath is the destination like 's3://bucket/prefix/delta/<schema>/_tmp/<tmp_uuid>.parquet'
func (e *DuckExporter) ExportSnapshotToTmp(ctx context.Context, pgConnStr string, s3TmpPath string, schemaID int16, snapshotTS int64) error {
	// Escape single quotes in the connection string and s3 path before embedding
	pgEsc := escapeLiteral(pgConnStr)
	s3Esc := escapeLiteral(s3TmpPath)

	// Table names (sanitized) – change log name is configurable, others are fixed for now
	changeLog := sanitizeIdentifier(e.Config.ChangeLogTable)
	if changeLog == "" {
		changeLog = "change_log"
	}
	entityMain := "entity_main"
	eavData := "eav_data"

	// Build source queries for postgres_query to avoid passing filters as table names
	clQuery := fmt.Sprintf(
		"SELECT schema_id, row_id, changed_at, deleted_at FROM %s WHERE schema_id = %d AND flushed_at = 0 AND changed_at <= %d",
		changeLog, schemaID, snapshotTS,
	)
	mQuery := fmt.Sprintf(
		"SELECT ltbase_row_id, ltbase_schema_id, ltbase_created_at, text_01, integer_01 FROM %s WHERE ltbase_schema_id = %d",
		entityMain, schemaID,
	)
	eQuery := fmt.Sprintf(
		"SELECT schema_id, row_id, attr_id, value_text FROM %s WHERE schema_id = %d",
		eavData, schemaID,
	)

	clQueryEsc := escapeLiteral(clQuery)
	mQueryEsc := escapeLiteral(mQuery)
	eQueryEsc := escapeLiteral(eQuery)

	// Build compression clause from config
	compression := e.Config.ParquetCompression
	if compression == "" {
		compression = DefaultParquetCompression
	}
	compressionLevel := e.Config.ParquetCompressionLevel
	if compressionLevel <= 0 {
		compressionLevel = DefaultParquetCompressionLevel
	}

	// Build SQL using postgres_query to read change_log/entity_main/eav_data and pivot EAV.
	// This is a simplified projection; adapt as needed to match production projection.
	sql := fmt.Sprintf(`PRAGMA memory_limit='2048MB';
ATTACH IF NOT EXISTS '%s' AS pg_db (TYPE postgres, READ_ONLY);

COPY (
SELECT
  m.ltbase_row_id AS row_id,
  m.ltbase_created_at AS created_at,
  cl.changed_at AS ver_ts,
  cl.deleted_at AS deleted_ts,
  CAST(m.text_01 AS VARCHAR) AS name,
  CAST(m.integer_01 AS INTEGER) AS age,
  MAX(CASE WHEN e.attr_id = 205 THEN CAST(e.value_text AS VARCHAR) END) AS tag
FROM postgres_query('pg_db', '%s') cl
JOIN postgres_query('pg_db', '%s') m
  ON cl.row_id = m.ltbase_row_id
LEFT JOIN postgres_query('pg_db', '%s') e
  ON cl.row_id = e.row_id
GROUP BY m.ltbase_row_id, m.ltbase_created_at, cl.changed_at, cl.deleted_at, m.text_01, m.integer_01
) TO '%s' (FORMAT PARQUET, COMPRESSION '%s', COMPRESSION_LEVEL %d);
`, pgEsc, clQueryEsc, mQueryEsc, eQueryEsc, s3Esc,
		strings.ToUpper(compression), compressionLevel)

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
