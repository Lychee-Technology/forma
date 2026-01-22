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
	pragmas := []string{
		fmt.Sprintf("PRAGMA memory_limit='%dMB';", cfg.DuckDBMemoryMB),
		fmt.Sprintf("PRAGMA threads=%d;", cfg.DuckDBThreads),
	}
	for _, p := range pragmas {
		if _, err := db.ExecContext(ctx2, p); err != nil {
			logger.Sugar().Warnw("duckdb pragma failed", "pragma", p, "err", err)
		}
	}
	// attempt to install/load extensions
	exts := []string{"httpfs", "parquet", "postgres_scanner"}
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
		ep := strings.TrimPrefix(cfg.S3Endpoint, "http://")
		if _, err := db.ExecContext(ctx2, fmt.Sprintf("SET s3_endpoint='%s';", ep)); err != nil {
			logger.Sugar().Warnw("duckdb set s3_endpoint failed", "err", err)
		}
		if _, err := db.ExecContext(ctx2, "SET s3_use_ssl=false;"); err != nil {
			logger.Sugar().Warnw("duckdb set s3_use_ssl failed", "err", err)
		}
		if _, err := db.ExecContext(ctx2, "SET s3_url_style='path';"); err != nil {
			logger.Sugar().Warnw("duckdb set s3_url_style failed", "err", err)
		}
	}

	return &DuckExporter{DB: db, Logger: logger}, nil
}

// ExportSnapshotToTmp builds an export SQL and runs COPY to the provided s3TmpPath.
// s3TmpPath is the destination like 's3://bucket/prefix/delta/<schema>/_tmp/<tmp_uuid>.parquet'
func (e *DuckExporter) ExportSnapshotToTmp(ctx context.Context, pgConnStr string, s3TmpPath string, schemaID int16, snapshotTS int64) error {
	// Escape single quotes in the connection string and s3 path before embedding
	pgEsc := strings.ReplaceAll(pgConnStr, "'", "''")
	s3Esc := strings.ReplaceAll(s3TmpPath, "'", "''")

	// Build SQL using postgres_scan to read change_log/entity_main/eav_data and pivot EAV.
	// This is a simplified projection; adapt as needed to match production projection.
	sql := fmt.Sprintf(`PRAGMA memory_limit='2048MB';
COPY (
SELECT
  m.ltbase_row_id AS row_id,
  m.ltbase_created_at AS created_at,
  cl.changed_at AS ver_ts,
  cl.deleted_at AS deleted_ts,
  CAST(m.text_01 AS VARCHAR) AS name,
  CAST(m.integer_01 AS INTEGER) AS age,
  MAX(CASE WHEN e.attr_id = 205 THEN CAST(e.value_text AS VARCHAR) END) AS tag
FROM postgres_scan('%s', 'change_log', 'schema_id = %d AND flushed_at = 0 AND changed_at <= %d') cl
JOIN postgres_scan('%s', 'entity_main', 'ltbase_schema_id = %d') m
  ON cl.row_id = m.ltbase_row_id
LEFT JOIN postgres_scan('%s', 'eav_data', 'schema_id = %d') e
  ON cl.row_id = e.row_id
GROUP BY m.ltbase_row_id, m.ltbase_created_at, cl.changed_at, cl.deleted_at, m.text_01, m.integer_01
) TO '%s' (FORMAT PARQUET, COMPRESSION 'ZSTD');
`, pgEsc, schemaID, snapshotTS, pgEsc, schemaID, pgEsc, schemaID, s3Esc)

	e.Logger.Sugar().Infow("duckdb export sql", "sql_preview", sql[:400])
	ctx2, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	if _, err := e.DB.ExecContext(ctx2, sql); err != nil {
		return fmt.Errorf("duckdb copy exec: %w", err)
	}
	return nil
}
