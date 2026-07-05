package federated

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

// DuckDBClient wraps a database/sql DB opened with the DuckDB driver.
type DuckDBClient struct {
	DB  *sql.DB
	cfg forma.DuckDBConfig
}

// ValidateDuckDBConfig performs basic sanity checks on user-provided DuckDB configuration.
func ValidateDuckDBConfig(cfg forma.DuckDBConfig) error {
	if !cfg.Enabled {
		// disabled is acceptable; nothing to validate
		return nil
	}
	if cfg.MemoryLimitMB < 0 {
		return fmt.Errorf("invalid memory_limit_mb: must be >= 0")
	}
	if cfg.MaxParallelism < 0 {
		return fmt.Errorf("invalid max_parallelism: must be >= 0")
	}
	if cfg.MaxConnections < 1 {
		return fmt.Errorf("max_connections must be >= 1")
	}
	if cfg.QueryTimeout <= 0 {
		return fmt.Errorf("query_timeout must be > 0")
	}
	// DBPath may be empty (in-memory), so no strict check here
	return nil
}

// validateS3Credential checks that an S3 credential value is safe to embed in a DuckDB SET
// statement. DuckDB's PRAGMA/SET does not support parameterized queries, so we validate
// the value against an allowlist of characters instead.
// Rejected characters: single-quote ('), double-quote ("), semicolon (;), and backslash (\).
func validateS3Credential(name, value string) error {
	const forbidden = `'";\ `
	for _, ch := range forbidden {
		if strings.ContainsRune(value, ch) {
			return fmt.Errorf("S3 credential %q contains forbidden character %q; DuckDB SET does not support parameterized queries", name, string(ch))
		}
	}
	return nil
}

// NewDuckDBClient creates and configures a DuckDB client according to the provided config.
// It attempts to load common extensions (httpfs/parquet) and configure S3 access via PRAGMA when requested.
func NewDuckDBClient(cfg forma.DuckDBConfig) (*DuckDBClient, error) {
	return NewDuckDBClientContext(context.Background(), cfg)
}

// NewDuckDBClientContext creates and configures a DuckDB client while honoring
// the caller-provided context during bootstrap.
func NewDuckDBClientContext(ctx context.Context, cfg forma.DuckDBConfig) (*DuckDBClient, error) {
	if !cfg.Enabled {
		return nil, fmt.Errorf("duckdb disabled in config")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	dsn := cfg.DBPath
	if dsn == "" {
		dsn = ":memory:"
	}

	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	// Apply a small connection configuration.
	// File-backed DuckDB in read/write mode is effectively single-writer; more
	// than one pooled connection can surface "database locked" errors under load.
	db.SetMaxOpenConns(1)
	if cfg.MaxConnections > 0 {
		if cfg.MaxConnections > 1 && dsn != ":memory:" {
			zap.S().Warnw("duckdb: MaxConnections > 1 with file-backed database may cause database locked errors under concurrent load",
				"dbPath", dsn,
				"maxConnections", cfg.MaxConnections,
			)
		}
		db.SetMaxOpenConns(cfg.MaxConnections)
	}

	// Try a quick ping with timeout
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping duckdb: %w", err)
	}

	if err := configureExtensions(ctx, db, cfg); err != nil {
		db.Close()
		return nil, err
	}

	if err := configureS3(ctx, db, cfg); err != nil {
		db.Close()
		return nil, err
	}

	if err := applyResourcePragmas(ctx, db, cfg); err != nil {
		db.Close()
		return nil, err
	}

	return &DuckDBClient{DB: db, cfg: cfg}, nil
}

// configureExtensions installs and loads user-specified extensions, plus httpfs (when S3
// is enabled) and parquet (when parquet is enabled).
func configureExtensions(ctx context.Context, db *sql.DB, cfg forma.DuckDBConfig) error {
	// User-supplied extensions
	for _, ext := range cfg.Extensions {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("INSTALL %s;", ext)); err != nil {
			zap.S().Warnw("duckdb: install extension failed", "extension", ext, "err", err)
			continue
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("LOAD %s;", ext)); err != nil {
			zap.S().Warnw("duckdb: load extension failed", "extension", ext, "err", err)
		}
	}

	// httpfs — required for S3 access
	if cfg.EnableS3 {
		if _, err := db.ExecContext(ctx, "INSTALL httpfs;"); err == nil {
			if _, err := db.ExecContext(ctx, "LOAD httpfs;"); err != nil {
				zap.S().Warnw("duckdb: load httpfs failed", "err", err)
			}
		} else {
			zap.S().Warnw("duckdb: install httpfs failed", "err", err)
		}
	}

	// parquet extension
	if cfg.EnableParquet {
		if _, err := db.ExecContext(ctx, "INSTALL parquet;"); err == nil {
			if _, err := db.ExecContext(ctx, "LOAD parquet;"); err != nil {
				zap.S().Warnw("duckdb: load parquet failed", "err", err)
			}
		} else {
			zap.S().Warnw("duckdb: install parquet failed", "err", err)
		}
	}

	return nil
}

// configureS3 sets DuckDB S3 PRAGMA values when S3 is enabled in the config.
// Returns an error (and expects the caller to close the DB) if a credential fails
// the character-denylist validation.
func configureS3(ctx context.Context, db *sql.DB, cfg forma.DuckDBConfig) error {
	if !cfg.EnableS3 {
		return nil
	}

	if cfg.S3AccessKey != "" {
		if err := validateS3Credential("s3_access_key_id", cfg.S3AccessKey); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("SET s3_access_key_id='%s';", cfg.S3AccessKey)); err != nil {
			zap.S().Warnw("duckdb: set s3_access_key_id failed", "err", err)
		}
	}
	if cfg.S3SecretKey != "" {
		if err := validateS3Credential("s3_secret_access_key", cfg.S3SecretKey); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("SET s3_secret_access_key='%s';", cfg.S3SecretKey)); err != nil {
			zap.S().Warnw("duckdb: set s3_secret_access_key failed", "err", err)
		}
	}
	if cfg.S3Region != "" {
		if err := validateS3Credential("s3_region", cfg.S3Region); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("SET s3_region='%s';", cfg.S3Region)); err != nil {
			zap.S().Warnw("duckdb: set s3_region failed", "err", err)
		}
	}
	if cfg.S3Endpoint != "" {
		endpoint := strings.TrimPrefix(cfg.S3Endpoint, "http://")
		if err := validateS3Credential("s3_endpoint", endpoint); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("SET s3_endpoint='%s';", endpoint)); err != nil {
			zap.S().Warnw("duckdb: set s3_endpoint failed", "err", err)
		}
		if _, err := db.ExecContext(ctx, "SET s3_use_ssl=false;"); err != nil {
			zap.S().Warnw("duckdb: set s3_use_ssl failed", "err", err)
		}
		if _, err := db.ExecContext(ctx, "SET s3_url_style='path';"); err != nil {
			zap.S().Warnw("duckdb: set s3_url_style failed", "err", err)
		}
	}

	return nil
}

// applyResourcePragmas sets memory_limit and thread-count pragmas when the config requests them.
func applyResourcePragmas(ctx context.Context, db *sql.DB, cfg forma.DuckDBConfig) error {
	if cfg.MemoryLimitMB > 0 {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA memory_limit='%dMB';", cfg.MemoryLimitMB)); err != nil {
			zap.S().Warnw("duckdb: set memory_limit failed", "err", err, "memoryLimitMB", cfg.MemoryLimitMB)
		}
	}
	if cfg.MaxParallelism > 0 {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA threads=%d;", cfg.MaxParallelism)); err != nil {
			zap.S().Warnw("duckdb: set threads failed", "err", err, "maxParallelism", cfg.MaxParallelism)
		}
	}
	return nil
}

// Close closes the underlying DuckDB DB.
func (c *DuckDBClient) Close() error {
	if c == nil || c.DB == nil {
		return nil
	}
	return c.DB.Close()
}

// HealthCheck performs a simple query to validate the DuckDB connection and basic runtime pragmas.
func (c *DuckDBClient) HealthCheck(ctx context.Context) error {
	if c == nil || c.DB == nil {
		return fmt.Errorf("duckdb client not initialized")
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	// Basic liveliness check
	row := c.DB.QueryRowContext(ctx, "SELECT 1;")
	var v int
	if err := row.Scan(&v); err != nil {
		return fmt.Errorf("duckdb health query failed: %w", err)
	}
	if v != 1 {
		return fmt.Errorf("unexpected duckdb health result: %d", v)
	}

	// Best-effort validation of configured pragmas
	if c.cfg.MemoryLimitMB > 0 {
		var mem string
		if err := c.DB.QueryRowContext(ctx, "PRAGMA memory_limit;").Scan(&mem); err != nil {
			zap.S().Warnw("duckdb: memory_limit pragma query failed (non-fatal)", "err", err)
		} else {
			if mem == "" {
				zap.S().Warnw("duckdb: memory_limit pragma returned empty (non-fatal)")
			}
		}
	}

	if c.cfg.MaxParallelism > 0 {
		var threads int
		if err := c.DB.QueryRowContext(ctx, "PRAGMA threads;").Scan(&threads); err != nil {
			zap.S().Warnw("duckdb: threads pragma query failed (non-fatal)", "err", err)
		} else {
			if threads <= 0 {
				zap.S().Warnw("duckdb: threads pragma invalid (non-fatal)", "threads", threads)
			}
		}
	}

	// Verify S3-related pragmas if S3 is enabled or S3 config provided (best-effort)
	if c.cfg.EnableS3 {
		// If an endpoint or region was configured, confirm the PRAGMA returns a value (may be empty if not set)
		if c.cfg.S3Endpoint != "" {
			var ep string
			if err := c.DB.QueryRowContext(ctx, "PRAGMA s3_endpoint;").Scan(&ep); err != nil {
				zap.S().Warnw("duckdb: s3_endpoint pragma query failed", "err", err)
			}
		}
		if c.cfg.S3Region != "" {
			var reg string
			if err := c.DB.QueryRowContext(ctx, "PRAGMA s3_region;").Scan(&reg); err != nil {
				zap.S().Warnw("duckdb: s3_region pragma query failed", "err", err)
			}
		}
	}

	// Parquet availability is best-effort checked via a benign pragma; failure to surface is non-fatal.
	if c.cfg.EnableParquet {
		if _, err := c.DB.ExecContext(ctx, "PRAGMA compile_options;"); err != nil {
			zap.S().Warnw("duckdb: parquet availability check failed (non-fatal)", "err", err)
		}
	}

	return nil
}
