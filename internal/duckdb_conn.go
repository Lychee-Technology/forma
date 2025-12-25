package internal

import (
	"context"
	"database/sql"
	"fmt"
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

// global client accessor for simple wiring during initial integration.
var globalDuckDBClient *DuckDBClient

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

// NewDuckDBClient creates and configures a DuckDB client according to the provided config.
// It attempts to load common extensions (httpfs/parquet) and configure S3 access via PRAGMA when requested.
func NewDuckDBClient(cfg forma.DuckDBConfig) (*DuckDBClient, error) {
	if !cfg.Enabled {
		return nil, fmt.Errorf("duckdb disabled in config")
	}

	dsn := cfg.DBPath
	if dsn == "" {
		dsn = ":memory:"
	}

	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	// Apply a small connection configuration
	db.SetMaxOpenConns(1) // DuckDB typically uses a single connection
	if cfg.MaxConnections > 0 {
		db.SetMaxOpenConns(cfg.MaxConnections)
	}

	// Try a quick ping with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping duckdb: %w", err)
	}

	// Load and configure extensions
	if len(cfg.Extensions) > 0 {
		for _, ext := range cfg.Extensions {
			if _, err := db.ExecContext(ctx, fmt.Sprintf("INSTALL %s;", ext)); err != nil {
				zap.S().Warnw("duckdb: install extension failed", "extension", ext, "err", err)
				continue
			}
			if _, err := db.ExecContext(ctx, fmt.Sprintf("LOAD %s;", ext)); err != nil {
				zap.S().Warnw("duckdb: load extension failed", "extension", ext, "err", err)
			}
		}
	}

	// Common extensions
	if cfg.EnableS3 {
		if _, err := db.ExecContext(ctx, "INSTALL httpfs;"); err == nil {
			if _, err := db.ExecContext(ctx, "LOAD httpfs;"); err != nil {
				zap.S().Warnw("duckdb: load httpfs failed", "err", err)
			}
		} else {
			zap.S().Warnw("duckdb: install httpfs failed", "err", err)
		}

		// Set S3 PRAGMA values if provided
		if cfg.S3AccessKey != "" {
			if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA s3_access_key='%s';", cfg.S3AccessKey)); err != nil {
				zap.S().Warnw("duckdb: set s3_access_key failed", "err", err)
			}
		}
		if cfg.S3SecretKey != "" {
			if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA s3_secret_key='%s';", cfg.S3SecretKey)); err != nil {
				zap.S().Warnw("duckdb: set s3_secret_key failed", "err", err)
			}
		}
		if cfg.S3Region != "" {
			if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA s3_region='%s';", cfg.S3Region)); err != nil {
				zap.S().Warnw("duckdb: set s3_region failed", "err", err)
			}
		}
		if cfg.S3Endpoint != "" {
			if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA s3_endpoint='%s';", cfg.S3Endpoint)); err != nil {
				zap.S().Warnw("duckdb: set s3_endpoint failed", "err", err)
			}
		}
	}

	// Parquet extension
	if cfg.EnableParquet {
		if _, err := db.ExecContext(ctx, "INSTALL parquet;"); err == nil {
			if _, err := db.ExecContext(ctx, "LOAD parquet;"); err != nil {
				zap.S().Warnw("duckdb: load parquet failed", "err", err)
			}
		} else {
			zap.S().Warnw("duckdb: install parquet failed", "err", err)
		}
	}

	// Apply resource pragmas if configured
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

	client := &DuckDBClient{
		DB:  db,
		cfg: cfg,
	}
	return client, nil
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

// SetDuckDBClient stores a global client for other packages to access during initial integration.
// Prefer explicit dependency injection for long-term design; this is a pragmatic first step.
func SetDuckDBClient(c *DuckDBClient) {
	globalDuckDBClient = c
}

// GetDuckDBClient returns the global client if set.
func GetDuckDBClient() *DuckDBClient {
	return globalDuckDBClient
}
