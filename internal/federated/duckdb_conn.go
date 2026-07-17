package federated

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
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

	// Pre-build the init statements; S3 credential validation fails fast here,
	// before any connection is opened.
	stmts, err := buildInitStatements(cfg)
	if err != nil {
		return nil, fmt.Errorf("build duckdb init statements: %w", err)
	}

	// LOAD/SET/PRAGMA are session-scoped, so they must run on every pooled
	// connection — not once against the pool, which reaches a single arbitrary
	// connection (issue #245). The connector init hook runs for each new physical
	// connection; PingContext below opens and thereby initializes the first one.
	connector, err := duckdb.NewConnector(dsn, makeConnInit(stmts))
	if err != nil {
		return nil, fmt.Errorf("open duckdb connector: %w", err)
	}
	db := sql.OpenDB(connector)

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

	return &DuckDBClient{DB: db, cfg: cfg}, nil
}

// initStmt is a single statement executed on every new physical DuckDB connection.
type initStmt struct {
	sql   string
	label string
}

// buildInitStatements assembles the INSTALL/LOAD/SET/PRAGMA statements every pooled
// connection must run on open: user extensions, httpfs (when S3 is enabled), parquet
// (when parquet is enabled), S3 session settings, and resource pragmas.
func buildInitStatements(cfg forma.DuckDBConfig) ([]initStmt, error) {
	stmts := extensionStmts(cfg)
	s3, err := s3Stmts(cfg)
	if err != nil {
		return nil, fmt.Errorf("build duckdb s3 statements: %w", err)
	}
	stmts = append(stmts, s3...)
	stmts = append(stmts, pragmaStmts(cfg)...)
	return stmts, nil
}

func extensionStmts(cfg forma.DuckDBConfig) []initStmt {
	var stmts []initStmt
	for _, ext := range cfg.Extensions {
		stmts = append(stmts,
			initStmt{sql: fmt.Sprintf("INSTALL %s;", ext), label: "install " + ext},
			initStmt{sql: fmt.Sprintf("LOAD %s;", ext), label: "load " + ext},
		)
	}
	if cfg.EnableS3 {
		stmts = append(stmts,
			initStmt{sql: "INSTALL httpfs;", label: "install httpfs"},
			initStmt{sql: "LOAD httpfs;", label: "load httpfs"},
		)
	}
	if cfg.EnableParquet {
		stmts = append(stmts,
			initStmt{sql: "INSTALL parquet;", label: "install parquet"},
			initStmt{sql: "LOAD parquet;", label: "load parquet"},
		)
	}
	return stmts
}

// s3Stmts builds the S3 session SET statements. Each credential passes the
// character-denylist validation first, so invalid values fail construction.
func s3Stmts(cfg forma.DuckDBConfig) ([]initStmt, error) {
	if !cfg.EnableS3 {
		return nil, nil
	}

	var stmts []initStmt
	if cfg.S3AccessKey != "" {
		if err := validateS3Credential("s3_access_key_id", cfg.S3AccessKey); err != nil {
			return nil, fmt.Errorf("invalid duckdb s3 config: %w", err)
		}
		stmts = append(stmts, initStmt{sql: fmt.Sprintf("SET s3_access_key_id='%s';", cfg.S3AccessKey), label: "set s3_access_key_id"})
	}
	if cfg.S3SecretKey != "" {
		if err := validateS3Credential("s3_secret_access_key", cfg.S3SecretKey); err != nil {
			return nil, fmt.Errorf("invalid duckdb s3 config: %w", err)
		}
		stmts = append(stmts, initStmt{sql: fmt.Sprintf("SET s3_secret_access_key='%s';", cfg.S3SecretKey), label: "set s3_secret_access_key"})
	}
	if cfg.S3Region != "" {
		if err := validateS3Credential("s3_region", cfg.S3Region); err != nil {
			return nil, fmt.Errorf("invalid duckdb s3 config: %w", err)
		}
		stmts = append(stmts, initStmt{sql: fmt.Sprintf("SET s3_region='%s';", cfg.S3Region), label: "set s3_region"})
	}
	if cfg.S3Endpoint != "" {
		endpoint := strings.TrimPrefix(cfg.S3Endpoint, "http://")
		if err := validateS3Credential("s3_endpoint", endpoint); err != nil {
			return nil, fmt.Errorf("invalid duckdb s3 config: %w", err)
		}
		stmts = append(stmts,
			initStmt{sql: fmt.Sprintf("SET s3_endpoint='%s';", endpoint), label: "set s3_endpoint"},
			initStmt{sql: "SET s3_use_ssl=false;", label: "set s3_use_ssl"},
			initStmt{sql: "SET s3_url_style='path';", label: "set s3_url_style"},
		)
	}
	return stmts, nil
}

func pragmaStmts(cfg forma.DuckDBConfig) []initStmt {
	var stmts []initStmt
	if cfg.MemoryLimitMB > 0 {
		stmts = append(stmts, initStmt{sql: fmt.Sprintf("PRAGMA memory_limit='%dMB';", cfg.MemoryLimitMB), label: "set memory_limit"})
	}
	if cfg.MaxParallelism > 0 {
		stmts = append(stmts, initStmt{sql: fmt.Sprintf("PRAGMA threads=%d;", cfg.MaxParallelism), label: "set threads"})
	}
	return stmts
}

// makeConnInit returns the connector init hook the driver runs for every new physical
// connection. Failed statements are logged and skipped so a degraded init never blocks
// the connection — the same policy the pool-level configuration used before.
func makeConnInit(stmts []initStmt) func(driver.ExecerContext) error {
	return func(execer driver.ExecerContext) error {
		for _, s := range stmts {
			// The driver binds the Connect context to the connection while this hook
			// runs; the statements are literal SQL, so no driver.NamedValue args.
			if _, err := execer.ExecContext(context.Background(), s.sql, nil); err != nil {
				zap.S().Warnw("duckdb: connection init step failed", "step", s.label, "err", err)
			}
		}
		return nil
	}
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
