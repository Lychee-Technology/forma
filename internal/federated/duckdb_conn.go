package federated

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/duckdbinit"
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
	steps, err := buildInitSteps(cfg)
	if err != nil {
		return nil, fmt.Errorf("build duckdb init statements: %w", err)
	}

	// LOAD/SET/PRAGMA are session-scoped, so they must run on every pooled
	// connection — not once against the pool, which reaches a single arbitrary
	// connection (issue #245). The connector init hook runs for each new physical
	// connection; PingContext below opens and thereby initializes the first one.
	// Failed init statements are logged and skipped, never blocking the
	// connection — construction fails only on credential validation or ping.
	connector, err := duckdb.NewConnector(dsn, duckdbinit.MakeConnInit(steps, zap.S()))
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

// buildInitSteps assembles the INSTALL/LOAD/SET/PRAGMA steps every pooled
// connection must run on open: user extensions, httpfs (when S3 is enabled),
// parquet (when parquet is enabled), S3 session settings, and resource pragmas.
func buildInitSteps(cfg forma.DuckDBConfig) ([]duckdbinit.Step, error) {
	steps := buildExtensionSteps(cfg)
	s3, err := buildS3Steps(cfg)
	if err != nil {
		return nil, fmt.Errorf("build duckdb s3 statements: %w", err)
	}
	steps = append(steps, s3...)
	steps = append(steps, buildPragmaSteps(cfg)...)
	return steps, nil
}

// buildExtensionSteps pairs each extension's INSTALL and LOAD into one step, so a
// failed INSTALL skips that extension's LOAD.
func buildExtensionSteps(cfg forma.DuckDBConfig) []duckdbinit.Step {
	var steps []duckdbinit.Step
	appendExt := func(ext string) {
		steps = append(steps, duckdbinit.ExtensionStep(ext))
	}
	for _, ext := range cfg.Extensions {
		appendExt(ext)
	}
	if cfg.EnableS3 {
		appendExt("httpfs")
	}
	if cfg.EnableParquet {
		appendExt("parquet")
	}
	return steps
}

// buildS3Steps builds the S3 session SET statements. Each credential passes the
// character-denylist validation first, so invalid values fail construction. The
// SET statements are independent of each other, hence one step apiece.
func buildS3Steps(cfg forma.DuckDBConfig) ([]duckdbinit.Step, error) {
	if !cfg.EnableS3 {
		return nil, nil
	}

	var steps []duckdbinit.Step
	credentials := []struct{ name, value string }{
		{"s3_access_key_id", cfg.S3AccessKey},
		{"s3_secret_access_key", cfg.S3SecretKey},
		{"s3_region", cfg.S3Region},
	}
	for _, c := range credentials {
		if c.value == "" {
			continue
		}
		if err := duckdbinit.ValidateS3Credential(c.name, c.value); err != nil {
			return nil, fmt.Errorf("invalid duckdb s3 config: %w", err)
		}
		steps = append(steps, duckdbinit.SingleStmtStep(fmt.Sprintf("SET %s='%s';", c.name, c.value), "set "+c.name))
	}
	if cfg.S3Endpoint != "" {
		endpoint := strings.TrimPrefix(cfg.S3Endpoint, "http://")
		if err := duckdbinit.ValidateS3Credential("s3_endpoint", endpoint); err != nil {
			return nil, fmt.Errorf("invalid duckdb s3 config: %w", err)
		}
		steps = append(steps,
			duckdbinit.SingleStmtStep(fmt.Sprintf("SET s3_endpoint='%s';", endpoint), "set s3_endpoint"),
			duckdbinit.SingleStmtStep("SET s3_use_ssl=false;", "set s3_use_ssl"),
			duckdbinit.SingleStmtStep("SET s3_url_style='path';", "set s3_url_style"),
		)
	}
	return steps, nil
}

func buildPragmaSteps(cfg forma.DuckDBConfig) []duckdbinit.Step {
	var steps []duckdbinit.Step
	if cfg.MemoryLimitMB > 0 {
		steps = append(steps, duckdbinit.SingleStmtStep(fmt.Sprintf("PRAGMA memory_limit='%dMB';", cfg.MemoryLimitMB), "set memory_limit"))
	}
	if cfg.MaxParallelism > 0 {
		steps = append(steps, duckdbinit.SingleStmtStep(fmt.Sprintf("PRAGMA threads=%d;", cfg.MaxParallelism), "set threads"))
	}
	return steps
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
