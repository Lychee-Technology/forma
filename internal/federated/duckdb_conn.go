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
// It loads httpfs and parquet when the config enables them, and fails when
// either cannot be loaded, and configures S3 access via SET.
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
	// A failed optional statement is logged and skipped; a failed required one
	// (httpfs, parquet, the S3 session settings) fails the connection, and
	// thereby the ping and construction (#752). The factory treats a client
	// that fails to open as DuckDB unavailable and degrades to Postgres-only.
	// The recovering connector reopens the database with the same hook when
	// DuckDB invalidates it (#702).
	connInit := duckdbinit.MakeConnInit(steps, zap.S(), duckdbinit.DefaultInitTimeout)
	connector, err := duckdbinit.NewRecoveringConnector(func() (driver.Connector, error) {
		return duckdb.NewConnector(dsn, connInit)
	}, zap.S())
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
// failed INSTALL skips that extension's LOAD. httpfs (S3) and parquet are
// required when the config enables them: a connection without them fails
// every federated read, so it must fail to open instead of passing the
// constructor's ping and degrading each query (#752). Extensions listed in
// cfg.Extensions stay optional, logged and skipped as before.
func buildExtensionSteps(cfg forma.DuckDBConfig) []duckdbinit.Step {
	var steps []duckdbinit.Step
	for _, ext := range cfg.Extensions {
		steps = append(steps, duckdbinit.ExtensionStep(ext))
	}
	if cfg.EnableS3 {
		steps = append(steps, duckdbinit.RequiredExtensionStep("httpfs"))
	}
	if cfg.EnableParquet {
		steps = append(steps, duckdbinit.RequiredExtensionStep("parquet"))
	}
	return steps
}

// buildS3Steps builds the S3 session SET statements. Each credential passes the
// character-denylist validation first, so invalid values fail construction. The
// SET statements are independent of each other, hence one step apiece. They are
// required: a connection that loaded httpfs but missed its endpoint or keys
// would sign S3 requests for the wrong store (#245, #752).
func buildS3Steps(cfg forma.DuckDBConfig) ([]duckdbinit.Step, error) {
	if !cfg.EnableS3 {
		return nil, nil
	}

	type s3Setting struct {
		name, value string
		secret      bool
	}
	var steps []duckdbinit.Step
	settings := []s3Setting{
		{"s3_access_key_id", cfg.S3AccessKey, true},
		{"s3_secret_access_key", cfg.S3SecretKey, true},
		{"s3_region", cfg.S3Region, false},
	}
	if cfg.S3Endpoint != "" {
		settings = append(settings, s3Setting{"s3_endpoint", strings.TrimPrefix(cfg.S3Endpoint, "http://"), false})
	}
	for _, c := range settings {
		if c.value == "" {
			continue
		}
		step, err := duckdbinit.S3SettingStep(c.name, c.value, c.secret)
		if err != nil {
			return nil, fmt.Errorf("invalid duckdb s3 config: %w", err)
		}
		steps = append(steps, step.AsRequired())
	}
	if cfg.S3Endpoint != "" {
		steps = append(steps,
			duckdbinit.SingleStmtStep("SET s3_use_ssl=false;", "set s3_use_ssl").AsRequired(),
			duckdbinit.SingleStmtStep("SET s3_url_style='path';", "set s3_url_style").AsRequired(),
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
