package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
)

// ValidatePostgresConfig performs basic sanity checks on Postgres-related settings.
func ValidatePostgresConfig(cfg forma.DatabaseConfig) error {
	if cfg.Host == "" {
		return fmt.Errorf("database.host is required")
	}
	if cfg.Port <= 0 || cfg.Port > 65535 {
		return fmt.Errorf("database.port must be a valid TCP port")
	}
	if cfg.MaxConnections <= 0 {
		return fmt.Errorf("database.maxConnections must be greater than 0")
	}
	// Timeout may be zero (use defaults elsewhere), no strict check here.
	return nil
}

// PostgresHealthCheck attempts to connect and ping a Postgres instance using a DSN.
// timeout may be 0 to use a sensible default (5s).
func PostgresHealthCheck(ctx context.Context, dsn string, timeout time.Duration) error {
	if dsn == "" {
		return fmt.Errorf("empty dsn")
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("parse postgres dsn: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("postgres ping failed: %w", err)
	}

	// Best-effort simple query to validate basic SQL execution.
	if _, err := pool.Exec(ctx, "SELECT 1"); err != nil {
		return fmt.Errorf("postgres simple query failed: %w", err)
	}

	return nil
}
