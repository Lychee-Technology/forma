package bootstrap

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
)

// buildPoolConfig converts a DatabaseConfig into a pgxpool.Config.
// Extracted so that the pool-field assignments (MaxConns, lifetimes, timeout)
// can be verified in unit tests without requiring a live database connection.
func buildPoolConfig(config forma.DatabaseConfig) (*pgxpool.Config, error) {
	poolConfig, err := pgxpool.ParseConfig(buildDSN(config))
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}
	poolConfig.MaxConns = int32(config.MaxConnections)
	poolConfig.MaxConnLifetime = config.ConnMaxLifetime
	poolConfig.MaxConnIdleTime = config.ConnMaxIdleTime
	poolConfig.ConnConfig.ConnectTimeout = config.Timeout
	return poolConfig, nil
}

func NewPostgresPoolFromConfigContext(ctx context.Context, config forma.DatabaseConfig) (*pgxpool.Pool, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("postgres bootstrap context: %w", err)
	}

	poolConfig, err := buildPoolConfig(config)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	pingTimeout := 5 * time.Second
	if config.Timeout > 0 && config.Timeout < pingTimeout {
		pingTimeout = config.Timeout
	}

	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()

	if err := pingCtx.Err(); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres ping context: %w", err)
	}

	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return pool, nil
}

func NewPostgresPoolFromConfig(config forma.DatabaseConfig) (*pgxpool.Pool, error) {
	return NewPostgresPoolFromConfigContext(context.Background(), config)
}

// buildDSN constructs a postgres:// DSN from DatabaseConfig, correctly
// percent-encoding the username and password so that reserved characters
// (e.g. @, :, /) do not break URL parsing in pgxpool.ParseConfig.
func buildDSN(config forma.DatabaseConfig) string {
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(config.Username, config.Password),
		Host:   fmt.Sprintf("%s:%d", config.Host, config.Port),
		Path:   "/" + config.Database,
	}
	q := u.Query()
	if config.SSLMode != "" {
		q.Set("sslmode", config.SSLMode)
	}
	u.RawQuery = q.Encode()
	return u.String()
}
