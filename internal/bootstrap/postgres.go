package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
)

func NewPostgresPoolFromConfigContext(ctx context.Context, config forma.DatabaseConfig) (*pgxpool.Pool, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("postgres bootstrap context: %w", err)
	}

	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		config.Username,
		config.Password,
		config.Host,
		config.Port,
		config.Database,
		config.SSLMode,
	)

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	poolConfig.MaxConns = int32(config.MaxConnections)
	poolConfig.MinConns = int32(config.MaxIdleConns)
	poolConfig.MaxConnLifetime = config.ConnMaxLifetime
	poolConfig.MaxConnIdleTime = config.ConnMaxIdleTime
	poolConfig.ConnConfig.ConnectTimeout = config.Timeout

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
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("failed to ping database: %w", err)
		}
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return pool, nil
}

func NewPostgresPoolFromConfig(config forma.DatabaseConfig) (*pgxpool.Pool, error) {
	return NewPostgresPoolFromConfigContext(context.Background(), config)
}
