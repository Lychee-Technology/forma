package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/factory"
)

func NewEntityManager(config *forma.Config) forma.EntityManager {
	pool, err := createDatabasePool(config)
	if err != nil {
		panic(fmt.Sprintf("failed to create database pool: %v", err))
	}

	em, err := factory.NewEntityManagerWithConfig(config, pool)
	if err != nil {
		panic(fmt.Sprintf("failed to create entity manager: %v", err))
	}
	return em
}

// createDatabasePool creates a PostgreSQL connection pool
func createDatabasePool(config *forma.Config) (*pgxpool.Pool, error) {
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		config.Database.Username,
		config.Database.Password,
		config.Database.Host,
		config.Database.Port,
		config.Database.Database,
		config.Database.SSLMode,
	)

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	poolConfig.MaxConns = int32(config.Database.MaxConnections)
	poolConfig.MinConns = int32(config.Database.MaxIdleConns)
	poolConfig.MaxConnLifetime = config.Database.ConnMaxLifetime
	poolConfig.MaxConnIdleTime = config.Database.ConnMaxIdleTime
	poolConfig.ConnConfig.ConnectTimeout = config.Database.Timeout

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return pool, nil
}
