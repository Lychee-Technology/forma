package main

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"lychee.technology/ltbase/forma"
	"lychee.technology/ltbase/forma/internal"
)

func NewEntityManager(config *forma.Config) (forma.EntityManager, *internal.MetadataCache) {
	// Create database connection pool
	pool, err := createDatabasePool(config)
	if err != nil {
		panic(fmt.Sprintf("failed to create database pool: %v", err))
	}

	rows, err := pool.Query(context.Background(), `SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
AND table_type = 'BASE TABLE';`)

	if err != nil {
		panic(fmt.Sprintf("failed to verify database connection: %v", err))
	}
	defer rows.Close()

	fmt.Println("Database tables:")
	tables := []string{}
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			panic(fmt.Sprintf("failed to scan table name: %v", err))
		}
		tables = append(tables, tableName)
		fmt.Printf("  - %s\n", tableName)
	}

	if err := rows.Err(); err != nil {
		panic(fmt.Sprintf("error iterating rows: %v", err))
	}

	if len(tables) < 2 || !slices.Contains(tables, config.Database.TableNames.SchemaRegistry) || !slices.Contains(tables, config.Database.TableNames.EAVData) {
		panic("required tables are missing in the database")
	}

	// Load metadata from database at startup
	fmt.Println("\nLoading metadata from database...")
	metadataLoader := internal.NewMetadataLoader(
		pool,
		config.Database.TableNames.SchemaRegistry,
		config.Entity.SchemaDirectory,
	)

	metadataCache, err := metadataLoader.LoadMetadata(context.Background())
	if err != nil {
		panic(fmt.Sprintf("failed to load metadata: %v", err))
	}

	fmt.Printf("Metadata loaded successfully: %d schemas\n\n", len(metadataCache.ListSchemas()))

	// Initialize file-based schema registry (still needed for compatibility)
	registry, err := internal.NewFileSchemaRegistry(config.Entity.SchemaDirectory)
	if err != nil {
		panic(fmt.Sprintf("failed to initialize schema registry: %v", err))
	}

	// Initialize transformer
	transformer := internal.NewTransformer(registry)

	// Initialize PostgreSQL attribute repository with metadata cache
	repository := internal.NewPostgresAttributeRepository(
		pool,
		config.Database.TableNames.EAVData,
		config.Database.TableNames.SchemaRegistry,
		metadataCache,
	)

	// Create and return entity manager
	return internal.NewEntityManager(transformer, repository, registry, config), metadataCache
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
