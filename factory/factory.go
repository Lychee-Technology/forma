package factory

import (
	"context"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"go.uber.org/zap"
)

// NewEntityManagerWithConfig creates a new EntityManager with the provided configuration and database pool.
// This is the primary way for external projects to create an EntityManager instance.
//
// If config.SchemaRegistry is provided, it will be used instead of creating a file-based registry.
// This allows callers to provide their own SchemaRegistry implementation.
//
// Usage:
//
//	import (
//	    "github.com/lychee-technology/forma"
//	    "github.com/lychee-technology/forma/factory"
//	)
//
//	config := forma.DefaultConfig()
//	em, err := factory.NewEntityManagerWithConfig(config, pool)
//	if err != nil {
//	    // handle error
//	}
//
// With custom SchemaRegistry:
//
//	config := forma.DefaultConfig()
//	config.SchemaRegistry = myCustomRegistry
//	em, err := factory.NewEntityManagerWithConfig(config, pool)
func NewEntityManagerWithConfig(config *forma.Config, pool *pgxpool.Pool) (forma.EntityManager, error) {
	rows, err := pool.Query(context.Background(), `SELECT table_name FROM information_schema.tables t
		WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
		union SELECT table_name FROM information_schema.views v WHERE table_schema = 'public';`)

	if err != nil {
		return nil, fmt.Errorf("failed to verify database connection: %w", err)
	}
	defer rows.Close()

	zap.S().Info("Database tables:")
	tables := []string{}
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
		zap.S().Infow("found table", "name", tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	if len(tables) < 2 || !slices.Contains(tables, config.Database.TableNames.SchemaRegistry) || !slices.Contains(tables, config.Database.TableNames.EAVData) {
		return nil, fmt.Errorf("required tables are missing in the database")
	}

	// Load metadata from database at startup
	zap.S().Info("Loading metadata from database...")
	metadataLoader := internal.NewMetadataLoader(
		pool,
		config.Database.TableNames.SchemaRegistry,
		config.Entity.SchemaDirectory,
	)

	metadataCache, err := metadataLoader.LoadMetadata(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to load metadata: %w", err)
	}

	zap.S().Infow("Metadata loaded successfully", "schemaCount", len(metadataCache.ListSchemas()))

	// SchemaRegistry must be provided in config
	if config.SchemaRegistry == nil {
		return nil, fmt.Errorf("config.SchemaRegistry is required: please provide a SchemaRegistry implementation")
	}
	registry := config.SchemaRegistry
	zap.S().Info("Using provided SchemaRegistry implementation")

	// Initialize transformer
	transformer := internal.NewPersistentRecordTransformer(registry)

	// Initialize PostgreSQL persistent repository with metadata cache
	repository := internal.NewPostgresPersistentRecordRepository(
		pool,
		metadataCache,
	)

	// Create and return entity manager
	return internal.NewEntityManager(transformer, repository, registry, config), nil
}

func NewFileSchemaRegistry(pool *pgxpool.Pool, schemaTable string, schemaDir string) (forma.SchemaRegistry, error) {
	return internal.NewFileSchemaRegistry(pool, schemaTable, schemaDir)
}
