package factory

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"go.uber.org/zap"
)

// queryPool is a minimal interface used for querying table names.
// It matches *pgxpool.Pool and pgxmock pools used in tests.
type queryPool interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// metadataLoader is a minimal interface for loading metadata.
// This allows tests to inject mock implementations.
type metadataLoader interface {
	LoadMetadata(ctx context.Context) (*internal.MetadataCache, error)
}

// defaultMetadataLoaderFactory is the default factory function for creating metadata loaders.
// It can be overridden in tests for injection.
var defaultMetadataLoaderFactory = func(pool *pgxpool.Pool, schemaTable, schemaDir string) metadataLoader {
	return internal.NewMetadataLoader(pool, schemaTable, schemaDir)
}

var defaultDuckDBClientFactory = internal.NewDuckDBClientContext

// tableCollector is a test hook for table discovery.
var tableCollector = collectTablesFromPool

const defaultDatabaseSchema = "public"

// collectTablesFromPool queries information_schema for table/view names and returns the list.
func collectTablesFromPool(ctx context.Context, pool queryPool, schema string) ([]string, error) {
	inspectionSchema := normalizeSchemaName(schema)
	rows, err := pool.Query(ctx, `SELECT table_name FROM information_schema.tables t
WHERE table_schema = $1 AND table_type = 'BASE TABLE'
UNION SELECT table_name FROM information_schema.views v WHERE table_schema = $1;`, inspectionSchema)

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
	return tables, nil
}

// NewEntityManagerWithConfigContext creates a new EntityManager using the provided
// context, configuration, and database pool. It is the canonical implementation;
// use this when you need to control the context used during initialisation (e.g.
// to propagate cancellation or deadlines to the startup queries).
//
// See NewEntityManagerWithConfig for usage examples.
func NewEntityManagerWithConfigContext(ctx context.Context, config *forma.Config, pool *pgxpool.Pool) (forma.EntityManager, error) {
	schemaName := normalizeSchemaName(config.Database.Schema)
	effectiveConfig := configWithQualifiedTables(config, schemaName)
	tables, err := tableCollector(ctx, pool, schemaName)
	if err != nil {
		return nil, err
	}

	requiredTables := []string{
		normalizeTableName(effectiveConfig.Database.TableNames.SchemaRegistry),
		normalizeTableName(effectiveConfig.Database.TableNames.EAVData),
		normalizeTableName(effectiveConfig.Database.TableNames.EntityMain),
	}
	for _, required := range requiredTables {
		if required == "" || !slices.Contains(tables, required) {
			return nil, fmt.Errorf("required tables are missing in the database")
		}
	}

	// Load metadata from database at startup
	zap.S().Info("Loading metadata from database...")
	loader := defaultMetadataLoaderFactory(
		pool,
		effectiveConfig.Database.TableNames.SchemaRegistry,
		effectiveConfig.Entity.SchemaDirectory,
	)

	metadataCache, err := loader.LoadMetadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load metadata: %w", err)
	}

	zap.S().Infow("Metadata loaded successfully", "schemaCount", len(metadataCache.ListSchemas()))

	// SchemaRegistry must be provided in config
	if effectiveConfig.SchemaRegistry == nil {
		return nil, fmt.Errorf("config.SchemaRegistry is required: please provide a SchemaRegistry implementation")
	}
	registry := effectiveConfig.SchemaRegistry
	zap.S().Info("Using provided SchemaRegistry implementation")

	// Initialize transformer
	transformer := internal.NewPersistentRecordTransformer(registry)

	// Initialize PostgreSQL persistent repository with metadata cache
	var duckClient *internal.DuckDBClient = nil

	// Initialize DuckDB client if enabled in config
	if effectiveConfig.DuckDB.Enabled {
		zap.S().Infow("initializing DuckDB client", "dbPath", effectiveConfig.DuckDB.DBPath)
		duckClient, err = defaultDuckDBClientFactory(ctx, effectiveConfig.DuckDB)
		if err != nil {
			zap.S().Warnw("failed to initialize DuckDB client; continuing without DuckDB", "err", err)
		} else {
			zap.S().Infow("duckdb client initialized")
		}
	}
	repository := internal.NewDBPersistentRecordRepository(pool, metadataCache)
	federatedEngine := internal.NewDBFederatedQueryEngine(
		repository,
		internal.NewPostgresDirtyIDFetcher(pool),
		internal.NewDuckDBClientQueryExecutor(duckClient),
		internal.NewCircuitBreaker(5, time.Minute, time.Minute),
		effectiveConfig.DuckDB,
		metadataCache,
		internal.DuckDBPostgresConnStringFromPool(pool),
	)
	// Create and return entity manager
	return internal.NewEntityManager(transformer, repository, federatedEngine, registry, effectiveConfig), nil
}

// NewEntityManagerWithConfig creates a new EntityManager with the provided configuration and database pool.
// This is the primary way for external projects to create an EntityManager instance.
//
// config.SchemaRegistry must already be initialized before calling this
// function. The factory validates database state and builds the entity manager
// around the provided registry; it does not create a fallback registry.
//
// Usage:
//
// import (
//
//	"github.com/lychee-technology/forma"
//	"github.com/lychee-technology/forma/factory"
//
// )
//
// config := forma.DefaultConfig(registry)
// em, err := factory.NewEntityManagerWithConfig(config, pool)
//
//	if err != nil {
//	   // handle error
//	}
//
// With custom SchemaRegistry:
//
// config := forma.DefaultConfig(registry)
// config.SchemaRegistry = myCustomRegistry
// em, err := factory.NewEntityManagerWithConfig(config, pool)
func NewEntityManagerWithConfig(config *forma.Config, pool *pgxpool.Pool) (forma.EntityManager, error) {
	return NewEntityManagerWithConfigContext(context.Background(), config, pool)
}

func NewFileSchemaRegistryContext(ctx context.Context, pool *pgxpool.Pool, schemaTable string, schemaDir string) (forma.SchemaRegistry, error) {
	return internal.NewFileSchemaRegistryContext(ctx, pool, schemaTable, schemaDir)
}

func NewFileSchemaRegistry(pool *pgxpool.Pool, schemaTable string, schemaDir string) (forma.SchemaRegistry, error) {
	return NewFileSchemaRegistryContext(context.Background(), pool, schemaTable, schemaDir)
}

func normalizeSchemaName(schema string) string {
	name := strings.TrimSpace(schema)
	if name == "" {
		return defaultDatabaseSchema
	}
	return name
}

func normalizeTableName(name string) string {
	if name == "" {
		return ""
	}
	parts := strings.Split(name, ".")
	for i := len(parts) - 1; i >= 0; i-- {
		trimmed := strings.Trim(parts[i], ` "`)
		if trimmed != "" {
			return trimmed
		}
	}
	return strings.Trim(name, ` "`)
}

func configWithQualifiedTables(config *forma.Config, schema string) *forma.Config {
	cloned := *config
	cloned.Database = config.Database
	cloned.Database.TableNames = qualifyTableNames(schema, config.Database.TableNames)
	return &cloned
}

func qualifyTableNames(schema string, tables forma.TableNames) forma.TableNames {
	return forma.TableNames{
		SchemaRegistry: qualifyTableName(schema, tables.SchemaRegistry),
		EntityMain:     qualifyTableName(schema, tables.EntityMain),
		EAVData:        qualifyTableName(schema, tables.EAVData),
		ChangeLog:      qualifyTableName(schema, tables.ChangeLog),
	}
}

func qualifyTableName(schema, table string) string {
	trimmed := strings.TrimSpace(table)
	if trimmed == "" {
		return ""
	}
	if strings.Contains(trimmed, ".") || schema == "" {
		return trimmed
	}
	return schema + "." + trimmed
}
