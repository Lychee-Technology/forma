package factory

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/queryplan"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"

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
	LoadMetadata(ctx context.Context) (*schemameta.MetadataCache, error)
}

// entityManagerDependencies carries factory seams for EntityManager construction.
// Only add fields that are strictly construction dependencies of the factory.
// Business-logic seams belong on the EntityManager interface, not here.
type entityManagerDependencies struct {
	collectTables     func(context.Context, queryPool, string) ([]string, error)
	newMetadataLoader func(*pgxpool.Pool, string, string) metadataLoader
	newDuckDBClient   func(context.Context, forma.DuckDBConfig) (*federated.DuckDBClient, error)
	newParquetSource  func(context.Context, forma.DuckDBConfig) (federated.ParquetSource, error)
}

func defaultEntityManagerDependencies() entityManagerDependencies {
	return entityManagerDependencies{
		collectTables: collectTablesFromPool,
		newMetadataLoader: func(pool *pgxpool.Pool, schemaTable, schemaDir string) metadataLoader {
			return schemameta.NewMetadataLoader(pool, schemaTable, schemaDir)
		},
		newDuckDBClient: federated.NewDuckDBClientContext,
		// A factory test that enables DuckDB + a manifest template and leaves
		// this default in place hits the real AWS credential chain — call
		// swapManifestS3Client (parquet_source_test.go) to stay hermetic.
		newParquetSource: newManifestParquetSource,
	}
}

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
	return newEntityManagerWithConfigContext(ctx, config, pool, defaultEntityManagerDependencies())
}

func newEntityManagerWithConfigContext(ctx context.Context, config *forma.Config, pool *pgxpool.Pool, deps entityManagerDependencies) (forma.EntityManager, error) {
	schemaName := normalizeSchemaName(config.Database.Schema)
	effectiveConfig := configWithQualifiedTables(config, schemaName)
	// Configuration errors are rejected before any I/O: an incoherent manifest
	// read surface must not be discovered halfway through startup, after the
	// database has already been queried.
	if err := effectiveConfig.DuckDB.ValidateManifestRead(); err != nil {
		return nil, fmt.Errorf("invalid duckdb manifest configuration: %w", err)
	}
	tables, err := deps.collectTables(ctx, pool, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to collect tables for schema %s: %w", schemaName, err)
	}

	if err := requireCoreTables(effectiveConfig, tables); err != nil {
		return nil, fmt.Errorf("core table check failed: %w", err)
	}

	// Load metadata from database at startup
	zap.S().Info("Loading metadata from database...")
	loader := deps.newMetadataLoader(
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
	transformer := transform.NewPersistentRecordTransformer(registry)

	// Resolve every registered schema before opening any read surface. This fails
	// closed, so a schema that cannot resolve aborts startup rather than silently
	// losing validation at runtime (#314).
	//
	// The position is load-bearing: nothing above owns a closable resource, so this
	// failure return has nothing to leak. Queries have already run (collectTables,
	// LoadMetadata) — this is pre-read-surface, not pre-I/O. Never move a step that
	// opens a client, pool, or handle above this line.
	schemaValidator, err := schemavalidate.New(registry, effectiveConfig.Entity.SchemaDirectory)
	if err != nil {
		return nil, fmt.Errorf("failed to build schema validator: %w", err)
	}

	// Relation declarations are checked at the same fail-closed position and for
	// the same reason: a schema that requires a relation root makes its entity
	// unwritable at runtime, because the root is stripped from every payload
	// before the validator sees it (#318). NewEntityManager cannot enforce this —
	// it swallows a relation-index load failure into a warning and continues with
	// stripping disabled — so it has to happen here, where startup fails.
	//
	// This step opens no client, pool, or handle, so it respects the placement
	// rule stated above and has nothing to leak on its error return.
	//
	// It reads the registry, not SCHEMA_DIR, so it analyses the same documents
	// schemavalidate.New just resolved. Handing it the directory instead would let
	// a registry that does not serve the files on disk boot with an index built
	// from one document and a validator built from another.
	if err := internal.ValidateRelationSchemas(registry); err != nil {
		return nil, fmt.Errorf("failed to validate schema relations: %w", err)
	}

	duckClient, parquetSource, err := newFederatedReadSurface(ctx, effectiveConfig.DuckDB, deps)
	if err != nil {
		return nil, fmt.Errorf("failed to open the federated read surface: %w", err)
	}

	repository, federatedEngine := newRepositoryAndEngine(
		pool, metadataCache, effectiveConfig, duckClient, parquetSource)
	// The DuckDB client has no owner other than the manager being built:
	// register it so EntityManager.Close releases it (#302). Guarded on nil
	// because a typed-nil *DuckDBClient boxed into io.Closer would defeat
	// WithCloser's nil check.
	var managerOpts []internal.EntityManagerOption
	if duckClient != nil {
		managerOpts = append(managerOpts, internal.WithCloser(duckClient))
	}
	// Create and return entity manager
	return internal.NewEntityManager(
		transformer, repository, federatedEngine, registry, effectiveConfig, schemaValidator, managerOpts...), nil
}

// requireCoreTables fails closed when any of the three tables the manager
// cannot operate without is absent from the resolved schema.
func requireCoreTables(cfg *forma.Config, tables []string) error {
	for _, required := range []string{
		normalizeTableName(cfg.Database.TableNames.SchemaRegistry),
		normalizeTableName(cfg.Database.TableNames.EAVData),
		normalizeTableName(cfg.Database.TableNames.EntityMain),
	} {
		if required == "" || !slices.Contains(tables, required) {
			return fmt.Errorf("required tables are missing in the database")
		}
	}
	return nil
}

// newRepositoryAndEngine builds the OLTP repository and the federated engine
// as a pair, because they share one plan cache (#142): its lifetime is the
// manager's, so compiled plans survive across requests.
func newRepositoryAndEngine(
	pool *pgxpool.Pool, metadataCache *schemameta.MetadataCache, cfg *forma.Config,
	duckClient *federated.DuckDBClient, parquetSource federated.ParquetSource,
) (*internal.DBPersistentRecordRepository, *federated.DBFederatedQueryEngine) {
	planCache := queryplan.NewCache(4096)
	repository := internal.NewDBPersistentRecordRepository(pool, metadataCache, internal.WithPlanCache(planCache))
	// zap.L() is the same global the rest of this package logs through. It
	// carries the federated validator's #256 stamp/footer cross-check, whose
	// only surface is a log line: the read it observes succeeds, so no caller
	// error and no execution plan would ever mention it.
	engineOpts := []federated.EngineOption{
		federated.WithPlanCache(planCache),
		federated.WithLogger(zap.L()),
	}
	// Append only a real source, so the manifest-off path stays byte-identical
	// to the pre-#250 engine construction.
	if parquetSource != nil {
		engineOpts = append(engineOpts, federated.WithParquetSource(parquetSource))
	}
	return repository, federated.NewDBFederatedQueryEngine(
		repository,
		federated.NewPostgresDirtyIDFetcher(pool),
		federated.NewDuckDBClientQueryExecutor(duckClient),
		newDuckDBCircuitBreaker(cfg.DuckDB),
		cfg.DuckDB,
		metadataCache,
		federated.DuckDBPostgresConnStringFromPool(pool),
		engineOpts...,
	)
}

// newFederatedReadSurface opens the DuckDB client and the manifest parquet source
// together, because their failure modes are coupled and only make sense as a pair.
//
// A DuckDB client that fails to open is non-fatal: the engine degrades to
// Postgres-only. A parquet source that fails to build is fatal, because it would
// silently drop the cold tier. On that fatal path the already-open DuckDB client
// has no other owner, so its handle and pool are released here rather than leaked
// for the process lifetime. (*DuckDBClient).Close is nil-receiver safe, covering
// the DuckDB-off and client-construction-failed cases.
func newFederatedReadSurface(
	ctx context.Context,
	cfg forma.DuckDBConfig,
	deps entityManagerDependencies,
) (*federated.DuckDBClient, federated.ParquetSource, error) {
	var duckClient *federated.DuckDBClient = nil
	var err error

	if cfg.Enabled {
		zap.S().Infow("initializing DuckDB client", "dbPath", cfg.DBPath)
		duckClient, err = deps.newDuckDBClient(ctx, cfg)
		if err != nil {
			zap.S().Warnw("failed to initialize DuckDB client; continuing without DuckDB", "err", err)
		} else {
			zap.S().Infow("duckdb client initialized")
		}
	}

	parquetSource, err := deps.newParquetSource(ctx, cfg)
	if err != nil {
		if closeErr := duckClient.Close(); closeErr != nil {
			zap.S().Warnw("failed to close duckdb client after parquet source failure", "err", closeErr)
		}
		return nil, nil, fmt.Errorf("failed to build manifest parquet source: %w", err)
	}
	return duckClient, parquetSource, nil
}

func newDuckDBCircuitBreaker(cfg forma.DuckDBConfig) *federated.CircuitBreaker {
	// This is the one sanctioned reader of the deprecated field: it exists to
	// warn callers who still set it.
	if cfg.CircuitBreakerThreshold > 0 { //nolint:staticcheck // SA1019: intentional migration-warning read
		zap.S().Warnw("circuitBreakerThreshold is deprecated and ignored; use circuitBreakerFailureThreshold instead", "oldValue", cfg.CircuitBreakerThreshold) //nolint:staticcheck // SA1019: intentional migration-warning read
	}

	defaults := forma.DefaultConfig(nil).DuckDB
	failureThreshold := cfg.CircuitBreakerFailureThreshold
	if failureThreshold <= 0 {
		failureThreshold = defaults.CircuitBreakerFailureThreshold
	}
	window := cfg.CircuitBreakerWindow
	if window <= 0 {
		window = defaults.CircuitBreakerWindow
	}
	openDuration := cfg.CircuitBreakerOpenDuration
	if openDuration <= 0 {
		openDuration = defaults.CircuitBreakerOpenDuration
	}

	return federated.NewCircuitBreaker(failureThreshold, window, openDuration)
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
	return schemameta.NewFileSchemaRegistryContext(ctx, pool, schemaTable, schemaDir)
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
