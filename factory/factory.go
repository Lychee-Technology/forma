package factory

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/queryplan"
	"github.com/lychee-technology/forma/internal/schemameta"
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
	// newPublicationReader builds the federated reader's publication source
	// (publication_reader.go). It is called only when DuckDB is enabled.
	newPublicationReader func(context.Context, publicationReaderSpec) (federated.PublicationReader, error)
}

func defaultEntityManagerDependencies() entityManagerDependencies {
	return entityManagerDependencies{
		collectTables: collectTablesFromPool,
		newMetadataLoader: func(pool *pgxpool.Pool, schemaTable, schemaDir string) metadataLoader {
			return schemameta.NewMetadataLoader(pool, schemaTable, schemaDir)
		},
		newDuckDBClient: federated.NewDuckDBClientContext,
		// A factory test that enables DuckDB and leaves this default in place
		// pins epochs over its pool and hits the real AWS credential chain:
		// call swapPublicationS3Client (publication_reader_test.go) to stay
		// hermetic.
		newPublicationReader: newPublicationReader,
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
	engine, err := validateBeforeIO(effectiveConfig)
	if err != nil {
		return nil, fmt.Errorf("configuration rejected before connecting: %w", err)
	}
	tables, err := deps.collectTables(ctx, pool, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to collect tables for schema %s: %w", schemaName, err)
	}

	if err := requireCoreTables(effectiveConfig, tables); err != nil {
		return nil, fmt.Errorf("core table check failed: %w", err)
	}
	if err := requireRowGuards(effectiveConfig, tables); err != nil {
		return nil, fmt.Errorf("row guard table check failed: %w", err)
	}

	// Load metadata from database at startup
	zap.S().Info("Loading metadata from database...")
	loader := deps.newMetadataLoader(
		pool,
		effectiveConfig.Database.TableNames.SchemaRegistry,
		effectiveConfig.Entity.SchemaDirectory,
	)

	// The publication epochs are read before the metadata, so the reader
	// pins only the epochs the metadata belongs to (bindEpochPins).
	entityMain := effectiveConfig.Database.TableNames.EntityMain
	metadataCache, metadataEpochs, err := loadMetadataWithEpochs(ctx, loader, effectiveConfig.DuckDB, snapshotPoolOf(pool), entityMain)
	if err != nil {
		return nil, err
	}

	zap.S().Infow("Metadata loaded successfully", "schemaCount", len(metadataCache.ListSchemas()))

	// SchemaRegistry must be provided in config
	if effectiveConfig.SchemaRegistry == nil {
		return nil, fmt.Errorf("config.SchemaRegistry is required: please provide a SchemaRegistry implementation")
	}
	// The manager, the transformer and the guards read the registry, not the
	// loader's cache, so the registry's own caches must hold no attribute named
	// after a reserved column, entity_main, system or dedup (#470): a
	// caller-supplied one never went through admission.
	registry, err := schemameta.GuardReservedAttrNames(effectiveConfig.SchemaRegistry)
	if err != nil {
		return nil, fmt.Errorf("config.SchemaRegistry rejected: %w", err)
	}
	zap.S().Info("Using provided SchemaRegistry implementation")

	// Initialize transformer
	transformer := transform.NewPersistentRecordTransformer(registry)

	guards, err := buildSchemaGuards(registry, effectiveConfig.Entity.SchemaDirectory)
	if err != nil {
		return nil, wrapSchemaGuardError(effectiveConfig.Entity.SchemaDirectory, err)
	}
	if err := requireUniqueKeysBuilt(ctx, pool, effectiveConfig, tables, guards.uniqueKeys); err != nil {
		return nil, fmt.Errorf("failed to verify the schemas' unique keys: %w", err)
	}

	duckClient, reader, err := newFederatedReadSurface(ctx, deps, publicationReaderSpec{
		duck: effectiveConfig.DuckDB, pool: snapshotPoolOf(pool), meta: metadataCache,
		metadataEpochs: metadataEpochs, registryEpochs: effectiveConfig.SchemaRegistryEpochs, entityMain: entityMain,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open the federated read surface: %w", err)
	}

	// The repository enforces the unique keys the check above found built,
	// for as long as it lives (#651).
	repository, federatedEngine := newRepositoryAndEngine(
		pool, metadataCache, effectiveConfig, duckClient, reader, engine, internal.WithUniqueKeyCatalog(guards.uniqueKeys))
	// The DuckDB client has no owner other than the manager being built:
	// register it so EntityManager.Close releases it (#302). Guarded on nil
	// because a typed-nil *DuckDBClient boxed into io.Closer would defeat
	// WithCloser's nil check.
	// The relation index is the one built above, not a fresh load: the manager
	// must strip with exactly the declarations the guard just approved (#318
	// review). Not nil-guarded, because buildSchemaGuards returns a non-nil index
	// on its success path, and WithRelationIndex ignores a nil anyway. The
	// unique keys are the repository's, for the same reason; the manager
	// checks them against nothing else.
	managerOpts := []internal.EntityManagerOption{
		internal.WithRelationIndex(guards.relations), internal.WithUniqueKeys(guards.uniqueKeys),
	}
	if duckClient != nil {
		managerOpts = append(managerOpts, internal.WithCloser(duckClient))
	}
	// Create and return entity manager. Its only failure is a relation-index load,
	// which the index handed in above skips — but the error is wrapped and
	// returned rather than asserted away, so an option or a caller that later
	// stops supplying the index cannot go back to a silent nil index (#388).
	manager, err := internal.NewEntityManager(transformer, repository, internal.WrapFederatedEngine(ctx, federatedEngine),
		registry, effectiveConfig, guards.validator, managerOpts...)
	if err != nil {
		discardDuckClient(duckClient)
		return nil, fmt.Errorf("failed to build the entity manager: %w", err)
	}
	return manager, nil
}

// discardDuckClient releases a DuckDB client that was registered with a manager
// which is never returned.
//
// It exists for the same reason newFederatedReadSurface closes the client on its
// own fatal path: WithCloser made the manager the client's only owner, so on a
// construction failure nothing else will ever call Close, and the handle and its
// pool would be held for the process lifetime. Close is nil-receiver safe
// (federated/duckdb_conn.go), which covers the DuckDB-off case. The failure to
// close is logged rather than returned: the startup error the caller is about to
// receive is the one that explains the abort.
func discardDuckClient(duckClient *federated.DuckDBClient) {
	if err := duckClient.Close(); err != nil {
		zap.S().Warnw("failed to close the duckdb client after entity manager construction failed",
			"error", err)
	}
}

// validateBeforeIO rejects configuration errors before any I/O, and answers
// the repository option of the configured database engine:
//   - a retired or unknown hot-leg transport must fail rather than silently
//     selecting pgx, even when an embedder did not call Config.Validate (#858);
//   - an incoherent parquet read surface (DuckDB on without a bucket) must
//     not be discovered halfway through startup, after the database has
//     already been queried;
//   - the engine decides how writes exclude each other, so an unknown name
//     fails here rather than falling back to PostgreSQL's locks (#633).
func validateBeforeIO(cfg *forma.Config) (internal.RepoOption, error) {
	if err := cfg.DuckDB.ValidateHotLeg(); err != nil {
		return nil, fmt.Errorf("invalid duckdb hot-leg configuration: %w", err)
	}
	if err := cfg.DuckDB.ValidateParquetRead(); err != nil {
		return nil, fmt.Errorf("invalid duckdb parquet read configuration: %w", err)
	}
	engine, err := internal.WithDatabaseEngine(cfg.Database.Engine)
	if err != nil {
		return nil, fmt.Errorf("invalid database configuration: %w", err)
	}
	return engine, nil
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
// manager's, so compiled plans survive across requests. repoOpts are the
// repository's other construction-time collaborators.
func newRepositoryAndEngine(
	pool *pgxpool.Pool, metadataCache *schemameta.MetadataCache, cfg *forma.Config,
	duckClient *federated.DuckDBClient, reader federated.PublicationReader, repoOpts ...internal.RepoOption,
) (*internal.DBPersistentRecordRepository, *federated.DBFederatedQueryEngine) {
	planCache := queryplan.NewCache(4096)
	warnPgxPoolSizing(pool, cfg.DuckDB)
	repository := internal.NewDBPersistentRecordRepository(pool, metadataCache,
		append([]internal.RepoOption{internal.WithPlanCache(planCache)}, repoOpts...)...)
	// zap.L() is the same global the rest of this package logs through. It
	// carries three log-only signals. The federated validator's #256
	// stamp/footer cross-check, whose only surface is a log line: the read it
	// observes succeeds, so no caller error and no execution plan would ever
	// mention it. The #351 guard-violation attribution: under
	// AllowPartialDegradedMode the error naming the offending objects is
	// absorbed into a Postgres-only answer and toExecutionPlan drops plan
	// Notes, so the log line is the only surface that survives. And the #638
	// degraded-fallback line, the only record of the failure such an answer
	// absorbed. It also carries the circuit breaker's transition lines
	// (#634), which pair with the duckdb_circuit_breaker_transition_total
	// metric below.
	// The embedder's telemetry emitter reaches the engine here and the
	// manager through its config (#423); it is per instance, so nothing is
	// registered globally.
	engineOpts := []federated.EngineOption{
		federated.WithPlanCache(planCache),
		federated.WithLogger(zap.L()),
		federated.WithMetricEmitter(cfg.Metrics.Emitter),
		// The hot leg's snapshot and reads (#856): every federated pass
		// reads PostgreSQL through them (#858).
		federated.WithPgxHotLeg(pool, repository),
	}
	// Every pass resolves its parquet tiers from the publication record in
	// its snapshot (docs/publication-protocol-v2.md, "Readers"). The reader
	// exists exactly when DuckDB is enabled; an engine with DuckDB disabled
	// routes every query to PostgreSQL and runs no pass.
	if reader != nil {
		engineOpts = append(engineOpts, federated.WithPublicationReader(reader, federated.DefaultPublishedPassDeadline))
	}
	return repository, federated.NewDBFederatedQueryEngine(
		repository,
		federated.NewDuckDBClientQueryExecutor(duckClient),
		newDuckDBCircuitBreaker(cfg.DuckDB),
		cfg.DuckDB,
		metadataCache,
		engineOpts...,
	)
}

// newFederatedReadSurface opens the DuckDB client and builds the publication
// reader together, because their failure modes are coupled and only make
// sense as a pair: with DuckDB disabled it builds neither.
//
// Both failures are fatal. With DuckDB enabled every non-transactional Query
// enters the federated engine (#715), so a client that fails to open would
// leave the deployment answering every such query with an error, or, under
// the degraded fallback, quietly moving every scan onto PostgreSQL; a reader
// that fails to build would fail every pass. Startup fails instead, naming the
// cause. On the reader's failure the already-open DuckDB client has no other
// owner, so its handle and pool are released here rather than leaked for the
// process lifetime.
func newFederatedReadSurface(
	ctx context.Context,
	deps entityManagerDependencies,
	spec publicationReaderSpec,
) (*federated.DuckDBClient, federated.PublicationReader, error) {
	if !spec.duck.Enabled {
		return nil, nil, nil
	}
	zap.S().Infow("initializing DuckDB client", "dbPath", spec.duck.DBPath)
	duckClient, err := deps.newDuckDBClient(ctx, spec.duck)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize duckdb client (duckdb is enabled): %w", err)
	}
	zap.S().Infow("duckdb client initialized")

	reader, err := deps.newPublicationReader(ctx, spec)
	if err != nil {
		discardDuckClient(duckClient)
		return nil, nil, fmt.Errorf("failed to build the publication reader: %w", err)
	}
	return duckClient, reader, nil
}

// snapshotPoolOf answers pool as a snapshotPool, or a nil interface for a nil
// pool, so a typed nil never passes the reader's nil check.
func snapshotPoolOf(pool *pgxpool.Pool) snapshotPool {
	if pool == nil {
		return nil
	}
	return pool
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
