package benchmark

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	federated "github.com/lychee-technology/forma/internal/e2e_harness/federated"
	fedengine "github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/queryplan"
	"github.com/lychee-technology/forma/internal/schemameta"
)

// benchmarkQueryStack is what every service-path benchmark query assembles
// over its pgx pool: the benchmark schema registry table, the registry and
// metadata read from it, and the repository and federated engine built on
// that metadata, sharing the runner's plan cache.
type benchmarkQueryStack struct {
	schemaTable string
	registry    forma.SchemaRegistry
	metadata    *schemameta.MetadataCache
	duckCfg     forma.DuckDBConfig
	repo        *internal.DBPersistentRecordRepository
	engine      *fedengine.DBFederatedQueryEngine
}

// openBenchmarkPool opens a pgx pool on the harness's Postgres. The caller
// closes it.
func openBenchmarkPool(ctx context.Context, h *federated.FederatedTestHarness) (*pgxpool.Pool, error) {
	if h == nil || h.PGDSN == "" {
		return nil, fmt.Errorf("benchmark harness postgres DSN is required")
	}
	pool, err := pgxpool.New(ctx, h.PGDSN)
	if err != nil {
		return nil, fmt.Errorf("connect benchmark pgx pool: %w", err)
	}
	return pool, nil
}

// newBenchmarkQueryStack resets the benchmark schema registry table (see
// ensureBenchmarkSchemaRegistry), then loads the registry and metadata from it
// and builds the repository and federated engine.
func newBenchmarkQueryStack(ctx context.Context, pool *pgxpool.Pool, h *federated.FederatedTestHarness, planCache *queryplan.Cache) (*benchmarkQueryStack, error) {
	schemaTable, err := ensureBenchmarkSchemaRegistry(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("prepare benchmark schema registry: %w", err)
	}
	registry, err := schemameta.NewFileSchemaRegistry(pool, schemaTable, FixturesDir())
	if err != nil {
		return nil, fmt.Errorf("build benchmark schema registry: %w", err)
	}
	metadata, err := schemameta.NewMetadataLoader(pool, schemaTable, FixturesDir()).LoadMetadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("load benchmark metadata: %w", err)
	}
	duckCfg := engineDuckDBConfig(h)
	repo := internal.NewDBPersistentRecordRepository(pool, metadata, internal.WithPlanCache(planCache))
	engine := fedengine.NewDBFederatedQueryEngine(
		repo,
		fedengine.NewPostgresDirtyIDFetcher(pool),
		fedengine.NewDuckDBClientQueryExecutor(h.Duck),
		nil,
		duckCfg,
		metadata,
		fedengine.DuckDBPostgresConnStringFromPool(pool),
		fedengine.WithPlanCache(planCache),
	)
	return &benchmarkQueryStack{
		schemaTable: schemaTable,
		registry:    registry,
		metadata:    metadata,
		duckCfg:     duckCfg,
		repo:        repo,
		engine:      engine,
	}, nil
}

// resolveSchemaID resolves a benchmark schema name, defaulting to "trade".
func (s *benchmarkQueryStack) resolveSchemaID(schemaName string) (int16, error) {
	if schemaName == "" {
		schemaName = "trade"
	}
	schemaID, _, err := s.registry.GetSchemaAttributeCacheByName(schemaName)
	if err != nil {
		return 0, fmt.Errorf("resolve schema %s: %w", schemaName, err)
	}
	return schemaID, nil
}

// engineDuckDBConfig returns the engine-level DuckDB config for a harness:
// the exact configuration the harness started DuckDB with (single source of
// truth for resource settings), or a zero config when DuckDB is not running.
func engineDuckDBConfig(h *federated.FederatedTestHarness) forma.DuckDBConfig {
	if h.Duck == nil {
		return forma.DuckDBConfig{}
	}
	return h.DuckCfg
}

// benchmarkStorageTables names the harness's CDC tables.
func benchmarkStorageTables(h *federated.FederatedTestHarness) model.StorageTables {
	return model.StorageTables{
		EntityMain: h.CDCConfig.EntityMainTable,
		EAVData:    h.CDCConfig.EAVDataTable,
		ChangeLog:  h.CDCConfig.ChangeLogTable,
	}
}
