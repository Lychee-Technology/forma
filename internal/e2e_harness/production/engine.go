package production

import (
	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	fedengine "github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/transform"
)

// Engine returns the Env's real federated query engine, assembling it on
// first use: the production DBPersistentRecordRepository over the per-test
// pool, the Postgres dirty-ID fetcher, the per-test DuckDB client, an
// optional circuit breaker (WithBreaker), and the loaded metadata cache.
// This mirrors the production assembly used by the benchmark runner
// (internal/e2e_harness/federated/benchmark/execute.go).
func (e *Env) Engine() *fedengine.DBFederatedQueryEngine {
	if e.engine != nil {
		return e.engine
	}

	repo := internal.NewDBPersistentRecordRepository(e.Pool, e.Metadata)
	var breaker *fedengine.CircuitBreaker
	if e.opts.breakerFailures > 0 {
		breaker = fedengine.NewCircuitBreaker(e.opts.breakerFailures, e.opts.breakerCooldown, e.opts.breakerCooldown)
	}

	e.engine = fedengine.NewDBFederatedQueryEngine(
		repo,
		fedengine.NewPostgresDirtyIDFetcher(e.Pool),
		fedengine.NewDuckDBClientQueryExecutor(e.Duck),
		breaker,
		e.DuckCfg,
		e.Metadata,
		fedengine.DuckDBPostgresConnStringFromPool(e.Pool),
	)
	return e.engine
}

// EntityManager returns the Env's real production EntityManager, assembling
// it on first use over the same repository, engine, and registry the
// production stack wires together.
func (e *Env) EntityManager() forma.EntityManager {
	if e.manager != nil {
		return e.manager
	}

	schemaDir := e.opts.schemaDir
	if schemaDir == "" {
		schemaDir = FixtureSchemasDir()
	}
	config := &forma.Config{
		Database: forma.DatabaseConfig{
			TableNames: forma.TableNames{
				SchemaRegistry: e.Tables.SchemaRegistry,
				EntityMain:     e.Tables.EntityMain,
				EAVData:        e.Tables.EAVData,
				ChangeLog:      e.Tables.ChangeLog,
			},
		},
		Query: forma.QueryConfig{
			DefaultPageSize: 20,
			MaxPageSize:     1000,
		},
		Entity: forma.EntityConfig{
			SchemaDirectory: schemaDir,
		},
		DuckDB: e.DuckCfg,
	}

	repo := internal.NewDBPersistentRecordRepository(e.Pool, e.Metadata)
	transformer := transform.NewPersistentRecordTransformer(e.Registry)
	e.manager = internal.NewEntityManager(transformer, repo, e.Engine(), e.Registry, config)
	return e.manager
}
