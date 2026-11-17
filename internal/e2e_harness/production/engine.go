package production

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	fedengine "github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/lychee-technology/forma/internal/queryplan"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"
)

// Engine returns the Env's real federated query engine, assembling it on
// first use: the production DBPersistentRecordRepository over the per-test
// pool, the pgx hot leg's snapshot pool and reader, the per-test DuckDB client, the
// factory-shape shared plan cache (#345), an optional circuit breaker
// (WithBreaker), and the loaded metadata cache.
// This mirrors the production assembly used by the benchmark runner
// (internal/e2e_harness/federated/benchmark/execute.go).
func (e *Env) Engine() *fedengine.DBFederatedQueryEngine {
	if e.engine != nil {
		return e.engine
	}
	e.engine = e.assembleEngine(e.DuckCfg)
	return e.engine
}

// assembleEngine builds the Env's engine over cfg.
func (e *Env) assembleEngine(cfg forma.DuckDBConfig) *fedengine.DBFederatedQueryEngine {
	// One plan cache per engine assembly, shared between the repository and
	// the engine exactly as factory.newRepositoryAndEngine pairs them
	// (#142/#345). Building it here rather than on the Env ties its lifetime
	// to the memoized engine: EvolveSchema and ReopenDuckDB drop e.engine, so
	// the cache is discarded with it, matching the cold restart that both model.
	planCache := queryplan.NewCache(4096)
	repo := internal.NewDBPersistentRecordRepository(e.Pool, e.Metadata, internal.WithPlanCache(planCache))
	if e.breaker == nil && e.opts.breakerFailures > 0 {
		// Built once per Env, not per engine assembly: breaker state must
		// survive ReopenDuckDB/RestartPostgres handle rebuilds so #185
		// recovery scenarios can observe the open-to-closed transition.
		e.breaker = fedengine.NewCircuitBreaker(e.opts.breakerFailures, e.opts.breakerCooldown, e.opts.breakerCooldown)
	}

	// The engine's logger carries the #256 stamp/footer cross-check warning,
	// which has no other outlet — the read it observes succeeds. A nil
	// emitter (the default) emits nothing, as in the factory.
	opts := []fedengine.EngineOption{
		fedengine.WithLogger(e.logger),
		fedengine.WithPlanCache(planCache),
		fedengine.WithMetricEmitter(e.opts.metricEmitter),
	}
	var hotLeg fedengine.HotLegReader = repo
	if e.HotLegReaderWrap != nil {
		hotLeg = e.HotLegReaderWrap(hotLeg)
	}
	opts = append(opts, fedengine.WithPgxHotLeg(e.Pool, hotLeg))
	if src := e.parquetSource(); src != nil {
		if e.ParquetSourceWrap != nil {
			src = e.ParquetSourceWrap(src)
		}
		opts = append(opts, fedengine.WithParquetSource(src))
	}
	return fedengine.NewDBFederatedQueryEngine(
		repo,
		fedengine.NewDuckDBClientQueryExecutor(e.Duck),
		e.breaker,
		cfg,
		e.Metadata,
		opts...,
	)
}

// Breaker returns the circuit breaker the Env's engine admits queries
// through, or nil when the Env was built without WithBreaker. It assembles
// the engine first, because the breaker is created with it. The breaker is
// cached on the Env across ReopenDuckDB/RestartPostgres rebuilds, so the
// returned value stays the live one. Tests use it to observe breaker state
// directly (IsOpen), e.g. to pin that a read left the breaker untouched
// (#499).
func (e *Env) Breaker() *fedengine.CircuitBreaker {
	e.Engine()
	return e.breaker
}

// clusterS3Client forwards every S3 call to the Cluster's *current* client at
// call time, so a client rebuilt by RestartS3 (the host-mapped port can change)
// is picked up without re-wiring. This is what lets parquetSource dedup onto
// manifest.NewS3QuerySource without freezing the client at construction —
// the naive dedup #250 plan decision D2 rejected (#302).
type clusterS3Client struct{ cluster *Cluster }

var _ manifest.S3ProbeClient = clusterS3Client{}

func (f clusterS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return f.cluster.S3.GetObject(ctx, params, optFns...)
}

func (f clusterS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return f.cluster.S3.PutObject(ctx, params, optFns...)
}

func (f clusterS3Client) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return f.cluster.S3.HeadObject(ctx, params, optFns...)
}

// parquetSource builds the manifest-driven parquet path resolver matching the
// Env's CDC manifest wiring (#187) through the shared production assembly
// (manifest.NewS3QuerySource, #250/#302). Reads scan exactly the listed
// objects, missing listed keys classify as ErrParquetSetInconsistent, and
// never-flushed schemas fall back to the legacy glob. Nil when the Env opted
// out of manifests (WithoutManifest) — those Envs keep glob reads.
func (e *Env) parquetSource() fedengine.ParquetSource {
	if e.CDC.ManifestTemplate == "" {
		return nil
	}
	return manifest.NewS3QuerySource(clusterS3Client{e.Cluster}, manifest.S3QuerySourceConfig{
		Bucket:           e.Cluster.Bucket,
		ManifestPrefix:   e.CDC.ManifestPrefix,
		ManifestTemplate: e.CDC.ManifestTemplate,
		DataPrefix:       e.S3Prefix,
	})
}

// EntityManager returns the Env's real production EntityManager, assembling
// it on first use over the same repository, engine, and registry the
// production stack wires together.
//
// The JSON Schema validator is built here rather than passed as nil (#314).
// A nil validator switches write-path validation off entirely, so every
// fixture payload this harness writes would be vacuously valid and the suite
// would assert nothing about the constraints production now enforces. It is
// built from e.Registry and the Env's current schema directory, which is what
// factory.NewEntityManagerWithConfigContext does, and it fails closed on an
// unresolvable schema for the same reason production refuses to start.
//
// It is built alongside the manager, not at provision time, so EvolveSchema —
// which drops the memoized manager and repoints the schema directory —
// re-derives it from the new generation, mirroring the restart it models.
//
// Its deployment runs DuckDB, so every non-transactional Query enters the
// federated engine (#715) under the Env's routing strategy. The Env leaves
// the strategy empty unless WithRoutingStrategy sets one, and the engine
// reads an empty strategy as its default decision, a DuckDB pass for every
// query; RoutedEntityManager picks another.
func (e *Env) EntityManager() forma.EntityManager {
	if e.manager != nil {
		return e.manager
	}
	e.manager = e.buildEntityManager(e.DuckCfg, e.Engine())
	return e.manager
}

// DuckDBRoutedEntityManager is EntityManager on a deployment whose routing
// sends every query through a DuckDB pass: the freshness-first strategy
// keeps the engine's default all-tier decision, which no cost heuristic
// narrows to PostgreSQL. A request no longer chooses its route (#715), so
// this is how a test reads the parquet tiers through the public API.
func (e *Env) DuckDBRoutedEntityManager() forma.EntityManager {
	return e.RoutedEntityManager(forma.RoutingStrategyFreshnessFirst)
}

// RoutedEntityManager is EntityManager on a deployment running the given
// routing strategy; hybrid answers a page under 1000 rows from PostgreSQL
// alone, as the default production configuration does. It is memoized per
// strategy like EntityManager, each over its own engine and plan cache.
func (e *Env) RoutedEntityManager(strategy forma.RoutingStrategy) forma.EntityManager {
	if m, ok := e.routedManagers[strategy]; ok {
		return m
	}
	cfg := e.DuckCfg
	cfg.Routing.Strategy = strategy
	if e.routedManagers == nil {
		e.routedManagers = map[forma.RoutingStrategy]forma.EntityManager{}
	}
	e.routedManagers[strategy] = e.buildEntityManager(cfg, e.assembleEngine(cfg))
	return e.routedManagers[strategy]
}

// PostgresEntityManager is EntityManager on a deployment without DuckDB:
// every Query runs on PostgreSQL alone, whatever the routing strategy, and
// no engine records an execution plan for it (#715). It is the PostgreSQL
// control a test compares a federated answer against: asked for the plan,
// its answer carries none, which is how a test pins that the engine was never
// entered. Memoized like EntityManager.
func (e *Env) PostgresEntityManager() forma.EntityManager {
	if e.postgresManager != nil {
		return e.postgresManager
	}
	off := e.DuckCfg
	off.Enabled = false
	e.postgresManager = e.buildEntityManager(off, e.Engine())
	return e.postgresManager
}

// buildEntityManager assembles a manager over engine for a deployment
// running duckCfg.
func (e *Env) buildEntityManager(duckCfg forma.DuckDBConfig, engine *fedengine.DBFederatedQueryEngine) forma.EntityManager {

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
		DuckDB: duckCfg,
	}

	repo := internal.NewDBPersistentRecordRepository(e.Pool, e.Metadata)
	transformer := transform.NewPersistentRecordTransformer(e.Registry)
	// The harness constructs the manager directly instead of through the factory,
	// so it has to run the factory's relation guard itself or it is not modelling
	// production startup (#318). That means reproducing the factory's shape, not
	// merely making its calls:
	//
	//   - both guards are built from one snapshot of the registry's documents, so
	//     a registry that answers differently on a second read cannot hand the
	//     validator and the index two different documents
	//     (internal.SnapshotSchemaDocuments, factory.buildSchemaGuards);
	//   - the index is built once and handed to the manager, because a harness that
	//     approved one index and then let the manager load another would be
	//     modelling the wrong startup. The manager's own load now fails closed
	//     (#388), so what a second load could still cost is agreement, not the
	//     guard;
	//   - the manager keeps e.Registry rather than the snapshot, exactly as the
	//     factory leaves the caller's registry in place.
	documents := internal.SnapshotSchemaDocuments(e.Registry)
	validator, err := schemavalidate.New(documents, schemaDir)
	if err != nil {
		e.T.Fatalf("build schema validator over %s: %v", schemaDir, err)
	}
	relationIndex, err := internal.LoadRelationIndex(documents)
	if err != nil {
		e.T.Fatalf("validate schema relations over the registry for %s: %v", schemaDir, err)
	}
	manager, err := internal.NewEntityManager(transformer, repo, engine, e.Registry, config, validator,
		internal.WithRelationIndex(relationIndex))
	if err != nil {
		e.T.Fatalf("build entity manager over %s: %v", schemaDir, err)
	}
	return manager
}
