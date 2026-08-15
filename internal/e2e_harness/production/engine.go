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
// pool, the Postgres dirty-ID fetcher, the per-test DuckDB client, the
// factory-shape shared plan cache (#345), an optional circuit breaker
// (WithBreaker), and the loaded metadata cache.
// This mirrors the production assembly used by the benchmark runner
// (internal/e2e_harness/federated/benchmark/execute.go).
func (e *Env) Engine() *fedengine.DBFederatedQueryEngine {
	if e.engine != nil {
		return e.engine
	}

	// One plan cache per engine assembly, shared between the repository and
	// the engine exactly as factory.newRepositoryAndEngine pairs them
	// (#142/#345). Building it here rather than on the Env ties its lifetime
	// to the memoized engine: EvolveSchema and ReopenDuckDB drop e.engine, so
	// the cache is discarded with it, matching the cold restart both model.
	planCache := queryplan.NewCache(4096)
	repo := internal.NewDBPersistentRecordRepository(e.Pool, e.Metadata, internal.WithPlanCache(planCache))
	if e.breaker == nil && e.opts.breakerFailures > 0 {
		// Built once per Env, not per engine assembly: breaker state must
		// survive ReopenDuckDB/RestartPostgres handle rebuilds so #185
		// recovery scenarios can observe the open-to-closed transition.
		e.breaker = fedengine.NewCircuitBreaker(e.opts.breakerFailures, e.opts.breakerCooldown, e.opts.breakerCooldown)
	}

	// The engine's logger carries the #256 stamp/footer cross-check warning,
	// which has no other outlet — the read it observes succeeds.
	opts := []fedengine.EngineOption{
		fedengine.WithLogger(e.logger),
		fedengine.WithPlanCache(planCache),
	}
	if src := e.parquetSource(); src != nil {
		if e.ParquetSourceWrap != nil {
			src = e.ParquetSourceWrap(src)
		}
		opts = append(opts, fedengine.WithParquetSource(src))
	}
	e.engine = fedengine.NewDBFederatedQueryEngine(
		repo,
		fedengine.NewPostgresDirtyIDFetcher(e.Pool),
		fedengine.NewDuckDBClientQueryExecutor(e.Duck),
		e.breaker,
		e.DuckCfg,
		e.Metadata,
		fedengine.DuckDBPostgresConnStringFromPool(e.Pool),
		opts...,
	)
	return e.engine
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
	manager, err := internal.NewEntityManager(transformer, repo, e.Engine(), e.Registry, config, validator,
		internal.WithRelationIndex(relationIndex))
	if err != nil {
		e.T.Fatalf("build entity manager over %s: %v", schemaDir, err)
	}
	e.manager = manager
	return e.manager
}
