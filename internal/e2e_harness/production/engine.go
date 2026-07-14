package production

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	fedengine "github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/manifest"
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
	if e.breaker == nil && e.opts.breakerFailures > 0 {
		// Built once per Env, not per engine assembly: breaker state must
		// survive ReopenDuckDB/RestartPostgres handle rebuilds so #185
		// recovery scenarios can observe the open-to-closed transition.
		e.breaker = fedengine.NewCircuitBreaker(e.opts.breakerFailures, e.opts.breakerCooldown, e.opts.breakerCooldown)
	}

	var opts []fedengine.EngineOption
	if src := e.parquetSource(); src != nil {
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

// parquetSource builds the manifest-driven parquet path resolver matching
// the Env's CDC manifest wiring (#187): reads scan exactly the listed
// objects, missing listed keys classify as ErrParquetSetInconsistent, and
// never-flushed schemas fall back to the legacy glob. Nil when the Env
// opted out of manifests (WithoutManifest) — those Envs keep glob reads.
// The closures read Cluster fields at call time, so an S3 client rebuilt by
// RestartS3 is picked up without re-wiring.
func (e *Env) parquetSource() fedengine.ParquetSource {
	if e.CDC.ManifestTemplate == "" {
		return nil
	}
	c := e.Cluster
	return &manifest.QuerySource{
		Store:    &manifest.S3Store{Client: c.S3, Bucket: c.Bucket},
		Resolver: manifest.PathResolver{Prefix: e.CDC.ManifestPrefix, PathTemplate: e.CDC.ManifestTemplate},
		Bucket:   c.Bucket,
		Exists: func(ctx context.Context, key string) (bool, error) {
			_, err := c.S3.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(c.Bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				var notFound *types.NotFound
				if errors.As(err, &notFound) {
					return false, nil
				}
				return false, err
			}
			return true, nil
		},
		Fallback: func(schemaID int16) string {
			return fmt.Sprintf("s3://%s/%s/%d/*.parquet", c.Bucket, e.S3Prefix, schemaID)
		},
	}
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
