package production

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/cdc"
	fedengine "github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
	"go.uber.org/zap"
)

// Env is one test's isolated slice of the shared cluster: its own Postgres
// database (standard production table names), S3 prefix, and in-memory
// DuckDB client.
type Env struct {
	T       *testing.T
	Cluster *Cluster

	RunID  string
	Seed   int64
	DBName string

	Pool     *pgxpool.Pool
	Tables   model.StorageTables
	Registry forma.SchemaRegistry
	Metadata *schemameta.MetadataCache
	Duck     *fedengine.DuckDBClient
	DuckCfg  forma.DuckDBConfig
	S3Prefix string
	CDC      cdc.CDCConfig

	logger *zap.Logger
	opts   envOptions

	manager      forma.EntityManager
	engine       *fedengine.DBFederatedQueryEngine
	events       []*Event
	eventSeq     int
	queryN       int
	queries      []*QueryResult
	lastDiff     *Diff
	rng          *rand.Rand
	genOrdinal   int
	provisionErr error
}

// EnvOption customizes NewEnv.
type EnvOption func(*envOptions)

type envOptions struct {
	seed            *int64
	schemaDir       string
	minRecords      int
	maxAgeMs        int64
	duckMemoryMB    int
	breakerFailures int
	breakerCooldown time.Duration
	routing         forma.RoutingStrategy
	withoutManifest bool
}

// WithSeed pins the per-test seed instead of deriving it from the cluster
// seed and test name.
func WithSeed(seed int64) EnvOption {
	return func(o *envOptions) { o.seed = &seed }
}

// WithSchemaDir loads fixture schemas from a custom directory instead of the
// bundled schemas/ fixtures.
func WithSchemaDir(dir string) EnvOption {
	return func(o *envOptions) { o.schemaDir = dir }
}

// WithFlushThresholds overrides the CDC flush thresholds (for #179). The
// default is MinRecords=1 so RunFlush always flushes.
func WithFlushThresholds(minRecords int, maxAgeMs int64) EnvOption {
	return func(o *envOptions) {
		o.minRecords = minRecords
		o.maxAgeMs = maxAgeMs
	}
}

// WithDuckMemoryMB overrides the per-test DuckDB memory limit (default 1024,
// lowered so parallel CI runs survive).
func WithDuckMemoryMB(mb int) EnvOption {
	return func(o *envOptions) { o.duckMemoryMB = mb }
}

// WithBreaker enables a circuit breaker on the federated engine (for #185).
func WithBreaker(maxFailures int, cooldown time.Duration) EnvOption {
	return func(o *envOptions) {
		o.breakerFailures = maxFailures
		o.breakerCooldown = cooldown
	}
}

// WithRoutingStrategy sets the engine routing strategy.
func WithRoutingStrategy(strategy forma.RoutingStrategy) EnvOption {
	return func(o *envOptions) { o.routing = strategy }
}

// WithoutManifest disables manifest tracking (the explicit opt-out; by
// default the Env wires a manifest template so flushes update the manifest).
func WithoutManifest() EnvOption {
	return func(o *envOptions) { o.withoutManifest = true }
}

// NewEnv provisions a per-test environment on the shared cluster and
// registers cleanup. Cleanup registration order matters (LIFO): resource
// teardown is registered first so the artifact dump (registered second, in
// registerArtifactDump) runs while the database still exists.
func NewEnv(t *testing.T, c *Cluster, opts ...EnvOption) *Env {
	t.Helper()
	ctx := context.Background()

	options := envOptions{minRecords: 1, maxAgeMs: 3600000, duckMemoryMB: 1024}
	for _, opt := range opts {
		opt(&options)
	}

	e := &Env{
		T:       t,
		Cluster: c,
		RunID:   c.RunID + "/" + sanitizeName(t.Name()),
		Seed:    deriveSeed(c.Seed, t.Name(), options.seed),
		opts:    options,
		logger:  newEnvLogger(),
		Tables: model.StorageTables{
			EntityMain:     "entity_main",
			EAVData:        "eav_data",
			ChangeLog:      "change_log",
			SchemaRegistry: "schema_registry",
		},
	}

	seq := c.envSeq.Add(1)
	e.DBName = fmt.Sprintf("e2e_%d", seq)
	e.S3Prefix = fmt.Sprintf("e2e/%s/env%d", c.RunID, seq)

	// Cleanups are registered BEFORE provisioning so a provisioning failure
	// still produces run-specific diagnostic artifacts (the dump steps
	// tolerate unprovisioned resources). t.Cleanup runs even after Fatalf.
	t.Cleanup(func() { e.teardown(context.Background()) })
	e.registerArtifactDump()

	if err := e.provision(ctx); err != nil {
		e.provisionErr = err
		t.Fatalf("provision production env: %v (diagnostics under %s)", err, e.ArtifactsDir())
	}

	t.Logf("production env %s: db=%s prefix=s3://%s/%s seed=%d", e.RunID, e.DBName, c.Bucket, e.S3Prefix, e.Seed)
	return e
}

// provision creates the database, tables, fixture schemas, metadata caches,
// DuckDB client, and CDC config.
func (e *Env) provision(ctx context.Context) error {
	c := e.Cluster
	if _, err := c.Base.PGDB.ExecContext(ctx, "CREATE DATABASE "+e.DBName); err != nil {
		return fmt.Errorf("create database %s: %w", e.DBName, err)
	}

	pool, err := pgxpool.New(ctx, e.PGDSN())
	if err != nil {
		return fmt.Errorf("connect test database: %w", err)
	}
	e.Pool = pool

	if err := applyProductionDDL(ctx, pool); err != nil {
		return err
	}

	schemaDir := e.opts.schemaDir
	if schemaDir == "" {
		schemaDir = FixtureSchemasDir()
	}
	if err := RegisterSchemas(ctx, pool, DefaultSchemaFixtures()...); err != nil {
		return err
	}

	registry, err := schemameta.NewFileSchemaRegistryContext(ctx, pool, e.Tables.SchemaRegistry, schemaDir)
	if err != nil {
		return fmt.Errorf("build schema registry: %w", err)
	}
	e.Registry = registry

	metadata, err := schemameta.NewMetadataLoader(pool, e.Tables.SchemaRegistry, schemaDir).LoadMetadata(ctx)
	if err != nil {
		return fmt.Errorf("load schema metadata: %w", err)
	}
	e.Metadata = metadata

	if err := e.startDuckDB(); err != nil {
		return err
	}

	e.CDC = e.buildCDCConfig()
	return nil
}

func (e *Env) startDuckDB() error {
	c := e.Cluster
	e.DuckCfg = forma.DuckDBConfig{
		Enabled:        true,
		DBPath:         ":memory:",
		MemoryLimitMB:  e.opts.duckMemoryMB,
		EnableS3:       true,
		EnableParquet:  true,
		S3Endpoint:     c.S3Endpoint,
		S3AccessKey:    c.S3AccessKey,
		S3SecretKey:    c.S3SecretKey,
		S3Region:       c.S3Region,
		MaxConnections: 2,
		MaxParallelism: 2,
		QueryTimeout:   60 * time.Second,
	}
	e.DuckCfg.Routing.Strategy = e.opts.routing

	duck, err := fedengine.NewDuckDBClient(e.DuckCfg)
	if err != nil {
		return fmt.Errorf("start duckdb client: %w", err)
	}
	e.Duck = duck
	return nil
}

func (e *Env) buildCDCConfig() cdc.CDCConfig {
	c := e.Cluster
	pgPort, _ := strconv.Atoi(c.PGPort)
	cfg := cdc.CDCConfig{
		ChangeLogTable:    e.Tables.ChangeLog,
		EntityMainTable:   e.Tables.EntityMain,
		EAVDataTable:      e.Tables.EAVData,
		MinRecords:        e.opts.minRecords,
		MaxAgeMs:          e.opts.maxAgeMs,
		BatchSize:         10000,
		PGHost:            c.PGHost,
		PGPort:            pgPort,
		PGUser:            c.PGUser,
		PGPassword:        c.PGPassword,
		PGDB:              e.DBName,
		PGSSLMode:         c.PGSSLMode,
		DuckThreads:       2,
		DuckMemLimit:      "1GB",
		QueryTimeout:      2 * time.Minute,
		S3Bucket:          c.Bucket,
		S3Prefix:          e.S3Prefix,
		S3Endpoint:        c.S3Endpoint,
		S3Region:          c.S3Region,
		S3UsePath:         true,
		S3AccessKeyID:     c.S3AccessKey,
		S3SecretAccessKey: c.S3SecretKey,
	}
	if !e.opts.withoutManifest {
		cfg.ManifestPrefix = e.S3Prefix
		cfg.ManifestTemplate = "manifest/{{.SchemaID}}.json"
	}
	return cfg
}

// RestartPostgres restarts the cluster's Postgres container and rebinds
// every per-test handle that referenced the old server: the pgx pool, the
// CDC config (the host-mapped port can change), the DuckDB client (so no
// cached postgres attachment survives), and the lazily built EntityManager
// and federated engine. Registry and Metadata are load-once snapshots and
// need no rebuild. Only for tests owning a DedicatedCluster.
func (e *Env) RestartPostgres(ctx context.Context) error {
	if err := e.Cluster.RestartPostgres(ctx); err != nil {
		return fmt.Errorf("restart cluster postgres: %w", err)
	}
	return e.reconnectAfterRestart(ctx)
}

// reconnectAfterRestart rebuilds the Env's server-bound handles against the
// restarted Postgres.
func (e *Env) reconnectAfterRestart(ctx context.Context) error {
	if e.Pool != nil {
		e.Pool.Close()
	}
	pool, err := pgxpool.New(ctx, e.PGDSN())
	if err != nil {
		return fmt.Errorf("reconnect test database after restart: %w", err)
	}
	e.Pool = pool

	if e.Duck != nil {
		_ = e.Duck.Close()
	}
	if err := e.startDuckDB(); err != nil {
		return err
	}
	e.CDC = e.buildCDCConfig()
	e.manager = nil
	e.engine = nil
	return nil
}

// PGDSN returns the DSN of this Env's dedicated database.
func (e *Env) PGDSN() string {
	c := e.Cluster
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		c.PGUser, c.PGPassword, c.PGHost, c.PGPort, e.DBName, c.PGSSLMode)
}

// redactedPGDSN is the DSN with the password masked; on-disk artifacts must
// not expose externally supplied credentials (PRODUCTION_E2E_EXTERNAL_*).
func (e *Env) redactedPGDSN() string {
	c := e.Cluster
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		c.PGUser, redactedPassword, c.PGHost, c.PGPort, e.DBName, c.PGSSLMode)
}

const redactedPassword = "REDACTED"

// teardown releases per-test resources: DuckDB, the connection pool, the
// database (WITH FORCE, after pools are closed), and the S3 prefix. Under
// KEEP_E2E_ENV=1 everything is kept and connection info is printed.
func (e *Env) teardown(ctx context.Context) {
	if KeepEnv() {
		fmt.Printf("KEEP_E2E_ENV=1: keeping env %s\n  pg dsn:    %s\n  s3 prefix: s3://%s/%s\n",
			e.RunID, e.PGDSN(), e.Cluster.Bucket, e.S3Prefix)
		return
	}
	if e.Duck != nil {
		_ = e.Duck.Close()
		e.Duck = nil
	}
	if e.Pool != nil {
		e.Pool.Close()
		e.Pool = nil
	}
	if e.Cluster.Base.PGDB != nil {
		if _, err := e.Cluster.Base.PGDB.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", e.DBName)); err != nil {
			e.T.Logf("drop database %s: %v", e.DBName, err)
		}
	}
	e.deleteS3Prefix(ctx)
}

// deleteS3Prefix removes every object under the Env's S3 prefix.
func (e *Env) deleteS3Prefix(ctx context.Context) {
	keys, err := e.listS3Keys(ctx)
	if err != nil {
		e.T.Logf("list s3 prefix %s: %v", e.S3Prefix, err)
		return
	}
	for _, key := range keys {
		if _, err := e.Cluster.S3.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(e.Cluster.Bucket),
			Key:    aws.String(key),
		}); err != nil {
			e.T.Logf("delete s3 object %s: %v", key, err)
		}
	}
}

// listS3Keys returns all object keys under the Env's S3 prefix.
func (e *Env) listS3Keys(ctx context.Context) ([]string, error) {
	var keys []string
	var token *string
	for {
		out, err := e.Cluster.S3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(e.Cluster.Bucket),
			Prefix:            aws.String(e.S3Prefix + "/"),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("list objects under %s: %w", e.S3Prefix, err)
		}
		for _, obj := range out.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
		if out.NextContinuationToken == nil {
			return keys, nil
		}
		token = out.NextContinuationToken
	}
}

func deriveSeed(clusterSeed int64, testName string, override *int64) int64 {
	if override != nil {
		return *override
	}
	h := fnv.New64a()
	fmt.Fprintf(h, "%d/%s", clusterSeed, testName)
	return int64(h.Sum64() & 0x7fffffffffffffff)
}

func sanitizeName(name string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '-'
		}
	}, name)
}

func newEnvLogger() *zap.Logger {
	if os.Getenv("E2E_VERBOSE") == "1" {
		logger, err := zap.NewDevelopment()
		if err == nil {
			return logger
		}
	}
	return zap.NewNop()
}
