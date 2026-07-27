package factory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Stubs
// ---------------------------------------------------------------------------

// stubProbeClient satisfies manifest.S3ProbeClient without touching the
// network; the assembly tests only inspect where it was wired, never call it.
type stubProbeClient struct{}

func (stubProbeClient) GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, errors.New("stubProbeClient.GetObject must not be called")
}

func (stubProbeClient) PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return nil, errors.New("stubProbeClient.PutObject must not be called")
}

func (stubProbeClient) HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return nil, errors.New("stubProbeClient.HeadObject must not be called")
}

// swapManifestS3Client replaces the S3 client seam for the duration of a test.
func swapManifestS3Client(t *testing.T, fn func(context.Context, forma.DuckDBConfig) (manifest.S3ProbeClient, error)) {
	t.Helper()
	original := newManifestS3Client
	newManifestS3Client = fn
	t.Cleanup(func() { newManifestS3Client = original })
}

// manifestReadConfig is a fully configured manifest read surface.
func manifestReadConfig() forma.DuckDBConfig {
	return forma.DuckDBConfig{
		Enabled:          true,
		S3Bucket:         "bkt",
		S3DataPrefix:     "data",
		ManifestPrefix:   "manifest",
		ManifestTemplate: "{{.SchemaID}}.json",
	}
}

// ---------------------------------------------------------------------------
// newManifestParquetSource
// ---------------------------------------------------------------------------

func TestNewManifestParquetSource_OffWhenTemplateEmpty(t *testing.T) {
	called := false
	swapManifestS3Client(t, func(context.Context, forma.DuckDBConfig) (manifest.S3ProbeClient, error) {
		called = true
		return stubProbeClient{}, nil
	})

	cfg := forma.DuckDBConfig{Enabled: true}

	src, err := newManifestParquetSource(context.Background(), cfg)

	require.NoError(t, err)
	// Interface-level nil: a typed-nil *manifest.QuerySource boxed here would
	// pass the engine's `parquetSource != nil` check and resolve paths against
	// a zero-value source. Compare the interface directly, not via assert.Nil.
	if src != nil {
		t.Fatalf("expected literal nil ParquetSource, got %#v", src)
	}
	assert.False(t, called, "S3 client must not be constructed when manifest reads are off")
}

func TestNewManifestParquetSource_OffWhenDuckDBDisabled(t *testing.T) {
	called := false
	swapManifestS3Client(t, func(context.Context, forma.DuckDBConfig) (manifest.S3ProbeClient, error) {
		called = true
		return stubProbeClient{}, nil
	})

	cfg := manifestReadConfig()
	cfg.Enabled = false

	src, err := newManifestParquetSource(context.Background(), cfg)

	require.NoError(t, err)
	if src != nil {
		t.Fatalf("expected literal nil ParquetSource, got %#v", src)
	}
	assert.False(t, called, "S3 client must not be constructed when DuckDB is disabled")
}

func TestNewManifestParquetSource_AssemblesQuerySource(t *testing.T) {
	client := stubProbeClient{}
	var gotCfg forma.DuckDBConfig
	swapManifestS3Client(t, func(_ context.Context, cfg forma.DuckDBConfig) (manifest.S3ProbeClient, error) {
		gotCfg = cfg
		return client, nil
	})

	cfg := manifestReadConfig()

	src, err := newManifestParquetSource(context.Background(), cfg)

	require.NoError(t, err)
	require.NotNil(t, src)
	assert.Equal(t, cfg, gotCfg, "the S3 client seam receives the DuckDB config verbatim")

	qs, ok := src.(*manifest.QuerySource)
	require.True(t, ok, "expected *manifest.QuerySource, got %T", src)
	assert.Equal(t, "bkt", qs.Bucket)
	assert.Equal(t, "manifest", qs.Resolver.Prefix)
	assert.Equal(t, "{{.SchemaID}}.json", qs.Resolver.PathTemplate)

	store, ok := qs.Store.(*manifest.S3Store)
	require.True(t, ok, "expected *manifest.S3Store, got %T", qs.Store)
	assert.Equal(t, "bkt", store.Bucket)
	assert.Equal(t, manifest.S3Client(client), store.Client)

	require.NotNil(t, qs.Exists, "missing-key classification must be wired")
	require.NotNil(t, qs.Fallback, "the legacy glob fallback must be wired from S3DataPrefix")
	assert.Equal(t, "s3://bkt/data/21/*.parquet", qs.Fallback(21))
}

// TestNewManifestParquetSource_RejectsPaddedValues pins the #250 PR review
// P2a decision: padded values are REJECTED, not silently trimmed here. The
// CDC/compaction write side never trims these shared values, so a reader-only
// trim would resolve a different key than the writer wrote for the very same
// configured string — byte parity between the two sides is the contract, and
// the config surface is where it is enforced.
//
// Boot note for cmd/server: a padded DUCKDB_MANIFEST_TEMPLATE from the
// environment still opens the manifestOn predicate (TrimSpace-based), so the
// shared prefixes get adopted and startup then fails here with the explicit
// whitespace message — loud and diagnosable, which is the intended outcome.
func TestNewManifestParquetSource_RejectsPaddedValues(t *testing.T) {
	swapManifestS3Client(t, func(context.Context, forma.DuckDBConfig) (manifest.S3ProbeClient, error) {
		t.Fatal("S3 client must not be constructed for a padded manifest configuration")
		return nil, nil
	})

	cfg := forma.DuckDBConfig{
		Enabled:          true,
		S3Bucket:         " bkt\n",
		S3DataPrefix:     "  data  ",
		ManifestPrefix:   "\tmanifest ",
		ManifestTemplate: " {{.SchemaID}}.json\n",
	}

	src, err := newManifestParquetSource(context.Background(), cfg)

	require.Error(t, err)
	if src != nil {
		t.Fatalf("expected literal nil ParquetSource on error, got %#v", src)
	}
	var cfgErr *forma.ConfigError
	require.True(t, errors.As(err, &cfgErr), "expected *forma.ConfigError, got %v", err)
	assert.Contains(t, cfgErr.Message, "whitespace")
}

// TestNewManifestParquetSource_PassesFieldsVerbatim: with whitespace rejected
// upstream, the assembly layer forwards the configured bytes unchanged — no
// second normalization that could diverge from the writers.
func TestNewManifestParquetSource_PassesFieldsVerbatim(t *testing.T) {
	swapManifestS3Client(t, func(context.Context, forma.DuckDBConfig) (manifest.S3ProbeClient, error) {
		return stubProbeClient{}, nil
	})

	cfg := manifestReadConfig()
	cfg.S3DataPrefix = "data/nested"

	src, err := newManifestParquetSource(context.Background(), cfg)

	require.NoError(t, err)
	qs, ok := src.(*manifest.QuerySource)
	require.True(t, ok, "expected *manifest.QuerySource, got %T", src)
	assert.Equal(t, cfg.S3Bucket, qs.Bucket)
	assert.Equal(t, cfg.ManifestPrefix, qs.Resolver.Prefix)
	assert.Equal(t, cfg.ManifestTemplate, qs.Resolver.PathTemplate)
	require.NotNil(t, qs.Fallback)
	assert.Equal(t, "s3://bkt/data/nested/21/*.parquet", qs.Fallback(21))
}

func TestNewManifestParquetSource_InvalidConfigFailsFast(t *testing.T) {
	swapManifestS3Client(t, func(context.Context, forma.DuckDBConfig) (manifest.S3ProbeClient, error) {
		t.Fatal("S3 client must not be constructed for an invalid manifest configuration")
		return nil, nil
	})

	cfg := manifestReadConfig()
	cfg.S3Bucket = "" // template set without a bucket

	src, err := newManifestParquetSource(context.Background(), cfg)

	require.Error(t, err)
	if src != nil {
		t.Fatalf("expected literal nil ParquetSource on error, got %#v", src)
	}
	var cfgErr *forma.ConfigError
	require.True(t, errors.As(err, &cfgErr), "expected *forma.ConfigError, got %v", err)
	assert.Equal(t, "duckdb.s3Bucket", cfgErr.Field)
}

func TestNewManifestParquetSource_S3ClientErrorFailsFast(t *testing.T) {
	sentinel := errors.New("no credentials")
	swapManifestS3Client(t, func(context.Context, forma.DuckDBConfig) (manifest.S3ProbeClient, error) {
		return nil, sentinel
	})

	src, err := newManifestParquetSource(context.Background(), manifestReadConfig())

	require.Error(t, err)
	if src != nil {
		t.Fatalf("expected literal nil ParquetSource on error, got %#v", src)
	}
	assert.ErrorIs(t, err, sentinel)
}

// ---------------------------------------------------------------------------
// Factory seam wiring
// ---------------------------------------------------------------------------

// parquetSourceTestConfig builds the minimal valid factory config used by the
// seam tests below.
func parquetSourceTestConfig(t *testing.T) *forma.Config {
	t.Helper()
	config := forma.DefaultConfig(newMockSchemaRegistry())
	config.Database.TableNames = forma.TableNames{
		SchemaRegistry: "schema_registry",
		EAVData:        "eav_data",
		EntityMain:     "entity_main",
	}
	config.Entity.SchemaDirectory = t.TempDir()
	return config
}

func TestNewEntityManagerWithConfig_Unit_NoParquetSourceWhenManifestOff(t *testing.T) {
	t.Parallel()

	deps := buildUnitEntityManagerDeps(schemameta.NewMetadataCache())
	called := false
	deps.newParquetSource = func(_ context.Context, cfg forma.DuckDBConfig) (federated.ParquetSource, error) {
		called = true
		return nil, nil
	}

	em, err := newEntityManagerWithConfigContext(context.Background(), parquetSourceTestConfig(t), nil, deps)

	require.NoError(t, err)
	require.NotNil(t, em)
	assert.True(t, called, "the parquet source seam is consulted unconditionally")
}

func TestNewEntityManagerWithConfig_Unit_ParquetSourceFromConfig(t *testing.T) {
	t.Parallel()

	deps := buildUnitEntityManagerDeps(schemameta.NewMetadataCache())
	var gotCfg forma.DuckDBConfig
	deps.newParquetSource = func(_ context.Context, cfg forma.DuckDBConfig) (federated.ParquetSource, error) {
		gotCfg = cfg
		return &manifest.QuerySource{Bucket: cfg.S3Bucket}, nil
	}

	config := parquetSourceTestConfig(t)
	config.DuckDB = manifestReadConfig()

	em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)

	require.NoError(t, err)
	require.NotNil(t, em)
	assert.Equal(t, "bkt", gotCfg.S3Bucket)
	assert.Equal(t, "data", gotCfg.S3DataPrefix)
	assert.Equal(t, "manifest", gotCfg.ManifestPrefix)
	assert.Equal(t, "{{.SchemaID}}.json", gotCfg.ManifestTemplate)
}

func TestNewEntityManagerWithConfig_Unit_ParquetSourceErrorFailsFast(t *testing.T) {
	t.Parallel()

	deps := buildUnitEntityManagerDeps(schemameta.NewMetadataCache())
	deps.newParquetSource = func(context.Context, forma.DuckDBConfig) (federated.ParquetSource, error) {
		return nil, errors.New("simulated s3 client failure")
	}

	em, err := newEntityManagerWithConfigContext(context.Background(), parquetSourceTestConfig(t), nil, deps)

	assert.Nil(t, em)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "manifest parquet source")
}

// TestNewEntityManagerWithConfig_Unit_ParquetSourceErrorClosesDuckDBClient
// pins the #250 PR review P2c leak: the DuckDB client is opened before the
// parquet source is built, and the source failure is fatal — so the already
// initialized client (a real DuckDB instance holding a file/memory handle and
// its connection pool) must be closed on the way out, not abandoned.
//
// Observable: HealthCheck runs a real query through the client's *sql.DB,
// which answers "sql: database is closed" once Close has run. That is the
// strongest available assertion — DuckDBClient is a concrete type, so no
// interface seam can record the Close call, and Close itself is idempotent
// (second call returns nil either way), which would not distinguish the two
// outcomes.
func TestNewEntityManagerWithConfig_Unit_ParquetSourceErrorClosesDuckDBClient(t *testing.T) {
	t.Parallel()

	var built *federated.DuckDBClient
	deps := buildUnitEntityManagerDeps(schemameta.NewMetadataCache())
	deps.newDuckDBClient = func(ctx context.Context, cfg forma.DuckDBConfig) (*federated.DuckDBClient, error) {
		client, err := federated.NewDuckDBClientContext(ctx, cfg)
		if err != nil {
			return nil, err
		}
		built = client
		return client, nil
	}
	deps.newParquetSource = func(context.Context, forma.DuckDBConfig) (federated.ParquetSource, error) {
		return nil, errors.New("simulated s3 client failure")
	}

	config := parquetSourceTestConfig(t)
	config.DuckDB = forma.DuckDBConfig{
		Enabled:        true,
		DBPath:         ":memory:",
		MaxConnections: 1,
		QueryTimeout:   30 * time.Second,
	}

	em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)

	assert.Nil(t, em)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "manifest parquet source",
		"the construction error must surface unchanged; cleanup must not mask it")
	require.NotNil(t, built, "the test needs a real DuckDB client to observe")

	healthErr := built.HealthCheck(context.Background())
	require.Error(t, healthErr, "the duckdb client must be closed before the fatal return")
	assert.Contains(t, healthErr.Error(), "database is closed")
}

func TestNewEntityManagerWithConfig_Unit_InvalidManifestConfigFailsFast(t *testing.T) {
	t.Parallel()

	deps := buildUnitEntityManagerDeps(schemameta.NewMetadataCache())
	// Any I/O reached before configuration validation is a contract breach:
	// a malformed config must be rejected before the factory touches the DB.
	deps.collectTables = func(context.Context, queryPool, string) ([]string, error) {
		t.Fatal("collectTables must not run before manifest config validation")
		return nil, nil
	}
	deps.newParquetSource = func(context.Context, forma.DuckDBConfig) (federated.ParquetSource, error) {
		t.Fatal("parquet source must not be constructed for an invalid manifest config")
		return nil, nil
	}

	config := parquetSourceTestConfig(t)
	config.DuckDB.ManifestPrefix = "manifest" // set without ManifestTemplate

	em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)

	assert.Nil(t, em)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid duckdb manifest configuration")
	var cfgErr *forma.ConfigError
	assert.True(t, errors.As(err, &cfgErr), "expected *forma.ConfigError, got %v", err)
}
