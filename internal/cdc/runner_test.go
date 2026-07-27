package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awsCreds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNewRunner_UsesNopLoggerWhenNil(t *testing.T) {
	runner := NewRunner(nil)
	require.NotNil(t, runner)
	require.NotNil(t, runner.logger)
}

func TestRunnerRunOnce_RequiresSchemaRegistry(t *testing.T) {
	runner := NewRunner(zap.NewNop())
	err := runner.RunOnce(context.Background(), CDCConfig{}, nil, false, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "schema registry is required")
}

func TestRunnerCachesS3Runtime(t *testing.T) {
	origNewS3ClientFn := newS3ClientFn
	defer func() {
		newS3ClientFn = origNewS3ClientFn
	}()

	loadCalls := 0
	clientCalls := 0
	stubLoadAWSConfig(t, func(context.Context, ...func(*config.LoadOptions) error) (aws.Config, error) {
		loadCalls++
		return aws.Config{}, nil
	})
	newS3ClientFn = func(cfg aws.Config, endpoint string, usePath bool) *s3.Client {
		clientCalls++
		return &s3.Client{}
	}

	runner := NewRunner(zap.NewNop())
	cfg := CDCConfig{S3Region: "us-east-1"}

	rt1, err := runner.getOrCreateS3Runtime(context.Background(), cfg)
	require.NoError(t, err)
	rt2, err := runner.getOrCreateS3Runtime(context.Background(), cfg)
	require.NoError(t, err)

	require.Same(t, rt1, rt2)
	require.Equal(t, 1, loadCalls)
	require.Equal(t, 1, clientCalls)
}

// TestRunnerGetOrCreateS3Runtime_EnvPairBecomesStaticProvider pins that a
// fully-set env pair still becomes a static provider (#302 rule unchanged).
func TestRunnerGetOrCreateS3Runtime_EnvPairBecomesStaticProvider(t *testing.T) {
	stubLoadAWSConfig(t, func(context.Context, ...func(*config.LoadOptions) error) (aws.Config, error) {
		return aws.Config{Region: "us-west-2"}, nil
	})
	origNewS3ClientFn := newS3ClientFn
	defer func() { newS3ClientFn = origNewS3ClientFn }()
	newS3ClientFn = func(cfg aws.Config, endpoint string, usePath bool) *s3.Client { return &s3.Client{} }

	t.Setenv("AWS_ACCESS_KEY_ID", "env-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-secret")

	runtime, err := NewRunner(zap.NewNop()).getOrCreateS3Runtime(context.Background(), CDCConfig{})
	require.NoError(t, err)
	require.Equal(t, "us-west-2", runtime.region)
	require.Equal(t, "env-key", runtime.accessKeyID)
	require.Equal(t, "env-secret", runtime.secretAccessKey)
	creds := retrieveCreds(t, runtime.credProvider)
	require.Equal(t, "env-key", creds.AccessKeyID)
	require.Equal(t, "env-secret", creds.SecretAccessKey)
}

// TestRunnerGetOrCreateS3Runtime_EnvTripleCarriesSessionToken pins the third
// credential site: the cached runtime must both remember the resolved session
// token and hand it to the static provider, or every long-lived Runner signs
// temporary credentials as if they were permanent keys (#329).
func TestRunnerGetOrCreateS3Runtime_EnvTripleCarriesSessionToken(t *testing.T) {
	stubLoadAWSConfig(t, func(context.Context, ...func(*config.LoadOptions) error) (aws.Config, error) {
		return aws.Config{Region: "us-west-2"}, nil
	})
	origNewS3ClientFn := newS3ClientFn
	defer func() { newS3ClientFn = origNewS3ClientFn }()
	newS3ClientFn = func(cfg aws.Config, endpoint string, usePath bool) *s3.Client { return &s3.Client{} }

	t.Setenv("AWS_ACCESS_KEY_ID", "env-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-secret")
	t.Setenv("AWS_SESSION_TOKEN", "env-token")

	runtime, err := NewRunner(zap.NewNop()).getOrCreateS3Runtime(context.Background(), CDCConfig{})
	require.NoError(t, err)
	require.Equal(t, "env-token", runtime.sessionToken)
	creds := retrieveCreds(t, runtime.credProvider)
	require.Equal(t, "env-key", creds.AccessKeyID)
	require.Equal(t, "env-secret", creds.SecretAccessKey)
	require.Equal(t, "env-token", creds.SessionToken)
}

// TestRunnerS3RuntimeCacheKeyIncludesSessionToken pins that the cached runtime
// is keyed on the whole credential triple. The provider bakes the token in, so
// a rotated AWS_SESSION_TOKEN under an unchanged access-key pair describes a
// different signing identity — key on the pair alone and the Runner keeps
// serving the expired token until the process restarts (#329).
func TestRunnerS3RuntimeCacheKeyIncludesSessionToken(t *testing.T) {
	loadCalls := 0
	stubLoadAWSConfig(t, func(context.Context, ...func(*config.LoadOptions) error) (aws.Config, error) {
		loadCalls++
		return aws.Config{Region: "us-west-2"}, nil
	})
	origNewS3ClientFn := newS3ClientFn
	defer func() { newS3ClientFn = origNewS3ClientFn }()
	newS3ClientFn = func(cfg aws.Config, endpoint string, usePath bool) *s3.Client { return &s3.Client{} }

	t.Setenv("AWS_ACCESS_KEY_ID", "env-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-secret")
	t.Setenv("AWS_SESSION_TOKEN", "token-1")

	runner := NewRunner(zap.NewNop())
	first, err := runner.getOrCreateS3Runtime(context.Background(), CDCConfig{})
	require.NoError(t, err)
	require.Equal(t, "token-1", first.sessionToken)

	// Same access-key pair, rotated token.
	t.Setenv("AWS_SESSION_TOKEN", "token-2")

	second, err := runner.getOrCreateS3Runtime(context.Background(), CDCConfig{})
	require.NoError(t, err)
	require.NotSame(t, first, second)
	require.Equal(t, 2, loadCalls)
	require.Equal(t, "token-2", second.sessionToken)
	creds := retrieveCreds(t, second.credProvider)
	require.Equal(t, "token-2", creds.SessionToken)
}

func TestRunnerCachesDuckExporter(t *testing.T) {
	origNewDuckExporterFn := newDuckExporterFn
	defer func() {
		newDuckExporterFn = origNewDuckExporterFn
	}()

	createCalls := 0
	newDuckExporterFn = func(ctx context.Context, cfg CDCConfig, s3AccessKey, s3Secret, s3SessionToken string, logger *zap.Logger) (*DuckExporter, error) {
		createCalls++
		return &DuckExporter{Logger: logger}, nil
	}

	runner := NewRunner(zap.NewNop())
	s3Runtime := &cachedS3Runtime{
		region:          "us-east-1",
		accessKeyID:     "key",
		secretAccessKey: "secret",
	}
	cfg := CDCConfig{
		DuckDBPath:   ":memory:",
		DuckThreads:  4,
		DuckMemLimit: "4GB",
		S3Region:     "us-east-1",
		S3UseSSL:     true,
	}

	exporter1, err := runner.getOrCreateDuckExporter(context.Background(), cfg, s3Runtime)
	require.NoError(t, err)
	exporter2, err := runner.getOrCreateDuckExporter(context.Background(), cfg, s3Runtime)
	require.NoError(t, err)

	require.Same(t, exporter1, exporter2)
	require.Equal(t, 1, createCalls)
}

// TestRunnerDuckExporterCacheKeyIgnoresChainResolvedRegion pins that the
// exporter cache key describes the exporter that was actually built. The
// exporter is configured from the raw cfg — an empty cfg.S3Region suppresses
// SET s3_region entirely — so two runs whose only difference is the region the
// AWS default chain happened to resolve produce byte-identical exporters. Key
// on the chain region and the cache claims a distinction the exporter never
// made, building (and leaking) a second identical DuckDB instance (#329).
func TestRunnerDuckExporterCacheKeyIgnoresChainResolvedRegion(t *testing.T) {
	origNewDuckExporterFn := newDuckExporterFn
	defer func() {
		newDuckExporterFn = origNewDuckExporterFn
	}()

	createCalls := 0
	newDuckExporterFn = func(ctx context.Context, cfg CDCConfig, s3AccessKey, s3Secret, s3SessionToken string, logger *zap.Logger) (*DuckExporter, error) {
		createCalls++
		return &DuckExporter{Logger: logger}, nil
	}

	runner := NewRunner(zap.NewNop())
	// cfg.S3Region is empty: the exporter never issues SET s3_region, so both
	// runtimes below configure the exact same exporter.
	cfg := CDCConfig{
		DuckDBPath:   ":memory:",
		DuckThreads:  4,
		DuckMemLimit: "4GB",
		S3UseSSL:     true,
	}
	runtimeEast := &cachedS3Runtime{
		region:          "us-east-1",
		accessKeyID:     "key",
		secretAccessKey: "secret",
	}
	runtimeWest := &cachedS3Runtime{
		region:          "eu-west-1",
		accessKeyID:     "key",
		secretAccessKey: "secret",
	}

	exporter1, err := runner.getOrCreateDuckExporter(context.Background(), cfg, runtimeEast)
	require.NoError(t, err)
	exporter2, err := runner.getOrCreateDuckExporter(context.Background(), cfg, runtimeWest)
	require.NoError(t, err)

	require.Same(t, exporter1, exporter2)
	require.Equal(t, 1, createCalls)
}

// TestRunnerDuckExporterCacheKeyIncludesSessionToken pins the other half of the
// same rule: the exporter bakes the token into SET s3_session_token at
// construction, so two runtimes differing only in their token configure
// different exporters and must not share a cache slot (#329).
func TestRunnerDuckExporterCacheKeyIncludesSessionToken(t *testing.T) {
	origNewDuckExporterFn := newDuckExporterFn
	defer func() {
		newDuckExporterFn = origNewDuckExporterFn
	}()

	createCalls := 0
	var seenTokens []string
	newDuckExporterFn = func(ctx context.Context, cfg CDCConfig, s3AccessKey, s3Secret, s3SessionToken string, logger *zap.Logger) (*DuckExporter, error) {
		createCalls++
		seenTokens = append(seenTokens, s3SessionToken)
		return &DuckExporter{Logger: logger}, nil
	}

	runner := NewRunner(zap.NewNop())
	cfg := CDCConfig{
		DuckDBPath:   ":memory:",
		DuckThreads:  4,
		DuckMemLimit: "4GB",
		S3Region:     "us-east-1",
		S3UseSSL:     true,
	}
	runtimeOld := &cachedS3Runtime{
		region:          "us-east-1",
		accessKeyID:     "key",
		secretAccessKey: "secret",
		sessionToken:    "token-1",
	}
	runtimeRotated := &cachedS3Runtime{
		region:          "us-east-1",
		accessKeyID:     "key",
		secretAccessKey: "secret",
		sessionToken:    "token-2",
	}

	exporter1, err := runner.getOrCreateDuckExporter(context.Background(), cfg, runtimeOld)
	require.NoError(t, err)
	exporter2, err := runner.getOrCreateDuckExporter(context.Background(), cfg, runtimeRotated)
	require.NoError(t, err)

	require.NotSame(t, exporter1, exporter2)
	require.Equal(t, 2, createCalls)
	require.Equal(t, []string{"token-1", "token-2"}, seenTokens)
}

func TestRunnerClose_ClearsCachesAndClosesExporters(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)

	runner := NewRunner(zap.NewNop())
	runner.s3Runtimes[s3RuntimeKey{region: "us-east-1"}] = &cachedS3Runtime{region: "us-east-1"}
	runner.duckExporters[duckExporterKey{dbPath: ":memory:"}] = &DuckExporter{
		DB:     db,
		Logger: zap.NewNop(),
	}

	require.NoError(t, runner.Close())
	require.Empty(t, runner.s3Runtimes)
	require.Empty(t, runner.duckExporters)
	require.Error(t, db.PingContext(context.Background()))
	require.NoError(t, runner.Close())
}

// TestRunnerGetOrCreateS3Runtime_EnvHalfPairPreservesDefaultChain mirrors
// setupAWSClient and internal/bootstrap: a lone AWS_ACCESS_KEY_ID must fall
// through to the default chain, never build an empty-secret static provider
// (#302 rule, third site #326).
func TestRunnerGetOrCreateS3Runtime_EnvHalfPairPreservesDefaultChain(t *testing.T) {
	chain := awsCreds.NewStaticCredentialsProvider("chain-key", "chain-secret", "")
	stubLoadAWSConfig(t, func(context.Context, ...func(*config.LoadOptions) error) (aws.Config, error) {
		return aws.Config{Region: "us-east-1", Credentials: chain}, nil
	})
	origNewS3ClientFn := newS3ClientFn
	defer func() { newS3ClientFn = origNewS3ClientFn }()
	newS3ClientFn = func(cfg aws.Config, endpoint string, usePath bool) *s3.Client { return &s3.Client{} }

	t.Setenv("AWS_ACCESS_KEY_ID", "env-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")

	runtime, err := NewRunner(zap.NewNop()).getOrCreateS3Runtime(context.Background(), CDCConfig{})
	require.NoError(t, err)
	require.Empty(t, runtime.accessKeyID)
	require.Empty(t, runtime.secretAccessKey)
	creds := retrieveCreds(t, runtime.credProvider)
	require.Equal(t, "chain-key", creds.AccessKeyID)
	require.Equal(t, "chain-secret", creds.SecretAccessKey)
}

// TestRunnerGetOrCreateS3Runtime_RegionPassedAtLoad pins WithRegion at load
// (not a post-load overwrite), mirroring setupAWSClient and
// internal/bootstrap (#302, third site #326).
func TestRunnerGetOrCreateS3Runtime_RegionPassedAtLoad(t *testing.T) {
	var loadedRegion string
	stubLoadAWSConfig(t, func(ctx context.Context, optFns ...func(*config.LoadOptions) error) (aws.Config, error) {
		lo := &config.LoadOptions{}
		for _, fn := range optFns {
			if err := fn(lo); err != nil {
				return aws.Config{}, fmt.Errorf("apply AWS load option: %w", err)
			}
		}
		loadedRegion = lo.Region
		return aws.Config{Region: lo.Region}, nil
	})
	origNewS3ClientFn := newS3ClientFn
	defer func() { newS3ClientFn = origNewS3ClientFn }()
	newS3ClientFn = func(cfg aws.Config, endpoint string, usePath bool) *s3.Client { return &s3.Client{} }

	runtime, err := NewRunner(zap.NewNop()).getOrCreateS3Runtime(context.Background(), CDCConfig{S3Region: "eu-central-1"})
	require.NoError(t, err)
	require.Equal(t, "eu-central-1", loadedRegion)
	require.Equal(t, "eu-central-1", runtime.region)
}

// TestRunnerGetOrCreateS3Runtime_UnconfiguredRegionPreservesChainRegion pins
// that an unset cfg.S3Region keeps whatever the default chain resolved,
// instead of the former unconditional "us-east-1" clobber (#326).
func TestRunnerGetOrCreateS3Runtime_UnconfiguredRegionPreservesChainRegion(t *testing.T) {
	stubLoadAWSConfig(t, func(context.Context, ...func(*config.LoadOptions) error) (aws.Config, error) {
		return aws.Config{Region: "ap-southeast-2"}, nil
	})
	origNewS3ClientFn := newS3ClientFn
	defer func() { newS3ClientFn = origNewS3ClientFn }()
	newS3ClientFn = func(cfg aws.Config, endpoint string, usePath bool) *s3.Client { return &s3.Client{} }

	runtime, err := NewRunner(zap.NewNop()).getOrCreateS3Runtime(context.Background(), CDCConfig{})
	require.NoError(t, err)
	require.Equal(t, "ap-southeast-2", runtime.region)
}
