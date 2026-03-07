package cdc

import (
	"context"
	"database/sql"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
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
	origLoadAWSConfigFn := loadAWSConfigFn
	origNewS3ClientFn := newS3ClientFn
	defer func() {
		loadAWSConfigFn = origLoadAWSConfigFn
		newS3ClientFn = origNewS3ClientFn
	}()

	loadCalls := 0
	clientCalls := 0
	loadAWSConfigFn = func(ctx context.Context) (aws.Config, error) {
		loadCalls++
		return aws.Config{}, nil
	}
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

func TestRunnerGetOrCreateS3Runtime_UsesDefaultRegionAndEnvCredentials(t *testing.T) {
	origLoadAWSConfigFn := loadAWSConfigFn
	origNewS3ClientFn := newS3ClientFn
	defer func() {
		loadAWSConfigFn = origLoadAWSConfigFn
		newS3ClientFn = origNewS3ClientFn
	}()

	t.Setenv("AWS_ACCESS_KEY_ID", "env-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-secret")

	loadAWSConfigFn = func(ctx context.Context) (aws.Config, error) {
		return aws.Config{}, nil
	}
	newS3ClientFn = func(cfg aws.Config, endpoint string, usePath bool) *s3.Client {
		return &s3.Client{}
	}

	runner := NewRunner(zap.NewNop())
	runtime, err := runner.getOrCreateS3Runtime(context.Background(), CDCConfig{})
	require.NoError(t, err)
	require.Equal(t, "us-east-1", runtime.region)
	require.Equal(t, "env-key", runtime.accessKeyID)
	require.Equal(t, "env-secret", runtime.secretAccessKey)
}

func TestRunnerCachesDuckExporter(t *testing.T) {
	origNewDuckExporterFn := newDuckExporterFn
	defer func() {
		newDuckExporterFn = origNewDuckExporterFn
	}()

	createCalls := 0
	newDuckExporterFn = func(ctx context.Context, cfg CDCConfig, s3AccessKey, s3Secret string, logger *zap.Logger) (*DuckExporter, error) {
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
