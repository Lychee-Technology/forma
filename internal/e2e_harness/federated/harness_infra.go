// Federated harness container/client infrastructure, extracted from
// harness.go (#220): testcontainer lifecycle and the MinIO-configured S3
// client. Pure move, no behavior change.

package federated

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/e2e_harness"
)

// startContainers starts Postgres, S3, and DuckDB containers. It returns the
// DuckDB configuration DuckDB was started with.
func startContainers(ctx context.Context, base *e2e_harness.TestHarness, opts harnessOptions) (forma.DuckDBConfig, error) {
	if _, err := base.StartPostgres(ctx); err != nil {
		return forma.DuckDBConfig{}, fmt.Errorf("start postgres: %w", err)
	}

	if _, err := base.StartS3(ctx); err != nil {
		_ = base.StopPostgres(ctx)
		return forma.DuckDBConfig{}, fmt.Errorf("start s3: %w", err)
	}

	duckCfg, err := startDuckDB(base, base.S3Endpoint, "minioadmin", "minioadmin", "us-east-1", opts)
	if err != nil {
		_ = base.StopS3(ctx)
		_ = base.StopPostgres(ctx)
		return forma.DuckDBConfig{}, fmt.Errorf("start duckdb: %w", err)
	}

	return duckCfg, nil
}

// startDuckDB starts DuckDB with the harness defaults, applying any resource
// overrides from opts, and returns the configuration it was started with.
func startDuckDB(base *e2e_harness.TestHarness, s3Endpoint, s3AccessKey, s3SecretKey, s3Region string, opts harnessOptions) (forma.DuckDBConfig, error) {
	duckCfg := forma.DuckDBConfig{
		Enabled: true,
		DBPath:  ":memory:",
		// 4096 matches the production default; the old 512 never actually
		// governed federated queries because the query template re-set the
		// instance to 4GB on every execution before #104 removed that PRAGMA.
		MemoryLimitMB:  4096,
		EnableS3:       true,
		EnableParquet:  true,
		S3Endpoint:     s3Endpoint,
		S3AccessKey:    s3AccessKey,
		S3SecretKey:    s3SecretKey,
		S3Region:       s3Region,
		MaxConnections: 4,
		QueryTimeout:   60 * time.Second,
		MaxParallelism: 4,
	}
	if opts.duckThreads > 0 {
		duckCfg.MaxParallelism = opts.duckThreads
	}
	if opts.duckMemoryLimitMB > 0 {
		duckCfg.MemoryLimitMB = opts.duckMemoryLimitMB
	}
	if opts.duckMaxConnections > 0 {
		duckCfg.MaxConnections = opts.duckMaxConnections
	}
	return duckCfg, base.StartDuckDB(duckCfg)
}

// createS3Client creates an AWS S3 client configured for MinIO.
func createS3Client(ctx context.Context, endpoint, region, accessKey, secretKey string) (*s3.Client, error) {
	if region == "" {
		region = "us-east-1"
	}
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	}), nil
}

// cleanupContainers stops all containers in reverse order.
func cleanupContainers(ctx context.Context, base *e2e_harness.TestHarness) {
	if base.Duck != nil {
		_ = base.StopDuckDB()
	}
	if base.S3Container != nil {
		_ = base.StopS3(ctx)
	}
	if base.PGContainer != nil {
		_ = base.StopPostgres(ctx)
	}
}
