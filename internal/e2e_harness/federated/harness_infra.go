// Federated harness infrastructure, extracted from harness.go: testcontainer
// lifecycle and the MinIO-configured S3 client (#220), and the choice between
// started containers and the external Postgres/S3 the environment names, with
// the connection settings each one implies (#431).

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

// harnessConnection is the S3 location and the Postgres and S3 credentials
// the harness connects with, which depend on whether it started its own
// containers or was pointed at external services.
type harnessConnection struct {
	bucket      string
	prefix      string
	pgUser      string
	pgPassword  string
	pgDatabase  string
	pgSSLMode   string
	s3Region    string
	s3AccessKey string
	s3SecretKey string
}

// containerConnection is the connection to the containers startContainers
// starts: the testcontainer defaults for Postgres and MinIO.
func containerConnection() harnessConnection {
	return harnessConnection{
		bucket:      "test-bucket",
		prefix:      "test-project",
		pgUser:      "postgres",
		pgPassword:  "password",
		pgDatabase:  "postgres",
		pgSSLMode:   "disable",
		s3Region:    "us-east-1",
		s3AccessKey: "minioadmin",
		s3SecretKey: "minioadmin",
	}
}

// startHarnessInfra brings up Postgres, S3 and DuckDB for the harness, either
// as containers or, when the environment configures it, against the external
// services it names. It returns the DuckDB configuration DuckDB was started
// with and the connection settings that go with the services in use. If a
// later service fails to start, it stops the ones already started, except
// that a connection to an external Postgres is left open.
func startHarnessInfra(ctx context.Context, base *e2e_harness.TestHarness, externalCfg *externalFederatedConfig, opts harnessOptions) (forma.DuckDBConfig, harnessConnection, error) {
	if externalCfg.Enabled {
		return startExternalInfra(ctx, base, externalCfg, opts)
	}
	duckCfg, err := startContainers(ctx, base, opts)
	if err != nil {
		return forma.DuckDBConfig{}, harnessConnection{}, fmt.Errorf("start containers: %w", err)
	}
	return duckCfg, containerConnection(), nil
}

// startExternalInfra connects to the external S3 and, when configured, the
// external Postgres; otherwise it starts a Postgres container. DuckDB always
// runs in-process. The Postgres credentials stay the container defaults
// unless the external Postgres is in use.
func startExternalInfra(ctx context.Context, base *e2e_harness.TestHarness, externalCfg *externalFederatedConfig, opts harnessOptions) (forma.DuckDBConfig, harnessConnection, error) {
	if externalCfg.UseExternalPG {
		if err := base.ConnectPostgres(ctx, externalCfg.PGDSN); err != nil {
			return forma.DuckDBConfig{}, harnessConnection{}, fmt.Errorf("connect external postgres: %w", err)
		}
	} else if _, err := base.StartPostgres(ctx); err != nil {
		return forma.DuckDBConfig{}, harnessConnection{}, fmt.Errorf("start postgres: %w", err)
	}

	base.S3Endpoint = externalCfg.S3Endpoint
	duckCfg, err := startDuckDB(base, externalCfg.S3Endpoint, externalCfg.S3AccessKey, externalCfg.S3SecretKey, externalCfg.S3Region, opts)
	if err != nil {
		cleanupContainers(ctx, base)
		return forma.DuckDBConfig{}, harnessConnection{}, fmt.Errorf("start duckdb: %w", err)
	}

	conn := containerConnection()
	conn.bucket = externalCfg.S3Bucket
	conn.prefix = externalCfg.S3Prefix
	conn.s3Region = externalCfg.S3Region
	conn.s3AccessKey = externalCfg.S3AccessKey
	conn.s3SecretKey = externalCfg.S3SecretKey
	if externalCfg.UseExternalPG {
		conn.pgUser = externalCfg.PGUser
		conn.pgPassword = externalCfg.PGPassword
		conn.pgDatabase = externalCfg.PGDatabase
		conn.pgSSLMode = externalCfg.PGSSLMode
	}
	return duckCfg, conn, nil
}

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
