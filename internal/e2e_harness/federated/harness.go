// Package federated provides E2E test harness for federated query testing.
// It extends the base e2e_harness with capabilities specific to the three-tier
// architecture: S3 Base files, S3 Delta files, and Postgres Hot Buffer.
package federated

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/e2e_harness"
	"go.uber.org/zap"
)

// FederatedTestHarness extends the base TestHarness with federated query testing capabilities.
type FederatedTestHarness struct {
	*e2e_harness.TestHarness

	// Configuration
	SchemaID  int16
	S3Bucket  string
	S3Prefix  string
	CDCConfig cdc.CDCConfig

	// Postgres connection info for DuckDB postgres_scan
	PGHost string
	PGPort string

	// Internal clients
	s3Client *s3.Client
	tables   internal.StorageTables
	logger   *zap.Logger

	// Test state tracking
	seededRecords map[string][]TestRecord // tier -> records
	flushedFiles  []string
	tmpDir        string

	// S3 failure simulation
	s3Disabled bool
}

// TestRecord represents a record for testing across all tiers.
type TestRecord struct {
	RowID      uuid.UUID
	SchemaID   int16
	Attributes map[string]any
	ChangedAt  int64
	DeletedAt  int64
	FlushedAt  int64
}

// QueryOptions configures federated query execution.
type QueryOptions struct {
	Limit     int
	Offset    int
	Filter    *Filter
	SortBy    string
	SortDesc  bool
	CountOnly bool
}

// Filter defines query filter conditions.
type Filter struct {
	RowID      uuid.UUID
	Conditions map[string]any
}

// QueryResult wraps query results with metadata.
type QueryResult struct {
	Records      []*internal.PersistentRecord
	TotalRecords int64
	Plan         *internal.ExecutionPlan
	Duration     time.Duration
}

// FlushResult contains CDC flush operation results.
type FlushResult struct {
	Flushed      bool
	RowsFlushed  int64
	FilesCreated []string
	Duration     time.Duration
}

// CompactionResult contains compaction operation results.
type CompactionResult struct {
	FilesCompacted int
	FilesCreated   int
	RowsMerged     int64
	Duration       time.Duration
}

// ComparisonReport contains data comparison results.
type ComparisonReport struct {
	Match               bool
	FederatedCount      int64
	PostgresCount       int64
	MissingInFed        []uuid.UUID
	MissingInPG         []uuid.UUID
	AttributeMismatches []AttributeMismatch
	FederatedChecksum   string
	PostgresChecksum    string
}

// AttributeMismatch describes a single attribute value mismatch.
type AttributeMismatch struct {
	RowID         uuid.UUID
	AttributeName string
	FederatedVal  any
	PostgresVal   any
}

// ParquetMetadata contains parquet file metadata.
type ParquetMetadata struct {
	RowCount   int64
	RowIDMin   string
	RowIDMax   string
	CreatedMin int64
	CreatedMax int64
	SizeBytes  int64
}

// NewFederatedTestHarness creates a new federated test harness with all dependencies.
func NewFederatedTestHarness(ctx context.Context) (*FederatedTestHarness, error) {
	logger, _ := zap.NewDevelopment()
	base := &e2e_harness.TestHarness{}

	// Start all containers (Postgres, S3, DuckDB)
	if err := startContainers(ctx, base); err != nil {
		return nil, err
	}

	// Create S3 client
	s3Client, err := createS3Client(ctx, base.S3Endpoint)
	if err != nil {
		cleanupContainers(ctx, base)
		return nil, fmt.Errorf("create s3 client: %w", err)
	}

	// Create test bucket
	bucket := "test-bucket"
	_, _ = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})

	tmpDir, err := os.MkdirTemp("", "federated-e2e-*")
	if err != nil {
		cleanupContainers(ctx, base)
		return nil, fmt.Errorf("create temp dir: %w", err)
	}

	// Parse Postgres host and port from DSN for DuckDB postgres_scan
	pgHost, pgPort := parsePGDSN(base.PGDSN)

	h := &FederatedTestHarness{
		TestHarness:   base,
		SchemaID:      1,
		S3Bucket:      bucket,
		S3Prefix:      "test-project",
		PGHost:        pgHost,
		PGPort:        pgPort,
		s3Client:      s3Client,
		logger:        logger,
		seededRecords: make(map[string][]TestRecord),
		tmpDir:        tmpDir,
		CDCConfig: cdc.CDCConfig{
			ChangeLogTable:  "change_log",
			EntityMainTable: "entity_main",
			EAVDataTable:    "eav_data",
			MinRecords:      100, // Lower threshold for testing
			MaxAgeMs:        60000,
			BatchSize:       1000,
			S3Bucket:        bucket,
			S3Prefix:        "test-project",
			S3Endpoint:      base.S3Endpoint,
			S3Region:        "us-east-1",
		},
		tables: internal.StorageTables{
			EntityMain: "entity_main",
			EAVData:    "eav_data",
			ChangeLog:  "change_log",
		},
	}

	// Initialize database schema
	if err := h.initDatabaseSchema(ctx); err != nil {
		_ = h.Cleanup(ctx)
		return nil, fmt.Errorf("init database schema: %w", err)
	}

	return h, nil
}

// startContainers starts Postgres, S3, and DuckDB containers.
func startContainers(ctx context.Context, base *e2e_harness.TestHarness) error {
	if _, err := base.StartPostgres(ctx); err != nil {
		return fmt.Errorf("start postgres: %w", err)
	}

	if _, err := base.StartS3(ctx); err != nil {
		_ = base.StopPostgres(ctx)
		return fmt.Errorf("start s3: %w", err)
	}

	duckCfg := forma.DuckDBConfig{
		Enabled:        true,
		DBPath:         ":memory:",
		MemoryLimitMB:  512,
		EnableS3:       true,
		EnableParquet:  true,
		S3Endpoint:     base.S3Endpoint,
		S3AccessKey:    "minio",
		S3SecretKey:    "minio",
		S3Region:       "us-east-1",
		MaxConnections: 4,
		QueryTimeout:   60 * time.Second,
		MaxParallelism: 4,
	}
	if err := base.StartDuckDB(duckCfg); err != nil {
		_ = base.StopS3(ctx)
		_ = base.StopPostgres(ctx)
		return fmt.Errorf("start duckdb: %w", err)
	}

	return nil
}

// createS3Client creates an AWS S3 client configured for MinIO.
func createS3Client(ctx context.Context, endpoint string) (*s3.Client, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minio", "minio", "")),
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

// initDatabaseSchema creates the required tables for testing.
func (h *FederatedTestHarness) initDatabaseSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS schema_registry (
			schema_id SMALLINT PRIMARY KEY,
			schema_name TEXT NOT NULL UNIQUE,
			created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000
		)`,
		`CREATE TABLE IF NOT EXISTS entity_main (
			ltbase_schema_id SMALLINT NOT NULL,
			ltbase_row_id UUID NOT NULL,
			text_01 TEXT,
			text_02 TEXT,
			smallint_01 SMALLINT,
			bigint_01 BIGINT,
			bigint_02 BIGINT,
			double_01 DOUBLE PRECISION,
			uuid_01 UUID,
			ltbase_created_at BIGINT NOT NULL,
			ltbase_updated_at BIGINT NOT NULL,
			ltbase_deleted_at BIGINT,
			ltbase_created_by TEXT,
			ltbase_updated_by TEXT,
			ltbase_deleted_by TEXT,
			PRIMARY KEY (ltbase_schema_id, ltbase_row_id)
		)`,
		`CREATE TABLE IF NOT EXISTS eav_data (
			schema_id SMALLINT NOT NULL,
			row_id UUID NOT NULL,
			attr_id INTEGER NOT NULL,
			value_text TEXT,
			value_numeric DOUBLE PRECISION,
			PRIMARY KEY (schema_id, row_id, attr_id)
		)`,
		`CREATE TABLE IF NOT EXISTS change_log (
			schema_id SMALLINT NOT NULL,
			row_id UUID NOT NULL,
			changed_at BIGINT NOT NULL,
			deleted_at BIGINT DEFAULT 0,
			flushed_at BIGINT DEFAULT 0,
			PRIMARY KEY (schema_id, row_id, flushed_at)
		)`,
		`INSERT INTO schema_registry (schema_id, schema_name) VALUES (1, 'test_entity') ON CONFLICT DO NOTHING`,
	}

	for _, stmt := range stmts {
		if _, err := h.PGDB.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("execute %s: %w", stmt[:50], err)
		}
	}

	return nil
}

// Cleanup releases all resources.
func (h *FederatedTestHarness) Cleanup(ctx context.Context) error {
	var errs []error

	if h.tmpDir != "" {
		os.RemoveAll(h.tmpDir)
	}

	if h.Duck != nil {
		if err := h.StopDuckDB(); err != nil {
			errs = append(errs, err)
		}
	}

	if h.S3Container != nil {
		if err := h.StopS3(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	if h.PGContainer != nil {
		if err := h.StopPostgres(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("cleanup errors: %v", errs)
	}
	return nil
}

// SetupSchema configures the schema for testing.
func (h *FederatedTestHarness) SetupSchema(schemaID int16, schemaName string) error {
	h.SchemaID = schemaID
	_, err := h.PGDB.Exec(
		`INSERT INTO schema_registry (schema_id, schema_name) VALUES ($1, $2) ON CONFLICT (schema_id) DO UPDATE SET schema_name = $2`,
		schemaID, schemaName,
	)
	return err
}
