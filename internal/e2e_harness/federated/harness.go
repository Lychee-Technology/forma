// Package federated provides E2E test harness for federated query testing.
// It extends the base e2e_harness with capabilities specific to the three-tier
// architecture: S3 Base files, S3 Delta files, and Postgres Hot Buffer.
package federated

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/declaredindex"
	"github.com/lychee-technology/forma/internal/model"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	forma "github.com/lychee-technology/forma"
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
	Registry  forma.SchemaRegistry
	// DuckCfg is the DuckDB configuration the harness started DuckDB with,
	// kept so engine construction reuses the truthful resource settings.
	DuckCfg forma.DuckDBConfig

	// Postgres connection info for the PostgreSQL catalog DuckDB attaches
	PGHost     string
	PGPort     string
	PGUser     string
	PGPassword string
	PGDatabase string
	PGSSLMode  string

	// S3 connection info shared by DuckDB and direct client access.
	S3Region    string
	S3AccessKey string
	S3SecretKey string

	// Internal clients
	s3Client *s3.Client
	tables   model.StorageTables
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
	// CreatedAt is the row's creation time, written to the parquet
	// ltbase_created_at column and to entity_main.ltbase_created_at. Zero
	// means "same as ChangedAt", the shape every pre-#460 fixture assumed;
	// a fixture that needs creation and version stamps to differ — the
	// created→updated→flushed case — sets it explicitly. See CreationStamp.
	CreatedAt int64
	// NoCreationStamp marks a production-shaped HARD-DELETE tombstone: the
	// row's entity_main record is gone, so the delta export's LEFT JOIN
	// (#173) yields a NULL ltbase_created_at. That NULL is the one
	// legitimate absence of a creation stamp — the production scan guard
	// permits it precisely for this shape (#460) — and it must survive flush
	// and compaction unchanged rather than being back-filled with a version
	// stamp. It is a separate flag rather than a nil CreatedAt because zero
	// already means "unspecified, fall back to ChangedAt", and the two must
	// not be conflated.
	NoCreationStamp bool
	ChangedAt       int64
	DeletedAt       int64
	FlushedAt       int64
}

// CreationStamp returns the row's creation time and whether it has one.
// A hard-delete tombstone has none (see NoCreationStamp); every other
// fixture falls back to ChangedAt, so records that do not distinguish
// creation from version stamp keep reporting the same value on both tiers
// exactly as they did before #460.
func (r TestRecord) CreationStamp() (int64, bool) {
	if r.NoCreationStamp {
		return 0, false
	}
	if r.CreatedAt != 0 {
		return r.CreatedAt, true
	}
	return r.ChangedAt, true
}

// QueryOptions configures federated query execution.
type QueryOptions struct {
	Limit          int
	Offset         int
	Filter         *Filter
	SortBy         string
	SortDesc       bool
	PreferHot      bool
	TradeTimeStart int64
	TradeTimeEnd   int64
	CountOnly      bool
	KeysetCursor   *model.KeysetCursor
}

// Filter defines query filter conditions.
type Filter struct {
	RowID      uuid.UUID
	Conditions map[string]any
}

// QueryResult wraps query results with metadata.
type QueryResult struct {
	Records      []*model.PersistentRecord
	TotalRecords int64
	Plan         *model.ExecutionPlan
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

// ComparisonReport contains data comparison results. Match is false when the
// totals differ, a row ID repeats on either side, a row is on one side only,
// or any compared field of a row differs (see CompareResults).
type ComparisonReport struct {
	Match               bool
	FederatedCount      int64
	PostgresCount       int64
	MissingInFed        []uuid.UUID
	MissingInPG         []uuid.UUID
	DuplicatesInFed     []uuid.UUID
	DuplicatesInPG      []uuid.UUID
	AttributeMismatches []AttributeMismatch
	FederatedChecksum   string
	PostgresChecksum    string
}

// AttributeMismatch describes a single field of a row whose value differs
// between the two results. AttributeName is a system column (schema_id,
// created_at, updated_at, deleted_at), an EAV attribute as
// "eav:<schema_id>/<attr_id>[<array_indices>]" (suffixed "@<row_id>" when it
// names another row), or a typed attribute as "<kind>:<name>", for example
// "text:name" or "float64:version". A value a side does not carry at all is
// reported as absentValue; an EAV key a side repeats, as all its values.
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

// HarnessOption customizes federated test harness construction.
type HarnessOption func(*harnessOptions)

type harnessOptions struct {
	duckThreads        int
	duckMemoryLimitMB  int
	duckMaxConnections int
}

// WithDuckDBResources overrides the DuckDB resource settings the harness
// starts DuckDB with. Zero values keep the harness defaults (4 threads,
// 4096MB memory limit, 4 connections).
func WithDuckDBResources(threads, memoryLimitMB, maxConnections int) HarnessOption {
	return func(o *harnessOptions) {
		o.duckThreads = threads
		o.duckMemoryLimitMB = memoryLimitMB
		o.duckMaxConnections = maxConnections
	}
}

// NewFederatedTestHarness creates a new federated test harness with all
// dependencies. It runs in four phases: bring up (or connect to) Postgres, S3
// and DuckDB; prepare the S3 bucket and scratch directory; assemble the
// harness; create the database schema. A phase that fails stops the
// containers and DuckDB the earlier ones started.
func NewFederatedTestHarness(ctx context.Context, opts ...HarnessOption) (*FederatedTestHarness, error) {
	options := harnessOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	logger, _ := zap.NewDevelopment()
	externalCfg, err := loadExternalFederatedConfigFromEnv()
	if err != nil {
		return nil, fmt.Errorf("load external federated config: %w", err)
	}

	base := &e2e_harness.TestHarness{}
	duckCfg, conn, err := startHarnessInfra(ctx, base, externalCfg, options)
	if err != nil {
		return nil, fmt.Errorf("start harness infrastructure: %w", err)
	}

	// #456: the federated (and benchmark) harnesses exercise the
	// caller-supplied S3ParquetPathTemplate hint, so opt into the feature and
	// scope it to this harness's bucket.
	duckCfg.S3Bucket = conn.bucket
	duckCfg.AllowCallerParquetPaths = true

	s3Client, tmpDir, err := prepareHarnessStorage(ctx, base.S3Endpoint, conn)
	if err != nil {
		cleanupContainers(ctx, base)
		return nil, fmt.Errorf("prepare harness storage: %w", err)
	}

	h := assembleHarness(base, duckCfg, conn, s3Client, tmpDir, logger)
	if err := h.initDatabaseSchema(ctx); err != nil {
		// Best-effort: construction is already failing, and the schema error
		// below is the one the caller must see. No testing.TB exists here, so
		// CleanupOrLog does not apply.
		_ = h.Cleanup(ctx)
		return nil, fmt.Errorf("init database schema: %w", err)
	}

	return h, nil
}

// prepareHarnessStorage creates the S3 client and the harness bucket, and the
// scratch directory Cleanup removes. Bucket creation is best-effort: its
// error, such as the bucket already existing, is ignored.
func prepareHarnessStorage(ctx context.Context, s3Endpoint string, conn harnessConnection) (*s3.Client, string, error) {
	s3Client, err := createS3Client(ctx, s3Endpoint, conn.s3Region, conn.s3AccessKey, conn.s3SecretKey)
	if err != nil {
		return nil, "", fmt.Errorf("create s3 client: %w", err)
	}

	_, _ = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(conn.bucket),
	})

	tmpDir, err := os.MkdirTemp("", "federated-e2e-*")
	if err != nil {
		return nil, "", fmt.Errorf("create temp dir: %w", err)
	}
	return s3Client, tmpDir, nil
}

// assembleHarness builds the harness from the started infrastructure and the
// connection settings it was started with.
func assembleHarness(
	base *e2e_harness.TestHarness,
	duckCfg forma.DuckDBConfig,
	conn harnessConnection,
	s3Client *s3.Client,
	tmpDir string,
	logger *zap.Logger,
) *FederatedTestHarness {
	// Postgres host and port for the PostgreSQL catalog DuckDB attaches.
	pgHost, pgPort := parsePGDSN(base.PGDSN)

	return &FederatedTestHarness{
		TestHarness:   base,
		DuckCfg:       duckCfg,
		SchemaID:      1,
		S3Bucket:      conn.bucket,
		S3Prefix:      conn.prefix,
		PGHost:        pgHost,
		PGPort:        pgPort,
		PGUser:        conn.pgUser,
		PGPassword:    conn.pgPassword,
		PGDatabase:    conn.pgDatabase,
		PGSSLMode:     conn.pgSSLMode,
		S3Region:      conn.s3Region,
		S3AccessKey:   conn.s3AccessKey,
		S3SecretKey:   conn.s3SecretKey,
		s3Client:      s3Client,
		logger:        logger,
		seededRecords: make(map[string][]TestRecord),
		tmpDir:        tmpDir,
		CDCConfig:     harnessCDCConfig(base.S3Endpoint, conn),
		tables: model.StorageTables{
			EntityMain: "entity_main",
			EAVData:    "eav_data",
			ChangeLog:  "change_log",
		},
	}
}

// harnessCDCConfig is the CDC configuration the harness flushes with: the
// harness tables and S3 location, with a threshold lowered for testing.
func harnessCDCConfig(s3Endpoint string, conn harnessConnection) cdc.CDCConfig {
	return cdc.CDCConfig{
		ChangeLogTable:    "change_log",
		EntityMainTable:   "entity_main",
		EAVDataTable:      "eav_data",
		MinRecords:        100, // Lower threshold for testing
		MaxAgeMs:          60000,
		BatchSize:         1000,
		S3Bucket:          conn.bucket,
		S3Prefix:          conn.prefix,
		S3Endpoint:        s3Endpoint,
		S3Region:          conn.s3Region,
		S3UsePath:         true,
		S3AccessKeyID:     conn.s3AccessKey,
		S3SecretAccessKey: conn.s3SecretKey,
	}
}

// initDatabaseSchema creates the required tables for testing.
func (h *FederatedTestHarness) initDatabaseSchema(ctx context.Context) error {
	for _, stmt := range federatedDDL {
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

	if h.PGDB != nil || h.PGContainer != nil {
		if err := h.StopPostgres(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("cleanup errors: %v", errs)
	}
	return nil
}

// cleanupBudget bounds the container stops CleanupOrLog runs.
const cleanupBudget = 2 * time.Minute

// CleanupOrLog releases all resources like Cleanup, reporting any failure
// through tb instead of returning it. Deferred call sites cannot act on the
// error, but it must not be silently discarded (errcheck, #436).
//
// A deferred cleanup often runs after the test's context has expired, most
// often because that expiry is what failed the test. Stopping the containers
// under it failed every stop with "context deadline exceeded" and could
// leave them running (#781), so cleanup keeps ctx's values but not its
// deadline or cancellation, and takes cleanupBudget of its own.
func (h *FederatedTestHarness) CleanupOrLog(ctx context.Context, tb testing.TB) {
	tb.Helper()
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupBudget)
	defer cancel()
	if err := h.Cleanup(cleanupCtx); err != nil {
		tb.Logf("harness cleanup failed: %v", err)
	}
}

// UseRegistry attaches the schema registry the harness seeds and queries
// through, after building the x-forma-indexes indexes its schemas declare on
// entity_main with the function init-db calls (#647). A harness database thus
// carries the same declared indexes an init-db database does.
func (h *FederatedTestHarness) UseRegistry(ctx context.Context, registry forma.SchemaRegistry) error {
	conn, err := pgx.Connect(ctx, h.PGDSN)
	if err != nil {
		return fmt.Errorf("connect to build declared indexes: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := declaredindex.EnsureFromRegistry(ctx, conn, h.tables.EntityMain, registry); err != nil {
		return fmt.Errorf("apply declared indexes on %s: %w", h.tables.EntityMain, err)
	}
	h.Registry = registry
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
