package cdc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awsCreds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dsql/auth"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/manifest"
	"go.uber.org/zap"
)

// generateIAMTokenFn is the function signature we use to generate IAM tokens.
// We wrap the upstream function to keep a stable signature for tests.
var generateIAMTokenFn = func(ctx context.Context, endpoint, region string, creds any) (string, error) {
	var cp aws.CredentialsProvider
	if c, ok := creds.(aws.CredentialsProvider); ok {
		cp = c
	}
	return auth.GenerateDbConnectAuthToken(ctx, endpoint, region, cp)
}

// S3ObjectClient is a minimal interface for copy + delete used by the CDC flusher.
type S3ObjectClient interface {
	CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// S3FullClient extends S3ObjectClient with GetObject and PutObject for manifest operations.
type S3FullClient interface {
	S3ObjectClient
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// RunOnce performs one full pass over schemas and attempts flush where needed.
// Caller may provide an S3ObjectClient; when nil, AWS config will be loaded from
// environment (still respecting cfg.S3Region). Optional schemaRegistry enables
// schema-aware projections in DuckDB export.
func RunOnce(ctx context.Context, cfg CDCConfig, s3Client S3ObjectClient, dryRun bool, logger *zap.Logger, schemaRegistry forma.SchemaRegistry) error {
	if schemaRegistry == nil {
		return fmt.Errorf("schema registry is required for CDC export")
	}
	// Setup AWS credentials and default S3 client.
	region, credProvider, defaultS3Client, err := setupAWSClient(ctx, cfg)
	if err != nil {
		return err
	}

	requireFullS3 := cfg.ManifestTemplate != ""
	activeS3Client, activeFullS3Client, err := resolveS3Clients(s3Client, defaultS3Client, requireFullS3)
	if err != nil {
		return err
	}

	// Setup manifest store if configured
	var manifestStore manifest.Store
	var manifestResolver manifest.PathResolver
	if cfg.ManifestTemplate != "" {
		manifestStore = &manifest.S3Store{
			Client: activeFullS3Client,
			Bucket: cfg.S3Bucket,
		}
		manifestResolver = manifest.PathResolver{
			Prefix:       cfg.ManifestPrefix,
			PathTemplate: cfg.ManifestTemplate,
		}
	}

	// Setup Postgres connection
	db, pgPassword, err := setupPostgresConnection(ctx, cfg, region, credProvider, logger)
	if err != nil {
		return err
	}
	defer db.Close()

	// Setup DuckDB exporter — credentials come from config fields; if not set,
	// NewDuckExporter will use empty strings and DuckDB inherits the environment.
	s3Key := cfg.S3AccessKeyID
	s3Secret := cfg.S3SecretAccessKey
	if s3Key == "" {
		s3Key = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	if s3Secret == "" {
		s3Secret = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
	duck, err := NewDuckExporter(ctx, cfg, s3Key, s3Secret, logger)
	if err != nil {
		return fmt.Errorf("new duck exporter: %w", err)
	}
	defer duck.DB.Close()

	tableName := cfg.ChangeLogTable
	if tableName == "" {
		tableName = "change_log"
	}

	// Get schemas with unflushed rows
	schemaIDs, err := getUnflushedSchemaIDs(ctx, db, tableName)
	if err != nil {
		return err
	}

	// Process each schema
	flushCtx := &schemaFlushContext{
		db:               db,
		duck:             duck,
		s3Client:         activeS3Client,
		cfg:              cfg,
		tableName:        tableName,
		pgPassword:       pgPassword,
		dryRun:           dryRun,
		logger:           logger,
		schemaRegistry:   schemaRegistry,
		manifestStore:    manifestStore,
		manifestResolver: manifestResolver,
	}

	return flushCtx.processSchemas(ctx, schemaIDs)
}

// setupAWSClient initializes the AWS credentials and S3 client.
func setupAWSClient(ctx context.Context, cfg CDCConfig) (string, aws.CredentialsProvider, *s3.Client, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", nil, nil, fmt.Errorf("load aws config: %w", err)
	}
	if cfg.S3Region != "" {
		awsCfg.Region = cfg.S3Region
	}
	if cfg.S3AccessKeyID != "" {
		awsCfg.Credentials = awsCreds.NewStaticCredentialsProvider(cfg.S3AccessKeyID, cfg.S3SecretAccessKey, "")
	} else if envKey := os.Getenv("AWS_ACCESS_KEY_ID"); envKey != "" {
		// Fall back to environment variables when not set in config.
		awsCfg.Credentials = awsCreds.NewStaticCredentialsProvider(envKey, os.Getenv("AWS_SECRET_ACCESS_KEY"), "")
	}

	// Build S3 client options
	var fullClient *s3.Client
	if cfg.S3Endpoint != "" {
		fullClient = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.BaseEndpoint = &cfg.S3Endpoint
			o.UsePathStyle = cfg.S3UsePath
		})
	} else {
		fullClient = s3.NewFromConfig(awsCfg)
	}

	return awsCfg.Region, awsCfg.Credentials, fullClient, nil
}

func resolveS3Clients(
	provided S3ObjectClient,
	fallback S3FullClient,
	requireFull bool,
) (S3ObjectClient, S3FullClient, error) {
	if provided == nil {
		if fallback == nil {
			return nil, nil, fmt.Errorf("s3 client is required")
		}
		return fallback, fallback, nil
	}

	fullClient, ok := provided.(S3FullClient)
	if !ok {
		if requireFull {
			return nil, nil, fmt.Errorf("manifest requires S3FullClient when custom s3 client is provided")
		}
		return provided, nil, nil
	}

	return provided, fullClient, nil
}

// setupPostgresConnection creates a Postgres connection, potentially using IAM auth.
func setupPostgresConnection(ctx context.Context, cfg CDCConfig, region string, credProvider aws.CredentialsProvider, logger *zap.Logger) (*sql.DB, string, error) {
	pgPassword := cfg.PGPassword

	// Try IAM auth token generation when enabled
	if cfg.PGUseIAM {
		endpoint := fmt.Sprintf("%s:%d", cfg.PGHost, cfg.PGPort)
		if token, err := generateIAMTokenFn(ctx, endpoint, region, credProvider); err == nil && token != "" {
			pgPassword = token
			logger.Sugar().Infow("generated IAM auth token for Postgres connection (dsql)")
		} else {
			logger.Sugar().Warnw("failed to generate IAM auth token; falling back to PG_PASSWORD if set", "err", err)
		}
	}

	sslMode := cfg.PGSSLMode
	if sslMode == "" {
		sslMode = DefaultPGSSLMode
	}
	pgConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.PGHost, cfg.PGPort, cfg.PGUser, pgPassword, cfg.PGDB, sslMode)

	db, err := sql.Open("pgx", pgConnStr)
	if err != nil {
		return nil, "", fmt.Errorf("open pg: %w", err)
	}

	return db, pgPassword, nil
}

// getUnflushedSchemaIDs queries for schema IDs with unflushed rows.
func getUnflushedSchemaIDs(ctx context.Context, db *sql.DB, tableName string) ([]int64, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT DISTINCT schema_id FROM %s WHERE flushed_at = 0", sanitizeIdentifier(tableName)))
	if err != nil {
		return nil, fmt.Errorf("query distinct schema ids: %w", err)
	}
	defer rows.Close()

	var schemaIDs []int64
	for rows.Next() {
		var sid int64
		if err := rows.Scan(&sid); err != nil {
			return nil, fmt.Errorf("scan schema id: %w", err)
		}
		schemaIDs = append(schemaIDs, sid)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate schema ids: %w", err)
	}
	return schemaIDs, nil
}

type schemaFlushContext struct {
	db               *sql.DB
	duck             *DuckExporter
	s3Client         S3ObjectClient
	cfg              CDCConfig
	tableName        string
	pgPassword       string
	dryRun           bool
	logger           *zap.Logger
	schemaRegistry   forma.SchemaRegistry
	manifestStore    manifest.Store
	manifestResolver manifest.PathResolver
	acquireLock      func(context.Context, *sql.DB, int16) (bool, error)
	releaseLock      func(context.Context, *sql.DB, int16) error
	executeSingle    func(*flushBatchExecutor, []uuid.UUID) error
	executeInChunks  func(*flushBatchExecutor, []uuid.UUID, int) error
	processSchemaFn  func(context.Context, int16) error
}

func (c *schemaFlushContext) processSchemas(ctx context.Context, schemaIDs []int64) error {
	if len(schemaIDs) == 0 {
		return nil
	}

	processSchema := c.processSchemaFn
	if processSchema == nil {
		processSchema = c.processSchema
	}

	var schemaErrs []error
	for _, sid := range schemaIDs {
		schemaID := int16(sid)
		if err := processSchema(ctx, schemaID); err != nil {
			c.logger.Sugar().Errorw("process schema failed", "schema_id", schemaID, "err", err)
			schemaErrs = append(schemaErrs, fmt.Errorf("schema %d: %w", schemaID, err))
		}
	}
	if len(schemaErrs) > 0 {
		return errors.Join(schemaErrs...)
	}

	return nil
}

// processSchema handles the flush process for a single schema.
func (c *schemaFlushContext) processSchema(ctx context.Context, schemaID int16) error {
	c.logger.Sugar().Infow("processing schema", "schema_id", schemaID)

	acquireLock := c.acquireLock
	if acquireLock == nil {
		acquireLock = AcquireSchemaLock
	}
	releaseLock := c.releaseLock
	if releaseLock == nil {
		releaseLock = ReleaseSchemaLock
	}

	// Try advisory lock
	locked, err := acquireLock(ctx, c.db, schemaID)
	if err != nil {
		return fmt.Errorf("acquire schema lock: %w", err)
	}
	if !locked {
		c.logger.Sugar().Infow("lock not acquired, skipping", "schema_id", schemaID)
		return nil
	}
	defer func() { _ = releaseLock(ctx, c.db, schemaID) }()

	// Check if flush is needed
	cnt, oldest, err := GetChangeLogStats(ctx, c.db, c.tableName, schemaID)
	if err != nil {
		return fmt.Errorf("get changelog stats: %w", err)
	}
	if cnt == 0 {
		c.logger.Sugar().Infow("no unflushed rows", "schema_id", schemaID)
		return nil
	}

	if !shouldFlush(c.cfg, cnt, oldest) {
		c.logger.Sugar().Infow("skip flush: thresholds not met", "schema_id", schemaID, "cnt", cnt, "oldest", oldest)
		return nil
	}

	// Execute flush
	if err := c.executeFlush(ctx, schemaID); err != nil {
		return fmt.Errorf("execute flush: %w", err)
	}
	return nil
}

// shouldFlush determines if flush thresholds are met.
func shouldFlush(cfg CDCConfig, cnt int64, oldest int64) bool {
	nowMs := time.Now().UnixMilli()
	if cfg.MinRecords > 0 && cnt >= int64(cfg.MinRecords) {
		return true
	}
	if oldest > 0 && nowMs-oldest >= cfg.MaxAgeMs {
		return true
	}
	return false
}

type flushBatchExecutor struct {
	db               *sql.DB
	duck             *DuckExporter
	s3Client         S3ObjectClient
	cfg              CDCConfig
	tableName        string
	schemaID         int16
	snapshot         int64
	pgConnForDuck    string
	attrCache        forma.SchemaAttributeCache
	dryRun           bool
	logger           *zap.Logger
	manifestStore    manifest.Store
	manifestResolver manifest.PathResolver
	executeSingle    func(*flushBatchExecutor, []uuid.UUID) error
	executeInChunks  func(*flushBatchExecutor, []uuid.UUID, int) error
	exportSnapshot   func(*DuckExporter, context.Context, CDCConfig, string, string, int16, int64, []uuid.UUID, forma.SchemaAttributeCache) error
}

func (e *flushBatchExecutor) executeBatch(ctx context.Context, batchIDs []uuid.UUID, tmpKey string, finalKey string, batchKind string) error {
	s3TmpPath := fmt.Sprintf("s3://%s/%s", e.cfg.S3Bucket, tmpKey)
	exportSnapshot := e.exportSnapshot
	if exportSnapshot == nil {
		exportSnapshot = func(duck *DuckExporter, ctx context.Context, cfg CDCConfig, pgConnStr string, s3TmpPath string, schemaID int16, snapshotTS int64, rowIDs []uuid.UUID, attrCache forma.SchemaAttributeCache) error {
			return duck.ExportSnapshotToTmp(ctx, cfg, pgConnStr, s3TmpPath, schemaID, snapshotTS, rowIDs, attrCache)
		}
	}

	if err := exportSnapshot(e.duck, ctx, e.cfg, e.pgConnForDuck, s3TmpPath, e.schemaID, e.snapshot, batchIDs, e.attrCache); err != nil {
		return fmt.Errorf("duck export snapshot (%s): %w", batchKind, err)
	}

	if err := CopyTmpToFinal(ctx, e.s3Client, e.cfg.S3Bucket, tmpKey, finalKey, e.logger); err != nil {
		return fmt.Errorf("copy tmp to final (%s): %w", batchKind, err)
	}

	if e.dryRun {
		e.logger.Sugar().Infow("dry-run: skipping mark flushed and manifest update", "schema_id", e.schemaID)
		return nil
	}

	flushedAt := time.Now().UnixMilli()
	updatedIDs, err := MarkFlushedIDsAtSnapshot(ctx, e.db, e.tableName, e.schemaID, batchIDs, e.snapshot, flushedAt)
	if err != nil {
		return fmt.Errorf("mark flushed at snapshot (%s): %w", batchKind, err)
	}

	if len(updatedIDs) == 0 {
		e.logger.Sugar().Infow("flush batch marked zero rows; possible concurrent updates", "schema_id", e.schemaID, "batch_kind", batchKind, "batch_size", len(batchIDs))
		return nil
	}

	if e.manifestStore != nil {
		if err := updateManifest(ctx, e.manifestStore, e.manifestResolver, e.schemaID, finalKey, "delta", updatedIDs, flushedAt, e.logger); err != nil {
			e.logger.Sugar().Errorw("manifest update failed", "err", err)
			// Don't return - the flush succeeded, manifest is non-critical.
		}
	}

	if len(updatedIDs) < len(batchIDs) {
		e.logger.Sugar().Infow("flush batch marked fewer rows than requested; possible concurrent updates", "schema_id", e.schemaID, "batch_kind", batchKind, "rows_flushed", len(updatedIDs), "batch_size", len(batchIDs))
	}

	e.logger.Sugar().Infow("flush batch completed", "schema_id", e.schemaID, "batch_kind", batchKind, "rows_flushed", len(updatedIDs), "batch_size", len(batchIDs), "final_key", finalKey)
	return nil
}

func buildFlushS3Keys(cfg CDCConfig, schemaID int16) (string, string) {
	tmpUUID := uuid.Must(uuid.NewV7()).String()
	finalUUID := uuid.Must(uuid.NewV7()).String()

	tmpKey := BuildTempPath(cfg.S3Prefix, schemaID, tmpUUID)
	finalKey := BuildDeltaPath(cfg.S3Prefix, schemaID, finalUUID)
	return tmpKey, finalKey
}

// executeFlush performs the actual flush operation.
func (c *schemaFlushContext) executeFlush(ctx context.Context, schemaID int16) error {
	ids, snapshot, err := SelectBatchRowIDs(ctx, c.db, c.tableName, schemaID, c.cfg.BatchSize)
	if err != nil {
		return fmt.Errorf("select batch row ids: %w", err)
	}
	if len(ids) == 0 {
		c.logger.Sugar().Infow("no rows in batch", "schema_id", schemaID)
		return nil
	}

	// Build postgres connection string for DuckDB postgres_query.
	sslMode := c.cfg.PGSSLMode
	if sslMode == "" {
		sslMode = DefaultPGSSLMode
	}
	pgConnForDuck := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.cfg.PGHost, c.cfg.PGPort, c.cfg.PGUser, c.pgPassword, c.cfg.PGDB, sslMode)
	pgConnForDuckLoggable := fmt.Sprintf("host=%s port=%d user=%s password=***REDACTED*** dbname=%s sslmode=%s",
		c.cfg.PGHost, c.cfg.PGPort, c.cfg.PGUser, c.cfg.PGDB, sslMode)
	c.logger.Sugar().Infow("export snapshot", "schema_id", schemaID, "snapshot_ts", snapshot, "rows", len(ids), "pgConnForDuck", pgConnForDuckLoggable)

	var attrCache forma.SchemaAttributeCache
	if c.schemaRegistry != nil {
		if _, cache, err := c.schemaRegistry.GetSchemaAttributeCacheByID(schemaID); err != nil {
			c.logger.Sugar().Warnw("schema registry lookup failed, using generic projection", "schema_id", schemaID, "err", err)
		} else {
			attrCache = cache
		}
	}

	executor := &flushBatchExecutor{
		db:               c.db,
		duck:             c.duck,
		s3Client:         c.s3Client,
		cfg:              c.cfg,
		tableName:        c.tableName,
		schemaID:         schemaID,
		snapshot:         snapshot,
		pgConnForDuck:    pgConnForDuck,
		attrCache:        attrCache,
		dryRun:           c.dryRun,
		logger:           c.logger,
		manifestStore:    c.manifestStore,
		manifestResolver: c.manifestResolver,
		executeSingle:    c.executeSingle,
		executeInChunks:  c.executeInChunks,
	}

	executeInChunks := executor.executeInChunks
	if executeInChunks == nil {
		executeInChunks = func(e *flushBatchExecutor, ids []uuid.UUID, max int) error {
			return executeFlushInChunks(ctx, e, ids, max)
		}
	}
	executeSingle := executor.executeSingle
	if executeSingle == nil {
		executeSingle = func(e *flushBatchExecutor, ids []uuid.UUID) error {
			return executeFlushSingle(ctx, e, ids)
		}
	}

	batchIDs := ids
	maxRows := 0
	if c.cfg.MaxBatchBytes > 0 && c.cfg.EstimatedRowBytes > 0 {
		maxRows = int(c.cfg.MaxBatchBytes / int64(c.cfg.EstimatedRowBytes))
	}

	if maxRows > 0 && len(batchIDs) > maxRows {
		c.logger.Sugar().Infow("splitting batch to meet byte target", "schema_id", schemaID, "from_rows", len(batchIDs), "chunk_rows", maxRows)
		return executeInChunks(executor, batchIDs, maxRows)
	}

	return executeSingle(executor, batchIDs)
}

func executeFlushInChunks(
	ctx context.Context,
	executor *flushBatchExecutor,
	batchIDs []uuid.UUID,
	maxRows int,
) error {
	for start := 0; start < len(batchIDs); start += maxRows {
		end := min(start+maxRows, len(batchIDs))
		sub := batchIDs[start:end]

		chunkTmpKey, chunkFinalKey := buildFlushS3Keys(executor.cfg, executor.schemaID)
		if err := executor.executeBatch(ctx, sub, chunkTmpKey, chunkFinalKey, "chunk"); err != nil {
			return err
		}
	}
	return nil
}

func executeFlushSingle(
	ctx context.Context,
	executor *flushBatchExecutor,
	batchIDs []uuid.UUID,
) error {
	tmpKey, finalKey := buildFlushS3Keys(executor.cfg, executor.schemaID)
	return executor.executeBatch(ctx, batchIDs, tmpKey, finalKey, "batch")
}

// updateManifest appends a file entry to the schema's manifest.
func updateManifest(
	ctx context.Context,
	store manifest.Store,
	resolver manifest.PathResolver,
	schemaID int16,
	filePath string,
	tier string,
	rowIDs []uuid.UUID,
	createdAt int64,
	logger *zap.Logger,
) error {
	if store == nil {
		return nil
	}

	manifestPath, err := resolver.Resolve(schemaID)
	if err != nil {
		return fmt.Errorf("resolve manifest path: %w", err)
	}

	// Build file entry
	entry := manifest.FileEntry{
		Tier:       tier,
		Path:       filePath,
		RowCount:   int64(len(rowIDs)),
		CreatedMin: createdAt,
		CreatedMax: createdAt,
	}

	// Set row ID bounds if available
	if len(rowIDs) > 0 {
		rowIDMin, rowIDMax := minMaxRowID(rowIDs)
		entry.RowIDMin = rowIDMin.String()
		entry.RowIDMax = rowIDMax.String()
	}

	if err := manifest.AppendFile(ctx, store, manifestPath, schemaID, entry); err != nil {
		return fmt.Errorf("append to manifest: %w", err)
	}

	logger.Sugar().Infow("manifest updated", "schema_id", schemaID, "manifest_path", manifestPath, "file_path", filePath, "tier", tier)
	return nil
}

func minMaxRowID(rowIDs []uuid.UUID) (uuid.UUID, uuid.UUID) {
	rowIDsSize := len(rowIDs)
	if rowIDsSize == 0 {
		return uuid.Nil, uuid.Nil
	}
	if rowIDsSize == 1 {
		return rowIDs[0], rowIDs[0]
	}
	minID := rowIDs[0]
	minIDTime := minID.Time()
	maxID := minID
	maxIDTime := minIDTime
	for _, id := range rowIDs[1:] {
		idTime := id.Time()
		if idTime < minIDTime {
			minID = id
			minIDTime = idTime
		}
		if idTime > maxIDTime {
			maxID = id
			maxIDTime = idTime
		}
	}
	return minID, maxID
}
