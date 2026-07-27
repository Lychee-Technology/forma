package cdc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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

// loadAWSConfig is the seam for loading the ambient AWS configuration,
// mirroring internal/bootstrap.loadAWSConfig. Every cdc S3 client site —
// setupAWSClient and the Runner's getOrCreateS3Runtime — goes through this
// one seam so credential and region parity holds (#302, #326). Tests swap
// it; production always resolves the real chain.
var loadAWSConfig = config.LoadDefaultConfig

// generateIAMTokenFn is the function signature we use to generate IAM tokens.
// We wrap the upstream function to keep a stable signature for tests.
var generateIAMTokenFn = func(ctx context.Context, endpoint, region string, creds any) (string, error) {
	var cp aws.CredentialsProvider
	if c, ok := creds.(aws.CredentialsProvider); ok {
		cp = c
	}
	return auth.GenerateDbConnectAuthToken(ctx, endpoint, region, cp)
}

// S3ObjectClient is a minimal interface for copy + delete + stat used by the CDC flusher.
type S3ObjectClient interface {
	CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
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

	// DuckDB httpfs credentials follow the same both-halves rule as the SDK
	// client (#326): with no fully-set static pair NewDuckExporter receives
	// empty strings and DuckDB inherits its own environment chain, so the SDK
	// client and httpfs never diverge under a half-set env pair.
	s3Key, s3Secret := resolveStaticS3Credentials(cfg)
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

// setupAWSClient initializes the AWS credentials and S3 client. Credential
// precedence and region handling deliberately mirror
// internal/bootstrap.NewS3Client — the two sites must stay in parity (#302).
func setupAWSClient(ctx context.Context, cfg CDCConfig) (string, aws.CredentialsProvider, *s3.Client, error) {
	var loadOpts []func(*config.LoadOptions) error
	if cfg.S3Region != "" {
		// WithRegion at load time so region-sensitive default-chain
		// resolution (STS, SSO) sees the configured region.
		loadOpts = append(loadOpts, config.WithRegion(cfg.S3Region))
	}
	awsCfg, err := loadAWSConfig(ctx, loadOpts...)
	if err != nil {
		return "", nil, nil, fmt.Errorf("load aws config: %w", err)
	}
	// Both-halves rule and config-wins precedence live in
	// resolveStaticS3Credentials — the shared rule for every cdc credential
	// site (#326). Empty pair leaves the default chain in place.
	if staticKey, staticSecret := resolveStaticS3Credentials(cfg); staticKey != "" {
		awsCfg.Credentials = awsCreds.NewStaticCredentialsProvider(staticKey, staticSecret, "")
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
	pgConnStr := BuildPGDSN(PGDSNParams{Host: cfg.PGHost, Port: cfg.PGPort, User: cfg.PGUser, Password: pgPassword, DB: cfg.PGDB, SSLMode: sslMode})

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
	attrCaches       map[int16]forma.SchemaAttributeCache
	manifestStore    manifest.Store
	manifestResolver manifest.PathResolver
	tryLock          func(context.Context, *sql.DB, int16) (bool, func(), error)
	executeSingle    func(*flushBatchExecutor, []uuid.UUID) error
	executeInChunks  func(*flushBatchExecutor, []uuid.UUID, int) error
	processSchemaFn  func(context.Context, int16) error
}

func (c *schemaFlushContext) processSchemas(ctx context.Context, schemaIDs []int64) error {
	if len(schemaIDs) == 0 {
		return nil
	}

	caches := make(map[int16]forma.SchemaAttributeCache, len(schemaIDs))
	for _, sid := range schemaIDs {
		schemaID := int16(sid)
		cache, err := resolveRequiredAttrCache(c.schemaRegistry, schemaID)
		if err != nil {
			return fmt.Errorf("cdc flush pre-flight: %w", err)
		}
		caches[schemaID] = cache
	}
	c.attrCaches = caches

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

	tryLock := c.tryLock
	if tryLock == nil {
		tryLock = TrySchemaLock
	}
	locked, unlock, err := tryLock(ctx, c.db, schemaID)
	if err != nil {
		return fmt.Errorf("acquire schema lock: %w", err)
	}
	if !locked {
		c.logger.Sugar().Infow("lock not acquired, skipping", "schema_id", schemaID)
		return nil
	}
	defer unlock()

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
	pgConnForDuck := BuildPGDSN(PGDSNParams{Host: c.cfg.PGHost, Port: c.cfg.PGPort, User: c.cfg.PGUser, Password: c.pgPassword, DB: c.cfg.PGDB, SSLMode: sslMode})
	// Redact the real (quoted) wire DSN rather than hand-building a stale preview
	// that no longer matches what DuckDB receives (#290).
	c.logger.Sugar().Infow("export snapshot", "schema_id", schemaID, "snapshot_ts", snapshot, "rows", len(ids), "pgConnForDuck", redactConnStr(pgConnForDuck))

	// Cache was resolved and validated by the processSchemas pre-flight (#193).
	attrCache := c.attrCaches[schemaID]

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
