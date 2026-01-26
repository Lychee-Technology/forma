package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awsCreds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dsql/auth"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
	"os"
)

// generateIAMTokenFn is the function signature we use to generate IAM tokens.
// We wrap the upstream function to keep a stable signature for tests.
var generateIAMTokenFn = func(ctx context.Context, endpoint, region string, creds interface{}) (string, error) {
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

// RunOnce performs one full pass over schemas and attempts flush where needed.
// Caller may provide an S3ObjectClient; when nil, AWS config will be loaded from
// environment (still respecting cfg.S3Region). Optional schemaRegistry enables
// schema-aware projections in DuckDB export.
func RunOnce(ctx context.Context, cfg CDCConfig, s3Client S3ObjectClient, dryRun bool, logger *zap.Logger, schemaRegistry forma.SchemaRegistry) error {
	if schemaRegistry == nil {
		return fmt.Errorf("schema registry is required for CDC export")
	}
	// Setup AWS credentials and S3 client
	region, credProvider, s3Client, err := setupAWSClient(ctx, cfg, s3Client)
	if err != nil {
		return err
	}

	// Setup Postgres connection
	db, pgPassword, err := setupPostgresConnection(ctx, cfg, region, credProvider, logger)
	if err != nil {
		return err
	}
	defer db.Close()

	// Setup DuckDB exporter
	duck, err := NewDuckExporter(ctx, cfg, os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), logger)
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
	for _, sid := range schemaIDs {
		schemaID := int16(sid)
		processSchema(ctx, db, duck, s3Client, cfg, tableName, schemaID, pgPassword, dryRun, logger, schemaRegistry)
	}

	return nil
}

// setupAWSClient initializes the AWS credentials and S3 client.
func setupAWSClient(ctx context.Context, cfg CDCConfig, s3Client S3ObjectClient) (string, aws.CredentialsProvider, S3ObjectClient, error) {
	if s3Client != nil {
		return cfg.S3Region, aws.AnonymousCredentials{}, s3Client, nil
	}

	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", nil, nil, fmt.Errorf("load aws config: %w", err)
	}
	if cfg.S3Region != "" {
		awsCfg.Region = cfg.S3Region
	}
	if envKey := os.Getenv("AWS_ACCESS_KEY_ID"); envKey != "" {
		awsCfg.Credentials = awsCreds.NewStaticCredentialsProvider(os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), "")
	}
	return awsCfg.Region, awsCfg.Credentials, s3.NewFromConfig(awsCfg), nil
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

	db, err := sql.Open("postgres", pgConnStr)
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
	return schemaIDs, nil
}

// processSchema handles the flush process for a single schema.
func processSchema(
	ctx context.Context,
	db *sql.DB,
	duck *DuckExporter,
	s3Client S3ObjectClient,
	cfg CDCConfig,
	tableName string,
	schemaID int16,
	pgPassword string,
	dryRun bool,
	logger *zap.Logger,
	schemaRegistry forma.SchemaRegistry,
) {
	logger.Sugar().Infow("processing schema", "schema_id", schemaID)

	// Try advisory lock
	locked, err := AcquireSchemaLock(ctx, db, schemaID)
	if err != nil {
		logger.Sugar().Errorw("acquire lock failed", "schema_id", schemaID, "err", err)
		return
	}
	if !locked {
		logger.Sugar().Infow("lock not acquired, skipping", "schema_id", schemaID)
		return
	}
	defer ReleaseSchemaLock(ctx, db, schemaID)

	// Check if flush is needed
	cnt, oldest, err := GetChangeLogStats(ctx, db, tableName, schemaID)
	if err != nil {
		logger.Sugar().Errorw("get changelog stats failed", "err", err)
		return
	}
	if cnt == 0 {
		logger.Sugar().Infow("no unflushed rows", "schema_id", schemaID)
		return
	}

	if !shouldFlush(cfg, cnt, oldest) {
		logger.Sugar().Infow("skip flush: thresholds not met", "schema_id", schemaID, "cnt", cnt, "oldest", oldest)
		return
	}

	// Execute flush
	executeFlush(ctx, db, duck, s3Client, cfg, tableName, schemaID, pgPassword, dryRun, logger, schemaRegistry)
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
func executeFlush(
	ctx context.Context,
	db *sql.DB,
	duck *DuckExporter,
	s3Client S3ObjectClient,
	cfg CDCConfig,
	tableName string,
	schemaID int16,
	pgPassword string,
	dryRun bool,
	logger *zap.Logger,
	schemaRegistry forma.SchemaRegistry,
) {
	// Select batch
	ids, snapshot, err := SelectBatchRowIDs(ctx, db, tableName, schemaID, cfg.BatchSize)
	if err != nil {
		logger.Sugar().Errorw("select batch failed", "err", err)
		return
	}
	if len(ids) == 0 {
		logger.Sugar().Infow("no rows in batch", "schema_id", schemaID)
		return
	}

	// Build paths
	tmpUUID := uuid.Must(uuid.NewV7()).String()
	finalUUID := uuid.Must(uuid.NewV7()).String()
	tmpKey := BuildTempPath(cfg.S3Prefix, schemaID, tmpUUID)
	finalKey := BuildDeltaPath(cfg.S3Prefix, schemaID, finalUUID)
	s3TmpPath := fmt.Sprintf("s3://%s/%s", cfg.S3Bucket, tmpKey)

	// Export snapshot
	sslMode := cfg.PGSSLMode
	if sslMode == "" {
		sslMode = DefaultPGSSLMode
	}
	pgConnForDuck := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.PGHost, cfg.PGPort, cfg.PGUser, pgPassword, cfg.PGDB, sslMode)
	logger.Sugar().Infow("export snapshot", "schema_id", schemaID, "snapshot_ts", snapshot, "tmp", s3TmpPath, "pgConnForDuck", pgConnForDuck)

	var attrCache forma.SchemaAttributeCache
	if schemaRegistry != nil {
		if _, cache, err := schemaRegistry.GetSchemaAttributeCacheByID(schemaID); err != nil {
			logger.Sugar().Warnw("schema registry lookup failed, using generic projection", "schema_id", schemaID, "err", err)
		} else {
			attrCache = cache
		}
	}

	batchIDs := ids
	maxRows := 0
	if cfg.MaxBatchBytes > 0 && cfg.EstimatedRowBytes > 0 {
		maxRows = int(cfg.MaxBatchBytes / int64(cfg.EstimatedRowBytes))
	}

	if maxRows > 0 && len(batchIDs) > maxRows {
		logger.Sugar().Infow("splitting batch to meet byte target", "schema_id", schemaID, "from_rows", len(batchIDs), "chunk_rows", maxRows)
		for start := 0; start < len(batchIDs); start += maxRows {
			end := start + maxRows
			if end > len(batchIDs) {
				end = len(batchIDs)
			}
			sub := batchIDs[start:end]
			if err := duck.ExportSnapshotToTmp(ctx, pgConnForDuck, s3TmpPath, schemaID, snapshot, sub, attrCache); err != nil {
				logger.Sugar().Errorw("duck export failed", "err", err)
				return
			}
			if err := CopyTmpToFinal(ctx, s3Client, cfg.S3Bucket, tmpKey, finalKey, logger); err != nil {
				logger.Sugar().Errorw("s3 copy tmp->final failed", "err", err)
				return
			}
			if dryRun {
				logger.Sugar().Infow("dry-run: skipping mark flushed", "schema_id", schemaID)
				return
			}
			flushedAt := time.Now().UnixMilli()
			rowsUpdated, err := MarkFlushedIDs(ctx, db, tableName, schemaID, sub, flushedAt)
			if err != nil {
				logger.Sugar().Errorw("mark flushed failed", "err", err)
				return
			}
			logger.Sugar().Infow("flush chunk completed", "schema_id", schemaID, "rows_flushed", rowsUpdated, "chunk_size", len(sub))
		}
		return
	}

	if err := duck.ExportSnapshotToTmp(ctx, pgConnForDuck, s3TmpPath, schemaID, snapshot, batchIDs, attrCache); err != nil {
		logger.Sugar().Errorw("duck export failed", "err", err)
		return
	}

	// Copy tmp -> final
	if err := CopyTmpToFinal(ctx, s3Client, cfg.S3Bucket, tmpKey, finalKey, logger); err != nil {
		logger.Sugar().Errorw("s3 copy tmp->final failed", "err", err)
		return
	}

	// Mark flushed
	if dryRun {
		logger.Sugar().Infow("dry-run: skipping mark flushed", "schema_id", schemaID)
		return
	}

	flushedAt := time.Now().UnixMilli()
	rowsUpdated, err := MarkFlushedIDs(ctx, db, tableName, schemaID, batchIDs, flushedAt)
	if err != nil {
		logger.Sugar().Errorw("mark flushed failed", "err", err)
		return
	}
	logger.Sugar().Infow("flush completed", "schema_id", schemaID, "rows_flushed", rowsUpdated, "final_key", finalKey)

	// Suppress unused variable warning
	_ = ids
}
