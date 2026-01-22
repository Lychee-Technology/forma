package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awsCreds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dsql/auth"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
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
// environment (still respecting cfg.S3Region).
func RunOnce(ctx context.Context, cfg CDCConfig, s3Client S3ObjectClient, dryRun bool, logger *zap.Logger) error {
	var region string
	var credProvider aws.CredentialsProvider
	// AWS config + S3 client
	if s3Client == nil {
		awsCfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return fmt.Errorf("load aws config: %w", err)
		}
		if cfg.S3Region != "" {
			awsCfg.Region = cfg.S3Region
		}
		if envKey := os.Getenv("AWS_ACCESS_KEY_ID"); envKey != "" {
			awsCfg.Credentials = awsCreds.NewStaticCredentialsProvider(os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), "")
		}
		region = awsCfg.Region
		credProvider = awsCfg.Credentials
		s3Client = s3.NewFromConfig(awsCfg)
	} else {
		region = cfg.S3Region
		credProvider = aws.AnonymousCredentials{}
	}

	// Build PG connection (sql.DB) - we need a connection for locks and marking
	pgPassword := cfg.PGPassword
	// Try IAM auth token generation when enabled
	if cfg.PGUseIAM {
		endpoint := fmt.Sprintf("%s:%d", cfg.PGHost, cfg.PGPort)
		// Use the wrapped helper so tests can override behavior
		if token, err := generateIAMTokenFn(ctx, endpoint, region, credProvider); err == nil && token != "" {
			pgPassword = token
			logger.Sugar().Infow("generated IAM auth token for Postgres connection (dsql)")
		} else {
			logger.Sugar().Warnw("failed to generate IAM auth token; falling back to PG_PASSWORD if set", "err", err)
		}
	}

	pgConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require",
		cfg.PGHost, cfg.PGPort, cfg.PGUser, pgPassword, cfg.PGDB)
	// open DB for locks/updates
	db, err := sql.Open("postgres", pgConnStr)
	if err != nil {
		return fmt.Errorf("open pg: %w", err)
	}
	defer db.Close()

	// create duck exporter
	duck, err := NewDuckExporter(ctx, cfg, os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), logger)
	if err != nil {
		return fmt.Errorf("new duck exporter: %w", err)
	}
	defer duck.DB.Close()

	tableName := cfg.ChangeLogTable
	if tableName == "" {
		tableName = "change_log"
	}

	// enumerate schemas with unflushed rows
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT DISTINCT schema_id FROM %s WHERE flushed_at = 0", sanitizeIdentifier(tableName)))
	if err != nil {
		return fmt.Errorf("query distinct schema ids: %w", err)
	}
	defer rows.Close()
	var schemaIDs []int64
	for rows.Next() {
		var sid int64
		if err := rows.Scan(&sid); err != nil {
			return fmt.Errorf("scan schema id: %w", err)
		}
		schemaIDs = append(schemaIDs, sid)
	}

	for _, sid := range schemaIDs {
		schemaID := int16(sid)
		logger.Sugar().Infow("processing schema", "schema_id", schemaID)
		// try advisory lock
		locked, err := AcquireSchemaLock(ctx, db, schemaID)
		if err != nil {
			logger.Sugar().Errorw("acquire lock failed", "schema_id", schemaID, "err", err)
			continue
		}
		if !locked {
			logger.Sugar().Infow("lock not acquired, skipping", "schema_id", schemaID)
			continue
		}
		// ensure we release lock
		func() {
			defer ReleaseSchemaLock(ctx, db, schemaID)

			cnt, oldest, err := GetChangeLogStats(ctx, db, tableName, schemaID)
			if err != nil {
				logger.Sugar().Errorw("get changelog stats failed", "err", err)
				return
			}
			if cnt == 0 {
				logger.Sugar().Infow("no unflushed rows", "schema_id", schemaID)
				return
			}
			nowMs := time.Now().UnixMilli()
			should := false
			if cfg.MinRecords > 0 && cnt >= int64(cfg.MinRecords) {
				should = true
			}
			if oldest > 0 && nowMs-oldest >= cfg.MaxAgeMs {
				should = true
			}
			if !should {
				logger.Sugar().Infow("skip flush: thresholds not met", "schema_id", schemaID, "cnt", cnt, "oldest", oldest)
				return
			}
			// select batch
			ids, snapshot, err := SelectBatchRowIDs(ctx, db, tableName, schemaID, cfg.BatchSize)
			if err != nil {
				logger.Sugar().Errorw("select batch failed", "err", err)
				return
			}
			if len(ids) == 0 {
				logger.Sugar().Infow("no rows in batch", "schema_id", schemaID)
				return
			}
			// build tmp and final key
			tmpUUID := uuid.Must(uuid.NewV7()).String()
			finalUUID := uuid.Must(uuid.NewV7()).String()
			tmpKey := strings.TrimSuffix(cfg.S3Prefix, "/") +
				fmt.Sprintf("/delta/%d/_tmp/%s.parquet", schemaID, tmpUUID)
			finalKey := strings.TrimSuffix(cfg.S3Prefix, "/") +
				fmt.Sprintf("/delta/%d/%s.parquet", schemaID, finalUUID)
			s3TmpPath := fmt.Sprintf("s3://%s/%s", cfg.S3Bucket, tmpKey)
			// build pg_conn for duckdb
			pgConnForDuck := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require", cfg.PGHost, cfg.PGPort, cfg.PGUser, pgPassword, cfg.PGDB)
			logger.Sugar().Infow("export snapshot", "schema_id", schemaID, "snapshot_ts", snapshot, "tmp", s3TmpPath)
			if err := duck.ExportSnapshotToTmp(ctx, pgConnForDuck, s3TmpPath, schemaID, snapshot); err != nil {
				logger.Sugar().Errorw("duck export failed", "err", err)
				return
			}
			// copy tmp -> final
			if err := CopyTmpToFinal(ctx, s3Client, cfg.S3Bucket, tmpKey, finalKey, logger); err != nil {
				logger.Sugar().Errorw("s3 copy tmp->final failed", "err", err)
				return
			}
			// mark flushed
			if dryRun {
				logger.Sugar().Infow("dry-run: skipping mark flushed", "schema_id", schemaID)
				return
			}
			flushedAt := time.Now().UnixMilli()
			rowsUpdated, err := MarkFlushed(ctx, db, tableName, schemaID, snapshot, flushedAt)
			if err != nil {
				logger.Sugar().Errorw("mark flushed failed", "err", err)
				return
			}
			logger.Sugar().Infow("flush completed", "schema_id", schemaID, "rows_flushed", rowsUpdated, "final_key", finalKey)
		}()
	}
	return nil
}
