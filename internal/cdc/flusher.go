package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

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

// RunOnce performs one full pass over schemas and attempts flush where needed.
func RunOnce(ctx context.Context, cfg CDCConfig, dryRun bool, logger *zap.Logger) error {
	// AWS config + S3 client
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load aws config: %w", err)
	}
	// override region if provided
	if cfg.S3Region != "" {
		awsCfg.Region = cfg.S3Region
	}
	if envKey := os.Getenv("AWS_ACCESS_KEY_ID"); envKey != "" {
		// ensure credentials provider from env used explicitly
		awsCfg.Credentials = awsCreds.NewStaticCredentialsProvider(os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), "")
	}
	s3Client := s3.NewFromConfig(awsCfg)

	// Build PG connection (sql.DB) - we need a connection for locks and marking
	pgPassword := cfg.PGPassword
	// Try IAM auth token generation when enabled
	if cfg.PGUseIAM {
		endpoint := fmt.Sprintf("%s:%d", cfg.PGHost, cfg.PGPort)
		// Use the DSQL auth helper to generate a DB connect token
		if token, err := auth.GenerateDbConnectAuthToken(ctx, endpoint, awsCfg.Region, awsCfg.Credentials); err == nil && token != "" {
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

	// enumerate schemas with unflushed rows
	rows, err := db.QueryContext(ctx, "SELECT DISTINCT schema_id FROM change_log WHERE flushed_at = 0")
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

			cnt, oldest, err := GetChangeLogStats(ctx, db, "change_log", schemaID)
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
			if cnt >= cfg.MinRecords {
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
			ids, snapshot, err := SelectBatchRowIDs(ctx, db, "change_log", schemaID, cfg.BatchSize)
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
			rowsUpdated, err := MarkFlushed(ctx, db, "change_log", schemaID, snapshot, flushedAt)
			if err != nil {
				logger.Sugar().Errorw("mark flushed failed", "err", err)
				return
			}
			logger.Sugar().Infow("flush completed", "schema_id", schemaID, "rows_flushed", rowsUpdated, "final_key", finalKey)
		}()
	}
	return nil
}
