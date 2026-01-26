package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/cdc"
	"go.uber.org/zap"
)

func runCDCFlush(args []string) error {
	fs := flag.NewFlagSet("cdc-flush", flag.ExitOnError)

	// Database connection
	pgHost := fs.String("pg-host", "localhost", "PostgreSQL host")
	pgPort := fs.Int("pg-port", 5432, "PostgreSQL port")
	pgUser := fs.String("pg-user", "postgres", "PostgreSQL user")
	pgPassword := fs.String("pg-password", "", "PostgreSQL password (or set PGPASSWORD env)")
	pgDB := fs.String("pg-db", "forma", "PostgreSQL database")
	pgUseIAM := fs.Bool("pg-use-iam", false, "Use IAM authentication for PostgreSQL")
	pgSSLMode := fs.String("pg-ssl-mode", getenvDefault("PG_SSL_MODE", "require"), "PostgreSQL sslmode")

	// Change log settings
	changeLogTable := fs.String("change-log-table", "change_log", "Change log table name")
	entityMainTable := fs.String("entity-main-table", "entity_main", "Entity main table name")
	eavDataTable := fs.String("eav-table", "eav_data", "EAV data table name")
	minRecords := fs.Int("min-records", 20000, "Minimum records before flush")
	maxAgeMs := fs.Int64("max-age-ms", 3600000, "Maximum age in ms before flush")
	batchSize := fs.Int("batch-size", 10000, "Maximum batch size per flush")
	estimatedRowBytes := fs.Int("estimated-row-bytes", 0, "Estimated bytes per row for batch sizing (0 to use default)")
	maxBatchBytes := fs.Int64("max-batch-bytes", 0, "Max batch size in bytes (0 to use default)")

	// DuckDB settings
	duckDBPath := fs.String("duckdb-path", "", "DuckDB path (empty for :memory:)")
	duckThreads := fs.Int("duck-threads", 4, "DuckDB thread count")
	duckMemLimit := fs.String("duck-mem-limit", "4GB", "DuckDB memory limit")
	queryTimeout := fs.Duration("query-timeout", 5*time.Minute, "Query timeout")

	// S3 settings
	s3Bucket := fs.String("s3-bucket", "", "S3 bucket for parquet files (required)")
	s3Prefix := fs.String("s3-prefix", "delta", "S3 prefix for delta files")
	s3Endpoint := fs.String("s3-endpoint", "", "S3 endpoint (for MinIO)")
	s3Region := fs.String("s3-region", "us-east-1", "S3 region")
	s3UseSSL := fs.Bool("s3-use-ssl", true, "Use SSL for S3")
	s3UsePath := fs.Bool("s3-use-path", false, "Use path-style S3 addressing")

	// Compression
	parquetCompression := fs.String("parquet-compression", "zstd", "Parquet compression codec")
	parquetCompressionLevel := fs.Int("parquet-compression-level", 3, "Parquet compression level")

	// Schema registry (optional)
	schemaRegistryTable := fs.String("schema-registry-table", "", "Schema registry table name (optional)")
	schemaDir := fs.String("schema-dir", "", "Directory with *_attributes.json files (required if schema-registry-table is set)")

	// Control
	dryRun := fs.Bool("dry-run", false, "Dry run mode (no actual flush)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *s3Bucket == "" {
		return fmt.Errorf("--s3-bucket is required")
	}
	if (*schemaRegistryTable == "") != (*schemaDir == "") {
		return fmt.Errorf("both --schema-registry-table and --schema-dir are required together")
	}

	// Get password from env if not provided
	password := *pgPassword
	if password == "" {
		password = os.Getenv("PGPASSWORD")
	}

	cfg := cdc.CDCConfig{
		ChangeLogTable:          *changeLogTable,
		EntityMainTable:         *entityMainTable,
		EAVDataTable:            *eavDataTable,
		MinRecords:              *minRecords,
		MaxAgeMs:                *maxAgeMs,
		BatchSize:               *batchSize,
		EstimatedRowBytes:       *estimatedRowBytes,
		MaxBatchBytes:           *maxBatchBytes,
		PGHost:                  *pgHost,
		PGPort:                  *pgPort,
		PGUser:                  *pgUser,
		PGPassword:              password,
		PGDB:                    *pgDB,
		PGUseIAM:                *pgUseIAM,
		PGSSLMode:               *pgSSLMode,
		DuckDBPath:              *duckDBPath,
		DuckThreads:             *duckThreads,
		DuckMemLimit:            *duckMemLimit,
		QueryTimeout:            *queryTimeout,
		ParquetCompression:      *parquetCompression,
		ParquetCompressionLevel: *parquetCompressionLevel,
		S3Bucket:                *s3Bucket,
		S3Prefix:                *s3Prefix,
		S3Endpoint:              *s3Endpoint,
		S3Region:                *s3Region,
		S3UseSSL:                *s3UseSSL,
		S3UsePath:               *s3UsePath,
	}.WithDefaults()

	// Create logger
	logger, err := zap.NewDevelopment()
	if err != nil {
		return fmt.Errorf("create logger: %w", err)
	}
	defer logger.Sync()

	ctx := context.Background()

	// Optional schema registry
	var schemaRegistry forma.SchemaRegistry
	if *schemaRegistryTable != "" {
		connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s", cfg.PGHost, cfg.PGPort, cfg.PGUser, cfg.PGPassword, cfg.PGDB, cfg.PGSSLMode)
		pool, err := pgxpool.New(ctx, connStr)
		if err != nil {
			return fmt.Errorf("create schema registry pool: %w", err)
		}
		defer pool.Close()
		reg, err := internal.NewFileSchemaRegistry(pool, *schemaRegistryTable, *schemaDir)
		if err != nil {
			return fmt.Errorf("create schema registry: %w", err)
		}
		schemaRegistry = reg
	}

	// Create S3 client
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.S3Region))
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	var s3Client *s3.Client
	if cfg.S3Endpoint != "" {
		// Custom endpoint (MinIO, etc.)
		s3Client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.BaseEndpoint = &cfg.S3Endpoint
			o.UsePathStyle = cfg.S3UsePath
		})
	} else {
		s3Client = s3.NewFromConfig(awsCfg)
	}

	// Run flush
	logger.Info("starting CDC flush",
		zap.String("bucket", cfg.S3Bucket),
		zap.String("prefix", cfg.S3Prefix),
		zap.Bool("dry_run", *dryRun))

	if err := cdc.RunOnce(ctx, cfg, s3Client, *dryRun, logger, schemaRegistry); err != nil {
		return fmt.Errorf("CDC flush failed: %w", err)
	}

	logger.Info("CDC flush completed")
	return nil
}
