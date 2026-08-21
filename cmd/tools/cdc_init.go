package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/lychee-technology/forma/internal/cdc"
	"go.uber.org/zap"
)

const (
	defaultInitBatchSize = 50000
)

// runCDCInit parses cdc-init CLI flags and delegates the actual base-file
// export to cdc.RunInit (the driver was extracted into internal/cdc, #173).
func runCDCInit(ctx context.Context, args []string) error { //nolint:funlen // #319: flag parsing extracted by this plan's Task 3
	fs := flag.NewFlagSet("cdc-init", flag.ContinueOnError)
	fs.SetOutput(flag.CommandLine.Output())

	var pg postgresFlags
	pg.register(fs, postgresFlagOptions{
		hostFlag:        "pg-host",
		portFlag:        "pg-port",
		userFlag:        "pg-user",
		passwordFlag:    "pg-password",
		databaseFlag:    "pg-db",
		sslModeFlag:     "pg-ssl-mode",
		hostDefault:     "localhost",
		portDefault:     5432,
		userDefault:     "postgres",
		passwordDefault: "",
		databaseDefault: "forma",
		sslModeDefault:  "require",
		hostUsage:       "PostgreSQL host",
		portUsage:       "PostgreSQL port",
		userUsage:       "PostgreSQL user",
		passwordUsage:   "PostgreSQL password (or set PGPASSWORD env)",
		databaseUsage:   "PostgreSQL database",
		sslModeUsage:    "PostgreSQL sslmode",
	})

	// Table names
	entityMainTable := fs.String("entity-main-table", "entity_main", "Entity main table name")
	eavDataTable := fs.String("eav-table", "eav_data", "EAV data table name")

	var duck duckExportFlags
	duck.register(fs, duckExportFlagOptions{
		memLimitDefault: "8GB",
		queryTimeout:    10 * time.Minute,
	})

	var s3Config s3Flags
	s3Config.register(fs, s3FlagOptions{
		includePrefix:  true,
		prefixFlag:     "s3-prefix",
		prefixDefault:  "base",
		prefixUsage:    "S3 prefix for base files",
		bucketUsage:    "S3 bucket for parquet files (required)",
		bucketRequired: true,
	})

	// Manifest settings (optional - enables manifest tracking)
	manifestPrefix := fs.String("manifest-prefix", "", "Manifest prefix in S3 (enables manifest tracking)")
	manifestTemplate := fs.String("manifest-template", "manifest/{{.SchemaID}}.json", "Manifest path template")

	var schemaRegistry schemaRegistryFlags
	schemaRegistry.register(fs, true)

	// Init-specific options
	batchSize := fs.Int("batch-size", defaultInitBatchSize, "Rows per batch (overridden by target-file-size-mb)")
	targetFileSizeMB := fs.Int("target-file-size-mb", 256, "Target parquet file size in MB (0 = use batch-size)")
	estimatedRowBytes := fs.Int("estimated-row-bytes", 0, "Override row size estimate in bytes (0 = auto from schema)")
	maxBatchSize := fs.Int("max-batch-size", 10000000, "Maximum batch size to cap memory usage")
	schemaIDFilter := fs.Int("schema-id", 0, "Specific schema ID to init (0 = all schemas)")
	dryRun := fs.Bool("dry-run", false, "Dry run mode (no actual export)")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	if err := s3Config.validate(true); err != nil {
		return err
	}
	if err := schemaRegistry.validate(true); err != nil {
		return err
	}

	// Track if user explicitly provided estimated-row-bytes (0 means auto-calculate from schema)
	autoEstimateRowBytes := *estimatedRowBytes == 0
	password := pg.resolvedPassword("PGPASSWORD")

	cfg := cdc.CDCConfig{
		EntityMainTable:         *entityMainTable,
		EAVDataTable:            *eavDataTable,
		BatchSize:               *batchSize,
		TargetFileSizeMB:        *targetFileSizeMB,
		EstimatedRowBytes:       *estimatedRowBytes,
		MaxBatchSize:            *maxBatchSize,
		PGHost:                  pg.host,
		PGPort:                  pg.port,
		PGUser:                  pg.user,
		PGPassword:              password,
		PGDB:                    pg.database,
		PGSSLMode:               pg.sslMode,
		DuckDBPath:              duck.duckDBPath,
		DuckThreads:             duck.duckThreads,
		DuckMemLimit:            duck.duckMemLimit,
		QueryTimeout:            duck.queryTimeout,
		ParquetCompression:      duck.parquetCompression,
		ParquetCompressionLevel: duck.parquetCompressionLevel,
		S3Bucket:                s3Config.bucket,
		S3Prefix:                s3Config.prefix,
		S3Endpoint:              s3Config.endpoint,
		S3Region:                s3Config.region,
		S3UseSSL:                s3Config.useSSL,
		S3UsePath:               s3Config.usePath,
		ManifestPrefix:          *manifestPrefix,
		ManifestTemplate:        *manifestTemplate,
	}.WithDefaults()

	logger, err := buildToolLogger(true)
	if err != nil {
		return fmt.Errorf("create logger: %w", err)
	}
	defer func() { _ = logger.Sync() }()

	pool, err := buildToolPostgresPool(ctx, pg.databaseConfig("PGPASSWORD", toolPostgresPoolSettings{
		maxConnections: 4,
		timeout:        30 * time.Second,
	}))
	if err != nil {
		return fmt.Errorf("create schema registry pool: %w", err)
	}
	defer pool.Close()

	registry, err := buildToolSchemaRegistry(ctx, pool, schemaRegistry.table, schemaRegistry.dir)
	if err != nil {
		return fmt.Errorf("create schema registry: %w", err)
	}

	s3Client, err := buildToolS3Client(ctx, cfg.S3Region, cfg.S3Endpoint, cfg.S3UsePath)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	// Run init
	logger.Info("starting CDC init",
		zap.String("bucket", cfg.S3Bucket),
		zap.String("prefix", cfg.S3Prefix),
		zap.String("manifest_template", cfg.ManifestTemplate),
		zap.Int("target_file_size_mb", cfg.TargetFileSizeMB),
		zap.Int("estimated_row_bytes", cfg.EstimatedRowBytes),
		zap.Int("max_batch_size", cfg.MaxBatchSize),
		zap.Int("fallback_batch_size", cfg.BatchSize),
		zap.Int("schema_id_filter", *schemaIDFilter),
		zap.Bool("dry_run", *dryRun),
		zap.Bool("auto_estimate_row_bytes", autoEstimateRowBytes))

	if _, err := cdc.RunInit(ctx, cdc.InitOptions{
		Config:               cfg,
		S3Client:             s3Client,
		SchemaRegistryTable:  schemaRegistry.table,
		SchemaIDFilter:       *schemaIDFilter,
		DryRun:               *dryRun,
		AutoEstimateRowBytes: autoEstimateRowBytes,
		Logger:               logger,
		SchemaRegistry:       registry,
	}); err != nil {
		return fmt.Errorf("CDC init failed: %w", err)
	}

	logger.Info("CDC init completed")
	return nil
}
