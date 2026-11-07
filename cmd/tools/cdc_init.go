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

// cdcInitOptions carries everything runCDCInit needs after the flag set has
// been parsed and validated.
type cdcInitOptions struct {
	cfg                  cdc.CDCConfig
	pg                   postgresFlags
	registry             schemaRegistryFlags
	schemaIDFilter       int
	dryRun               bool
	autoEstimateRowBytes bool
	replaceDelta         bool
	deltaPrefix          string
}

// parseCDCInitFlags registers, parses and validates the cdc-init flag set.
// It answers (nil, nil) for -help so the caller exits quietly, mirroring the
// errors.Is(err, flag.ErrHelp) branch this was extracted from (#319).
func parseCDCInitFlags(args []string) (*cdcInitOptions, error) {
	fs := flag.NewFlagSet("cdc-init", flag.ContinueOnError)
	fs.SetOutput(flag.CommandLine.Output())

	var shared cdcSharedFlags
	var initOnly cdcInitOnlyFlags
	shared.pg.register(fs, postgresFlagOptions{
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
		engineFlag:      "pg-engine",
	})

	// Table names
	fs.StringVar(&shared.entityMainTable, "entity-main-table", "entity_main", "Entity main table name")
	fs.StringVar(&shared.eavDataTable, "eav-table", "eav_data", "EAV data table name")

	shared.duck.register(fs, duckExportFlagOptions{
		memLimitDefault: "8GB",
		queryTimeout:    10 * time.Minute,
	})

	shared.s3.register(fs, s3FlagOptions{
		includePrefix:  true,
		prefixFlag:     "s3-prefix",
		prefixDefault:  "base",
		prefixUsage:    "S3 prefix for base files",
		bucketUsage:    "S3 bucket for parquet files (required)",
		bucketRequired: true,
	})

	// Manifest settings (optional - enables manifest tracking)
	fs.StringVar(&shared.manifestPrefix, "manifest-prefix", "", "Manifest prefix in S3 (enables manifest tracking)")
	fs.StringVar(&shared.manifestTemplate, "manifest-template", "manifest/{{.SchemaID}}.json", "Manifest path template")

	var schemaRegistry schemaRegistryFlags
	schemaRegistry.register(fs, true)

	// Init-specific options
	fs.IntVar(&shared.batchSize, "batch-size", defaultInitBatchSize, "Rows per batch (overridden by target-file-size-mb)")
	fs.IntVar(&initOnly.targetFileSizeMB, "target-file-size-mb", 256, "Target parquet file size in MB (0 = use batch-size)")
	fs.IntVar(&shared.estimatedRowBytes, "estimated-row-bytes", 0, "Override row size estimate in bytes (0 = auto from schema)")
	fs.IntVar(&initOnly.maxBatchSize, "max-batch-size", 10000000, "Maximum batch size to cap memory usage")
	schemaIDFilter := fs.Int("schema-id", 0, "Specific schema ID to init (0 = all schemas)")
	dryRun := fs.Bool("dry-run", false, "Dry run mode (no actual export)")
	replaceDelta := fs.Bool("replace-delta", false, "Purge the schema's delta tier after the base swap commits; without it init refuses while delta objects exist (#371)")
	deltaPrefix := fs.String("delta-prefix", "delta", "S3 prefix cdc-flush writes delta files under; listed for delta objects the manifest does not mention (empty = manifest only)")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, nil
		}
		return nil, err
	}

	if err := shared.s3.validate(true); err != nil {
		return nil, err
	}
	if err := schemaRegistry.validate(true); err != nil {
		return nil, err
	}
	if err := validateParquetPrefixFlag("s3-prefix", shared.s3.prefix); err != nil {
		return nil, err
	}
	if err := validateParquetPrefixFlag("delta-prefix", *deltaPrefix); err != nil {
		return nil, err
	}
	engine, err := shared.pg.cdcEngine("pg-engine")
	if err != nil {
		return nil, err
	}

	// Track if user explicitly provided estimated-row-bytes (0 means auto-calculate from schema)
	autoEstimateRowBytes := shared.estimatedRowBytes == 0
	shared.password = shared.pg.resolvedPassword("PGPASSWORD")

	cfg := buildCDCInitConfig(shared, initOnly)
	cfg.Engine = engine

	return &cdcInitOptions{
		cfg:                  cfg,
		pg:                   shared.pg,
		registry:             schemaRegistry,
		schemaIDFilter:       *schemaIDFilter,
		dryRun:               *dryRun,
		autoEstimateRowBytes: autoEstimateRowBytes,
		replaceDelta:         *replaceDelta,
		deltaPrefix:          *deltaPrefix,
	}, nil
}

// cdcInitOnlyFlags holds the cdc-init flag values outside the block it
// shares with cdc-flush (#445).
type cdcInitOnlyFlags struct {
	targetFileSizeMB int
	maxBatchSize     int
}

// buildCDCInitConfig constructs a CDC config from parsed flag values: the
// shared block, then the init-only fields, then WithDefaults last.
func buildCDCInitConfig(shared cdcSharedFlags, initOnly cdcInitOnlyFlags) cdc.CDCConfig {
	cfg := shared.sharedConfig()
	cfg.TargetFileSizeMB = initOnly.targetFileSizeMB
	cfg.MaxBatchSize = initOnly.maxBatchSize
	return cfg.WithDefaults()
}

// runCDCInit parses cdc-init CLI flags and delegates the actual base-file
// export to cdc.RunInit (the driver was extracted into internal/cdc, #173).
func runCDCInit(ctx context.Context, args []string) error {
	opts, err := parseCDCInitFlags(args)
	if err != nil {
		return err
	}
	if opts == nil {
		return nil
	}
	cfg := opts.cfg

	logger, err := buildToolLogger(true)
	if err != nil {
		return fmt.Errorf("create logger: %w", err)
	}
	defer func() { _ = logger.Sync() }()

	pool, err := buildToolPostgresPool(ctx, opts.pg.databaseConfig("PGPASSWORD", toolPostgresPoolSettings{
		maxConnections: 4,
		timeout:        30 * time.Second,
	}))
	if err != nil {
		return fmt.Errorf("create schema registry pool: %w", err)
	}
	defer pool.Close()

	registry, err := buildToolSchemaRegistry(ctx, pool, opts.registry.table, opts.registry.dir)
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
		zap.Int("schema_id_filter", opts.schemaIDFilter),
		zap.Bool("dry_run", opts.dryRun),
		zap.Bool("auto_estimate_row_bytes", opts.autoEstimateRowBytes),
		zap.Bool("replace_delta", opts.replaceDelta),
		zap.String("delta_prefix", opts.deltaPrefix))

	if _, err := cdc.RunInit(ctx, cdc.InitOptions{
		Config:               cfg,
		S3Client:             s3Client,
		SchemaRegistryTable:  opts.registry.table,
		SchemaIDFilter:       opts.schemaIDFilter,
		DryRun:               opts.dryRun,
		AutoEstimateRowBytes: opts.autoEstimateRowBytes,
		Logger:               logger,
		SchemaRegistry:       registry,
		ReplaceDelta:         opts.replaceDelta,
		DeltaPrefix:          opts.deltaPrefix,
	}); err != nil {
		return fmt.Errorf("CDC init failed: %w", err)
	}

	logger.Info("CDC init completed")
	return nil
}
