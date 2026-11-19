package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/lychee-technology/forma"
	publiccdc "github.com/lychee-technology/forma/cdc"
	"github.com/lychee-technology/forma/internal/cdcv2"
	"go.uber.org/zap"
)

// cdcFlushOptions carries everything runCDCFlush needs after the flag set has
// been parsed and validated.
type cdcFlushOptions struct {
	cfg      publiccdc.Config
	pg       postgresFlags
	registry schemaRegistryFlags
	dryRun   bool
}

// parseCDCFlushFlags registers, parses and validates the cdc-flush flag set.
// It answers (nil, nil) for -help so the caller exits quietly, mirroring the
// errors.Is(err, flag.ErrHelp) branch this was extracted from (#319).
func parseCDCFlushFlags(args []string) (*cdcFlushOptions, error) {
	fs := flag.NewFlagSet("cdc-flush", flag.ContinueOnError)
	fs.SetOutput(flag.CommandLine.Output())

	var shared cdcSharedFlags
	var flushOnly cdcFlushOnlyFlags
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
		includeUseIAM:   true,
		useIAMFlag:      "pg-use-iam",
		useIAMUsage:     "Use IAM authentication for PostgreSQL",
		engineFlag:      "pg-engine",
	})

	// Change log settings
	fs.StringVar(&shared.changeLogTable, "change-log-table", "change_log", cdcChangeLogTableUsage)
	fs.StringVar(&shared.entityMainTable, "entity-main-table", "entity_main", "Entity main table name")
	fs.StringVar(&shared.eavDataTable, "eav-table", "eav_data", "EAV data table name")
	fs.IntVar(&flushOnly.minRecords, "min-records", 20000, "Minimum records before flush")
	fs.Int64Var(&flushOnly.maxAgeMs, "max-age-ms", 3600000, "Maximum age in ms before flush")
	fs.IntVar(&shared.batchSize, "batch-size", 10000, fmt.Sprintf("Most unflushed entries one flush selects and publishes per schema "+
		"(PostgreSQL: at most %d, marked in one statement; Aurora DSQL: published in generations of at most %d)", cdcv2.MaxMarkEntries, cdcv2.DSQLGenerationLimit))
	fs.IntVar(&shared.estimatedRowBytes, "estimated-row-bytes", 0, "Estimated bytes per row for batch sizing (0 to use default)")
	fs.Int64Var(&flushOnly.maxBatchBytes, "max-batch-bytes", 0, "Max batch size in bytes (0 to use default)")

	shared.duck.register(fs, duckExportFlagOptions{
		memLimitDefault: "4GB",
		queryTimeout:    5 * time.Minute,
	})

	shared.s3.register(fs, s3FlagOptions{
		includePrefix:  true,
		prefixFlag:     "s3-prefix",
		prefixDefault:  defaultCDCDataPrefix,
		prefixUsage:    cdcDataPrefixUsage,
		bucketUsage:    "S3 bucket for parquet files and manifests (required)",
		bucketRequired: true,
	})

	var schemaRegistry schemaRegistryFlags
	schemaRegistry.register(fs, false)

	// Control
	dryRun := fs.Bool("dry-run", false, "Dry run mode (no actual flush)")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, nil
		}
		return nil, err
	}

	if err := shared.s3.validate(true); err != nil {
		return nil, err
	}
	if err := schemaRegistry.validate(false); err != nil {
		return nil, err
	}
	if err := validateParquetPrefixFlag("s3-prefix", shared.s3.prefix); err != nil {
		return nil, err
	}
	engine, err := shared.pg.cdcEngine("pg-engine")
	if err != nil {
		return nil, err
	}

	shared.password = shared.pg.resolvedPassword("PGPASSWORD")
	cfg := buildCDCConfig(shared, flushOnly)
	cfg.Engine = engine
	if err := cfg.ValidateFlush(); err != nil {
		return nil, fmt.Errorf("validate -batch-size and -pg-engine: %w", err)
	}

	return &cdcFlushOptions{cfg: cfg, pg: shared.pg, registry: schemaRegistry, dryRun: *dryRun}, nil
}

// cdcFlushOnlyFlags holds the cdc-flush flag values outside the block it
// shares with cdc-init (#445).
type cdcFlushOnlyFlags struct {
	minRecords    int
	maxAgeMs      int64
	maxBatchBytes int64
}

// buildCDCConfig constructs a CDC config from parsed flag values: the shared
// block, then the flush-only fields, then WithDefaults last.
func buildCDCConfig(shared cdcSharedFlags, flushOnly cdcFlushOnlyFlags) publiccdc.Config {
	cfg := shared.sharedConfig()
	cfg.MinRecords = flushOnly.minRecords
	cfg.MaxAgeMs = flushOnly.maxAgeMs
	cfg.MaxBatchBytes = flushOnly.maxBatchBytes
	cfg.PGUseIAM = shared.pg.useIAM
	return cfg.WithDefaults()
}

func runCDCFlush(ctx context.Context, args []string) error {
	opts, err := parseCDCFlushFlags(args)
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

	var registry forma.SchemaRegistry
	if opts.registry.table != "" {
		pool, err := buildToolPostgresPool(ctx, opts.pg.databaseConfig("PGPASSWORD", toolPostgresPoolSettings{
			maxConnections: 4,
			timeout:        30 * time.Second,
		}))
		if err != nil {
			return fmt.Errorf("create schema registry pool: %w", err)
		}
		defer pool.Close()
		reg, err := buildToolSchemaRegistry(ctx, pool, opts.registry.table, opts.registry.dir)
		if err != nil {
			return fmt.Errorf("create schema registry: %w", err)
		}
		registry = reg
	}

	s3Client, err := toolCDCObjectClientFn(ctx, cfg.S3Region, cfg.S3Endpoint, cfg.S3UsePath)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	// Run flush
	logger.Info("starting CDC flush",
		zap.String("bucket", cfg.S3Bucket),
		zap.String("prefix", cfg.S3Prefix),
		zap.String("engine", string(cfg.Engine)),
		zap.Int("batch_size", cfg.BatchSize),
		zap.Bool("dry_run", opts.dryRun))

	report, err := publiccdc.RunOnce(ctx, cfg, s3Client, opts.dryRun, logger, registry)
	if err != nil {
		logger.Info("CDC flush published before it failed", zap.Any("flushed_schemas", report.Flushed), zap.Any("skipped_schemas", report.Skipped))
		return fmt.Errorf("CDC flush failed: %w", err)
	}

	// A skipped schema is not a failure (#704): the run exits 0 and names it.
	logger.Info("CDC flush completed", zap.Any("flushed_schemas", report.Flushed), zap.Any("skipped_schemas", report.Skipped))
	return nil
}
