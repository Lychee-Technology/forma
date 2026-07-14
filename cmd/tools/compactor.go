package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/compaction"
	"go.uber.org/zap"
)

func runCompactor(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("compactor", flag.ContinueOnError)
	fs.SetOutput(flag.CommandLine.Output())

	// Schema selection
	schemaID := fs.Int("schema-id", 0, "Schema ID to compact (required)")

	var s3Config s3Flags
	s3Config.register(fs, s3FlagOptions{
		bucketUsage:    "S3 bucket for parquet/manifest files (required)",
		bucketRequired: true,
	})

	// Manifest settings
	manifestPrefix := fs.String("manifest-prefix", "", "Manifest prefix in S3")
	manifestTemplate := fs.String("manifest-template", "manifest/{{.SchemaID}}.json", "Manifest path template")

	// Compaction thresholds
	targetBaseSizeMB := fs.Int("target-base-size-mb", 256, "Target base file size in MB")
	maxDeltaSizeMB := fs.Int("max-delta-size-mb", 50, "Maximum delta size in MB")
	dirtyRatioPct := fs.Int("dirty-ratio-pct", 5, "Dirty ratio percentage threshold")

	// Retry settings
	maxRetries := fs.Int("max-retries", 5, "Maximum retry attempts")
	baseBackoffMs := fs.Int("base-backoff-ms", 100, "Base backoff in milliseconds")
	maxBackoffMs := fs.Int("max-backoff-ms", 10000, "Maximum backoff in milliseconds")

	// Data prefix
	dataPrefix := fs.String("data-prefix", "data", "Data prefix for parquet files")

	// Merge engine (dirty-ratio rewrite, #188): DuckDB reads the schema's
	// parquet set via httpfs, so it needs the same S3 wiring the CDC
	// exporters use. Credentials come from AWS_ACCESS_KEY_ID /
	// AWS_SECRET_ACCESS_KEY when not baked into the environment's config.
	var duckConfig duckExportFlags
	duckConfig.register(fs, duckExportFlagOptions{memLimitDefault: "4GB", queryTimeout: 5 * time.Minute})

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	if *schemaID == 0 {
		return fmt.Errorf("--schema-id is required")
	}
	if err := s3Config.validate(true); err != nil {
		return err
	}

	compactCfg := cdc.CompactionConfig{
		SchemaID:         int16(*schemaID),
		TargetBaseSizeMB: *targetBaseSizeMB,
		MaxDeltaSizeMB:   *maxDeltaSizeMB,
		DirtyRatioPct:    *dirtyRatioPct,
		MaxRetries:       *maxRetries,
		BaseBackoff:      time.Duration(*baseBackoffMs) * time.Millisecond,
		MaxBackoff:       time.Duration(*maxBackoffMs) * time.Millisecond,
	}.WithDefaults()

	manifestCfg := cdc.ManifestConfig{
		Bucket:       s3Config.bucket,
		Prefix:       *manifestPrefix,
		PathTemplate: *manifestTemplate,
	}

	logger, err := buildToolLogger(false)
	if err != nil {
		return fmt.Errorf("create logger: %w", err)
	}
	defer func() { _ = logger.Sync() }()

	s3Client, err := buildToolS3Client(ctx, s3Config.region, s3Config.endpoint, s3Config.usePath)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	// Create provider
	provider := compaction.NewS3ManifestProvider(manifestCfg, s3Client)

	// Merge engine: a CDC DuckDB exporter configured for the same S3.
	duckCfg := cdc.CDCConfig{
		DuckDBPath:              duckConfig.duckDBPath,
		DuckThreads:             duckConfig.duckThreads,
		DuckMemLimit:            duckConfig.duckMemLimit,
		QueryTimeout:            duckConfig.queryTimeout,
		ParquetCompression:      duckConfig.parquetCompression,
		ParquetCompressionLevel: duckConfig.parquetCompressionLevel,
		S3Bucket:                s3Config.bucket,
		S3Endpoint:              s3Config.endpoint,
		S3Region:                s3Config.region,
		S3UseSSL:                s3Config.useSSL,
		S3UsePath:               s3Config.usePath,
	}
	exporter, err := cdc.NewDuckExporter(ctx, duckCfg,
		os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), logger)
	if err != nil {
		return fmt.Errorf("open merge duckdb: %w", err)
	}
	defer func() { _ = exporter.DB.Close() }()

	// Create compactor
	c := &compaction.Compactor{
		Logger:     logger,
		Config:     compactCfg,
		Provider:   provider,
		Merger:     &compaction.DuckMerger{DB: exporter.DB},
		S3:         s3Client,
		Bucket:     s3Config.bucket,
		DataPrefix: *dataPrefix,
	}

	// Run compaction
	logger.Info("starting compaction",
		zap.Int16("schema_id", compactCfg.SchemaID),
		zap.String("bucket", s3Config.bucket))

	result, err := c.RunOnce(ctx)
	if err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}

	switch result.Outcome {
	case compaction.PromotionApplied:
		logger.Info("compaction completed",
			zap.Int16("schema_id", result.SchemaID),
			zap.Int64("version", result.Version),
			zap.Float64("dirty_ratio", result.DirtyRatio))
	case compaction.RewriteApplied:
		logger.Info("compaction rewrite completed",
			zap.Int16("schema_id", result.SchemaID),
			zap.Int64("version", result.Version),
			zap.Float64("dirty_ratio", result.DirtyRatio),
			zap.Int("files_merged", result.FilesMerged),
			zap.Int64("rows_in", result.RowsIn),
			zap.Int64("rows_out", result.RowsOut),
			zap.String("new_base_key", result.NewBaseKey))
	case compaction.RewritePending:
		logger.Warn("compaction deferred: rewrite pending",
			zap.Int16("schema_id", result.SchemaID),
			zap.Float64("dirty_ratio", result.DirtyRatio),
			zap.Int64("base_mb", result.BaseMB),
			zap.Int64("delta_mb", result.DeltaMB))
	default:
		logger.Info("compaction completed",
			zap.Int16("schema_id", result.SchemaID),
			zap.String("outcome", string(result.Outcome)))
	}
	return nil
}
