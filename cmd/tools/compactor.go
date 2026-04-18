package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
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

	// Create compactor
	c := &compaction.Compactor{
		Logger:     logger,
		Config:     compactCfg,
		Provider:   provider,
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
