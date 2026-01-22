package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/compaction"
	"go.uber.org/zap"
)

func runCompactor(args []string) error {
	fs := flag.NewFlagSet("compactor", flag.ExitOnError)

	// Schema selection
	schemaID := fs.Int("schema-id", 0, "Schema ID to compact (required)")

	// S3 settings
	s3Bucket := fs.String("s3-bucket", "", "S3 bucket for parquet/manifest files (required)")
	s3Endpoint := fs.String("s3-endpoint", "", "S3 endpoint (for MinIO)")
	s3Region := fs.String("s3-region", "us-east-1", "S3 region")
	_ = fs.Bool("s3-use-ssl", true, "Use SSL for S3") // reserved for future use
	s3UsePath := fs.Bool("s3-use-path", false, "Use path-style S3 addressing")

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
		return err
	}

	if *schemaID == 0 {
		return fmt.Errorf("--schema-id is required")
	}
	if *s3Bucket == "" {
		return fmt.Errorf("--s3-bucket is required")
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
		Bucket:       *s3Bucket,
		Prefix:       *manifestPrefix,
		PathTemplate: *manifestTemplate,
	}

	// Create logger
	logger, err := zap.NewProduction()
	if err != nil {
		return fmt.Errorf("create logger: %w", err)
	}
	defer logger.Sync()

	// Create S3 client
	ctx := context.Background()
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(*s3Region))
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	var s3Client *s3.Client
	if *s3Endpoint != "" {
		s3Client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.BaseEndpoint = s3Endpoint
			o.UsePathStyle = *s3UsePath
		})
	} else {
		s3Client = s3.NewFromConfig(awsCfg)
	}

	// Create provider
	provider := compaction.NewS3ManifestProvider(manifestCfg, s3Client)

	// Create compactor
	c := &compaction.Compactor{
		Logger:     logger,
		Config:     compactCfg,
		Provider:   provider,
		Bucket:     *s3Bucket,
		DataPrefix: *dataPrefix,
	}

	// Run compaction
	logger.Info("starting compaction",
		zap.Int16("schema_id", compactCfg.SchemaID),
		zap.String("bucket", *s3Bucket))

	if err := c.RunOnce(ctx); err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}

	logger.Info("compaction completed")
	return nil
}
