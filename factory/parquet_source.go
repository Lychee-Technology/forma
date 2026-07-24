package factory

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/bootstrap"
	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/manifest"
)

// newManifestS3Client is the seam for building the S3 client behind the
// manifest parquet source. Tests swap it to keep factory construction
// hermetic; production always resolves the real credential chain.
var newManifestS3Client = func(ctx context.Context, cfg forma.DuckDBConfig) (manifest.S3ProbeClient, error) {
	return bootstrap.NewS3Client(ctx, bootstrap.S3Options{
		Region:    cfg.S3Region,
		Endpoint:  cfg.S3Endpoint,
		AccessKey: cfg.S3AccessKey,
		SecretKey: cfg.S3SecretKey,
	})
}

// newManifestParquetSource builds the manifest-driven parquet source that
// federated reads use to scan exactly the manifest-listed objects (#250).
//
// It returns a literal nil ParquetSource — never a typed-nil
// *manifest.QuerySource — when manifest reads are not in play, because the
// engine gates on `parquetSource != nil` as an interface comparison: a boxed
// typed nil would pass that gate and then resolve every schema against a
// zero-value source.
//
// Two conditions must both hold for a source to exist:
//   - cfg.Enabled: DuckDB is off entirely, so nothing reads parquet.
//   - cfg.ManifestReadEnabled(): ManifestTemplate is the single opt-in gate.
//     Without it, manifest.NewS3QuerySource would silently adopt the
//     resolver's built-in default layout, pointing reads at a manifest path
//     the writers may never use.
//
// Configuration errors and S3 client construction failures are fatal: a
// half-configured manifest read surface degrades to a silently short result
// set (the cold tier simply goes missing), which is exactly the failure mode
// this wiring exists to prevent.
func newManifestParquetSource(ctx context.Context, cfg forma.DuckDBConfig) (federated.ParquetSource, error) {
	if err := cfg.ValidateManifestRead(); err != nil {
		return nil, fmt.Errorf("invalid duckdb manifest configuration: %w", err)
	}
	if !cfg.Enabled || !cfg.ManifestReadEnabled() {
		return nil, nil
	}

	client, err := newManifestS3Client(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to build s3 client for manifest parquet source: %w", err)
	}

	return manifest.NewS3QuerySource(client, manifest.S3QuerySourceConfig{
		Bucket:           cfg.S3Bucket,
		ManifestPrefix:   cfg.ManifestPrefix,
		ManifestTemplate: cfg.ManifestTemplate,
		DataPrefix:       cfg.S3DataPrefix,
	}), nil
}
