package bootstrap

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awscreds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// loadAWSConfig is the seam for loading the ambient AWS configuration. Tests
// swap it to avoid touching the real credential chain; see the
// toolLoadAWSConfigFn precedent in cmd/tools/tool_bootstrap.go.
var loadAWSConfig = awsconfig.LoadDefaultConfig

// S3Options describes the S3 endpoint and credentials for the read path.
//
// There is deliberately no UsePathStyle field: addressing style is derived
// from Endpoint (see NewS3Client) so that this client and DuckDB's httpfs
// always address the same objects the same way.
type S3Options struct {
	Region    string
	Endpoint  string
	AccessKey string
	SecretKey string
}

// NewS3Client builds an S3 client for the read path.
//
// Credential precedence mirrors internal/cdc/flusher.go setupAWSClient:
// static credentials from opts win, then AWS_ACCESS_KEY_ID /
// AWS_SECRET_ACCESS_KEY from the environment as a static provider, then
// whatever the default chain resolved.
func NewS3Client(ctx context.Context, opts S3Options) (*s3.Client, error) {
	awsCfg, err := loadAWSConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load aws config for s3 client (region %q): %w", opts.Region, err)
	}

	if opts.Region != "" {
		awsCfg.Region = opts.Region
	}
	applyCredentials(&awsCfg, opts)

	if opts.Endpoint == "" {
		return s3.NewFromConfig(awsCfg), nil
	}

	// A configured endpoint means a non-AWS, path-style object store. DuckDB's
	// httpfs is set to s3_url_style='path' whenever an endpoint is configured
	// (internal/federated/duckdb_conn.go), so this client must use path-style
	// addressing too — both read the same objects and must address them
	// identically.
	endpoint := opts.Endpoint
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = &endpoint
		o.UsePathStyle = true
	}), nil
}

// applyCredentials overlays static credentials onto the loaded config,
// preferring explicit options over environment variables and leaving the
// default chain in place when neither is set.
func applyCredentials(awsCfg *aws.Config, opts S3Options) {
	if opts.AccessKey != "" {
		awsCfg.Credentials = awscreds.NewStaticCredentialsProvider(opts.AccessKey, opts.SecretKey, "")
		return
	}
	if envKey := os.Getenv("AWS_ACCESS_KEY_ID"); envKey != "" {
		awsCfg.Credentials = awscreds.NewStaticCredentialsProvider(envKey, os.Getenv("AWS_SECRET_ACCESS_KEY"), "")
	}
}
