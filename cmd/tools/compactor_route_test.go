package main

import (
	"bytes"
	"context"
	"database/sql"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/lychee-technology/forma/internal/cdc"
)

// TestCompactorRefusesAnEnvironmentOnlyEndpoint: AWS_ENDPOINT_URL_S3 routes
// the SDK client, which checks the binding and hashes the merge sources, but
// not the DuckDB merge engine, which reads the sources again from the flags'
// endpoint. The run fails before it opens the database, the merge engine or
// anything in the bucket.
func TestCompactorRefusesAnEnvironmentOnlyEndpoint(t *testing.T) {
	isolateAWSConfig(t)
	setStaticS3Env(t)
	t.Setenv("AWS_ENDPOINT_URL_S3", "https://user:pw-secret@other.storage.example/?sig=secret")
	opened := false
	old := openCompactorDBFn
	t.Cleanup(func() { openCompactorDBFn = old })
	openCompactorDBFn = func(context.Context, cdc.CDCConfig, *zap.Logger) (*sql.DB, error) {
		opened = true
		return nil, context.Canceled
	}
	err := runCompactorOut(context.Background(), []string{
		"--schema-id", "7", "--s3-bucket", "bkt", "--entity-main-table", "entity_main",
	}, &bytes.Buffer{})
	require.ErrorIs(t, err, ErrMergeRouteMismatch)
	require.ErrorContains(t, err, "https://other.storage.example/")
	require.NotContains(t, err.Error(), "secret", "the configured endpoint may carry credentials")
	require.False(t, opened, "refused before the database is opened")

	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	t.Setenv("AWS_ENDPOINT_URL", "http://other.storage.example:9000")
	err = runCompactorOut(context.Background(), []string{
		"--schema-id", "7", "--s3-bucket", "bkt", "--entity-main-table", "entity_main",
	}, &bytes.Buffer{})
	require.ErrorIs(t, err, ErrMergeRouteMismatch, "AWS_ENDPOINT_URL routes the client too")
	require.False(t, opened)
}

// TestCheckMergeRoute pins what the merge engine can follow: the flags'
// endpoint, region and addressing, with --s3-use-ssl agreeing with the
// endpoint's scheme, and no FIPS, dual-stack or accelerated route.
func TestCheckMergeRoute(t *testing.T) {
	flags := s3Flags{bucket: "bkt", endpoint: "http://rustfs:9000", region: "us-east-1", usePath: true}
	client := func(mod func(*s3.Options)) *s3.Client {
		o := s3.Options{Region: flags.region, BaseEndpoint: aws.String(flags.endpoint), UsePathStyle: flags.usePath}
		if mod != nil {
			mod(&o)
		}
		return s3.New(o)
	}
	require.NoError(t, checkMergeRoute(client(nil), flags))
	regional := flags
	regional.endpoint, regional.useSSL = "", true
	require.NoError(t, checkMergeRoute(client(func(o *s3.Options) { o.BaseEndpoint = nil }), regional))

	for name, tc := range map[string]struct {
		flags func(*s3Flags)
		opts  func(*s3.Options)
	}{
		"an endpoint from elsewhere": {opts: func(o *s3.Options) { o.BaseEndpoint = &[]string{"http://other:9000"}[0] }},
		"another region":             {opts: func(o *s3.Options) { o.Region = "eu-west-1" }},
		"FIPS":                       {opts: func(o *s3.Options) { o.EndpointOptions.UseFIPSEndpoint = 1 }},
		"dual-stack":                 {opts: func(o *s3.Options) { o.EndpointOptions.UseDualStackEndpoint = 1 }},
		"acceleration":               {opts: func(o *s3.Options) { o.UseAccelerate = true }},
		"https off for https":        {flags: func(f *s3Flags) { f.endpoint = "https://s3.example" }, opts: func(o *s3.Options) { o.BaseEndpoint = &[]string{"https://s3.example"}[0] }},
		"https on for http":          {flags: func(f *s3Flags) { f.useSSL = true }},
		"https off for the AWS default endpoint": {
			flags: func(f *s3Flags) { f.endpoint, f.useSSL = "", false },
			opts:  func(o *s3.Options) { o.BaseEndpoint = nil },
		},
		"https on for an AWS default endpoint without HTTPS": {
			flags: func(f *s3Flags) { f.endpoint, f.useSSL = "", true },
			opts:  func(o *s3.Options) { o.BaseEndpoint, o.EndpointOptions.DisableHTTPS = nil, true },
		},
	} {
		f := flags
		if tc.flags != nil {
			tc.flags(&f)
		}
		require.ErrorIs(t, checkMergeRoute(client(tc.opts), f), ErrMergeRouteMismatch, name)
	}
}

// TestCompactorRefusesPlainHTTPToTheAWSDefaultEndpoint: without
// --s3-endpoint the SDK client checks and hashes the merge sources over the
// region's HTTPS endpoint, so --s3-use-ssl=false would have the merge engine
// read them again over plain HTTP. The run fails before it opens the
// database.
func TestCompactorRefusesPlainHTTPToTheAWSDefaultEndpoint(t *testing.T) {
	isolateAWSConfig(t)
	setStaticS3Env(t)
	opened := false
	old := openCompactorDBFn
	t.Cleanup(func() { openCompactorDBFn = old })
	openCompactorDBFn = func(context.Context, cdc.CDCConfig, *zap.Logger) (*sql.DB, error) {
		opened = true
		return nil, context.Canceled
	}
	err := runCompactorOut(context.Background(), []string{
		"--schema-id", "7", "--s3-bucket", "bkt", "--entity-main-table", "entity_main", "--s3-use-ssl=false",
	}, &bytes.Buffer{})
	require.ErrorIs(t, err, ErrMergeRouteMismatch)
	require.ErrorContains(t, err, `the AWS default endpoint of region "us-east-1" over https`)
	require.False(t, opened, "refused before the database is opened")
}
