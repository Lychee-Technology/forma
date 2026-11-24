package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/url"
	"strings"
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

// mergeRouteErr is checkMergeRoute's error alone.
func mergeRouteErr(c *s3.Client, s s3Flags) error {
	_, err := checkMergeRoute(c, s)
	return err
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
	require.NoError(t, mergeRouteErr(client(nil), flags))
	regional := flags
	regional.endpoint, regional.useSSL = "", true
	require.NoError(t, mergeRouteErr(client(func(o *s3.Options) { o.BaseEndpoint = nil }), regional))

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
		require.ErrorIs(t, mergeRouteErr(client(tc.opts), f), ErrMergeRouteMismatch, name)
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

// TestCheckMergeRouteComparesEffectiveAddressing: with --s3-use-path=false
// the merge engine always addresses the bucket virtual-hosted, while the SDK
// client falls back to path style for a bucket it cannot name in the host
// (R3-F1). Equal flags then still mean two routes, and the run is refused;
// --s3-use-path=true makes both path-style.
func TestCheckMergeRouteComparesEffectiveAddressing(t *testing.T) {
	check := func(f s3Flags) error {
		o := s3.Options{Region: f.region, UsePathStyle: f.usePath}
		if f.endpoint != "" {
			o.BaseEndpoint = aws.String(f.endpoint)
		}
		return mergeRouteErr(s3.New(o), f)
	}
	for name, tc := range map[string]struct {
		flags    s3Flags
		mismatch bool
	}{
		"a dotted bucket over the AWS default HTTPS endpoint":     {flags: s3Flags{bucket: "my.bucket", useSSL: true}, mismatch: true},
		"a dotted bucket over a custom HTTPS endpoint":            {flags: s3Flags{bucket: "my.bucket", endpoint: "https://s3.example", useSSL: true}, mismatch: true},
		"a bucket that is not a DNS label":                        {flags: s3Flags{bucket: "My_Bucket", endpoint: "https://s3.example", useSSL: true}, mismatch: true},
		"an IP endpoint":                                          {flags: s3Flags{bucket: "bkt", endpoint: "http://127.0.0.1:9000"}, mismatch: true},
		"a dotted bucket path-style over HTTPS":                   {flags: s3Flags{bucket: "my.bucket", useSSL: true, usePath: true}},
		"a dotted bucket path-style over a custom HTTPS endpoint": {flags: s3Flags{bucket: "my.bucket", endpoint: "https://s3.example", useSSL: true, usePath: true}},
		"a dotted bucket over a custom plain-HTTP endpoint":       {flags: s3Flags{bucket: "my.bucket", endpoint: "http://s3.example"}, mismatch: true},
		"a plain bucket virtual-hosted over HTTPS":                {flags: s3Flags{bucket: "bkt", useSSL: true}},
		"an IP endpoint path-style":                               {flags: s3Flags{bucket: "bkt", endpoint: "http://127.0.0.1:9000", usePath: true}},
	} {
		tc.flags.region = "us-east-1"
		err := check(tc.flags)
		if tc.mismatch {
			require.ErrorIs(t, err, ErrMergeRouteMismatch, name)
			require.ErrorContains(t, err, "pass --s3-use-path=true", name)
			continue
		}
		require.NoError(t, err, name)
	}
}

// TestCompactorRefusesADottedBucketOverHTTPSVirtualHosted: the SDK client
// reads a dotted bucket over HTTPS path-style, the merge engine would read it
// virtual-hosted under --s3-use-path=false (R3-F1). The run fails before it
// opens the database.
func TestCompactorRefusesADottedBucketOverHTTPSVirtualHosted(t *testing.T) {
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
		"--schema-id", "7", "--s3-bucket", "my.bucket", "--entity-main-table", "entity_main",
	}, &bytes.Buffer{})
	require.ErrorIs(t, err, ErrMergeRouteMismatch)
	require.ErrorContains(t, err, `bucket "my.bucket" path-style`)
	require.False(t, opened, "refused before the database is opened")

	err = runCompactorOut(context.Background(), []string{
		"--schema-id", "7", "--s3-bucket", "my.bucket", "--entity-main-table", "entity_main", "--s3-use-path=true",
	}, &bytes.Buffer{})
	require.NotErrorIs(t, err, ErrMergeRouteMismatch)
	require.True(t, opened, "path-style on both sides passes the route check")
}

// captureURL is an S3 HTTP client that records the URL of the request the SDK
// built and sends nothing.
type captureURL struct{ url *string }

func (c captureURL) Do(r *http.Request) (*http.Response, error) {
	*c.url = r.URL.String()
	return nil, errors.New("not sent")
}

// TestClientPathStyleMatchesTheRequestURL pins clientPathStyle to the request
// the SDK client actually builds for an object of the bucket: path-style
// exactly when the bucket is the first segment of the URL path and not in the
// host name.
func TestClientPathStyleMatchesTheRequestURL(t *testing.T) {
	for _, tc := range []struct {
		bucket, endpoint string
		disableHTTPS     bool
		forcePath        bool
	}{
		{bucket: "bkt"},
		{bucket: "bkt", disableHTTPS: true},
		{bucket: "bkt", forcePath: true},
		{bucket: "my.bucket"},
		{bucket: "my.bucket", disableHTTPS: true},
		{bucket: "bkt", endpoint: "https://s3.example"},
		{bucket: "my.bucket", endpoint: "https://s3.example"},
		{bucket: "my.bucket", endpoint: "http://s3.example"},
		{bucket: "My_Bucket", endpoint: "https://s3.example"},
		{bucket: "bkt", endpoint: "http://127.0.0.1:9000"},
		{bucket: "bkt", endpoint: "http://rustfs:9000", forcePath: true},
	} {
		var sent string
		o := s3.Options{
			Region: "us-east-1", UsePathStyle: tc.forcePath, HTTPClient: captureURL{&sent},
			Credentials: aws.AnonymousCredentials{}, RetryMaxAttempts: 1,
		}
		o.EndpointOptions.DisableHTTPS = tc.disableHTTPS
		if tc.endpoint != "" {
			o.BaseEndpoint = aws.String(tc.endpoint)
		}
		c := s3.New(o)
		_, _ = c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(tc.bucket), Key: aws.String("k")})
		require.NotEmpty(t, sent, "%+v", tc)
		u, err := url.Parse(sent)
		require.NoError(t, err)
		sentPath := strings.HasPrefix(u.Path, "/"+tc.bucket+"/") && !strings.HasPrefix(u.Host, tc.bucket+".")
		got, err := clientPathStyle(c.Options(), tc.bucket)
		require.NoError(t, err)
		require.Equal(t, sentPath, got, "%+v sent %s", tc, sent)
	}
}
