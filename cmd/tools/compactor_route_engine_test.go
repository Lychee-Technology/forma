package main

import (
	"bufio"
	"context"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// sdkObjectURL is the URL, query dropped, the S3 client sends a GET of key in
// bucket to; nothing is sent.
func sdkObjectURL(t *testing.T, o s3.Options, bucket, key string) *url.URL {
	t.Helper()
	var sent string
	o.HTTPClient, o.Credentials, o.RetryMaxAttempts = captureURL{&sent}, aws.AnonymousCredentials{}, 1
	_, _ = s3.New(o).GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	require.NotEmpty(t, sent)
	u, err := url.Parse(sent)
	require.NoError(t, err)
	u.RawQuery = ""
	return u
}

// TestMergeEngineEndpointFollowsThePartition: without --s3-endpoint the merge
// engine is configured with the endpoint the SDK resolves for the region, so
// the bucket URL it builds is the SDK's in every AWS partition, AWS China's
// amazonaws.com.cn included (R4-F1), virtual-hosted and path-style.
func TestMergeEngineEndpointFollowsThePartition(t *testing.T) {
	for _, tc := range []struct{ region, endpoint string }{
		{"us-east-1", "https://s3.us-east-1.amazonaws.com"},
		{"eu-west-1", "https://s3.eu-west-1.amazonaws.com"},
		{"cn-north-1", "https://s3.cn-north-1.amazonaws.com.cn"},
		{"cn-northwest-1", "https://s3.cn-northwest-1.amazonaws.com.cn"},
		{"us-gov-west-1", "https://s3.us-gov-west-1.amazonaws.com"},
	} {
		for _, usePath := range []bool{false, true} {
			f := s3Flags{bucket: "bkt", region: tc.region, useSSL: true, usePath: usePath}
			o := s3.Options{Region: tc.region, UsePathStyle: usePath}
			endpoint, err := checkMergeRoute(s3.New(o), f)
			require.NoError(t, err, tc.region)
			require.Equal(t, tc.endpoint, endpoint, tc.region)
			sent := sdkObjectURL(t, o, "bkt", "k.parquet")
			require.Equal(t, sent.String(), mergeEngineBucketURL(endpoint, f)+"/k.parquet", "%s path-style=%t", tc.region, usePath)
		}
	}
	// --s3-endpoint is taken as it is.
	f := s3Flags{bucket: "bkt", endpoint: "http://rustfs:9000", region: "us-east-1", usePath: true}
	endpoint, err := checkMergeRoute(s3.New(s3.Options{Region: f.region, BaseEndpoint: aws.String(f.endpoint), UsePathStyle: true}), f)
	require.NoError(t, err)
	require.Equal(t, f.endpoint, endpoint)
}

// otherHostResolver resolves every bucket to a host the merge engine is not
// configured with.
type otherHostResolver struct{}

func (otherHostResolver) ResolveEndpoint(context.Context, s3.EndpointParameters) (smithyendpoints.Endpoint, error) {
	u, _ := url.Parse("https://other.storage.example/bkt")
	return smithyendpoints.Endpoint{URI: *u}, nil
}

// TestCheckMergeRouteComparesTheBucketURL: flags, scheme and addressing that
// agree are not enough when the client resolves the bucket to another host
// than the merge engine reads it from.
func TestCheckMergeRouteComparesTheBucketURL(t *testing.T) {
	f := s3Flags{bucket: "bkt", endpoint: "https://s3.example", region: "us-east-1", useSSL: true, usePath: true}
	o := s3.Options{Region: f.region, BaseEndpoint: aws.String(f.endpoint), UsePathStyle: true, EndpointResolverV2: otherHostResolver{}}
	_, err := checkMergeRoute(s3.New(o), f)
	require.ErrorIs(t, err, ErrMergeRouteMismatch)
	require.ErrorContains(t, err, "https://other.storage.example/bkt")
	require.ErrorContains(t, err, "https://s3.example/bkt")
}

// recordConnects is an HTTP proxy that records the target of each CONNECT
// and refuses it, so nothing leaves the host.
func recordConnects(t *testing.T) (addr string, targets <-chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	ch := make(chan string, 64)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			line, _ := bufio.NewReader(c).ReadString('\n')
			if fields := strings.Fields(line); len(fields) > 1 && fields[0] == "CONNECT" {
				select {
				case ch <- fields[1]:
				default:
				}
			}
			_ = c.Close()
		}
	}()
	return ln.Addr().String(), ch
}

// TestMergeEngineReadsTheSDKRoute exercises the effective routes (R4-F1): the
// merge engine the compactor opens with checkMergeRoute's endpoint reads an
// object of the bucket from the host and URL the SDK client sends its own
// request for that object to. DuckDB left without an endpoint would read
// bkt.s3.cn-north-1.amazonaws.com in AWS China, where the SDK reads
// bkt.s3.cn-north-1.amazonaws.com.cn. The reads go through a local proxy that
// records and refuses them.
func TestMergeEngineReadsTheSDKRoute(t *testing.T) {
	setStaticS3Env(t)
	proxy, targets := recordConnects(t)
	for _, tc := range []struct {
		region  string
		usePath bool
	}{{"cn-north-1", false}, {"cn-north-1", true}, {"us-east-1", false}} {
		f := s3Flags{bucket: "bkt", region: tc.region, useSSL: true, usePath: tc.usePath}
		o := s3.Options{Region: tc.region, UsePathStyle: tc.usePath}
		endpoint, err := checkMergeRoute(s3.New(o), f)
		require.NoError(t, err)
		sent := sdkObjectURL(t, o, "bkt", "k.parquet")

		exporter, err := openMergeEngine(context.Background(), &compactorOptions{s3: f}, endpoint, zap.NewNop())
		require.NoError(t, err)
		for _, set := range []string{"SET http_proxy='" + proxy + "'", "SET http_retries=0", "SET http_timeout=10"} {
			_, err = exporter.DB.Exec(set)
			require.NoError(t, err, set)
		}
		_, err = exporter.DB.Exec("SELECT * FROM read_parquet('s3://bkt/k.parquet')")
		_ = exporter.DB.Close()
		require.Error(t, err, "the proxy refuses every read")
		require.ErrorContains(t, err, sent.String(), "%s path-style=%t: the merge engine reads the SDK's URL", tc.region, tc.usePath)
		select {
		case got := <-targets:
			require.Equal(t, sent.Host+":443", got, "%s path-style=%t", tc.region, tc.usePath)
		case <-time.After(5 * time.Second):
			t.Fatalf("%s path-style=%t: the merge engine sent nothing through the proxy", tc.region, tc.usePath)
		}
		for len(targets) > 0 {
			<-targets
		}
	}
}
