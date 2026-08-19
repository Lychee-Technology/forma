package cdc

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestRunnerS3RuntimeDistinctEndpointsCoexist pins the eviction boundary
// (#331): supersession is per group (region/endpoint/path-style). Two configs
// with different endpoints are different groups — the second must not evict
// the first, and the first must still be a cache hit afterwards.
func TestRunnerS3RuntimeDistinctEndpointsCoexist(t *testing.T) {
	loadCalls := 0
	stubLoadAWSConfig(t, func(context.Context, ...func(*config.LoadOptions) error) (aws.Config, error) {
		loadCalls++
		return aws.Config{Region: "us-east-1"}, nil
	})
	stubNewS3Client(t, func(aws.Config, string, bool) *s3.Client { return &s3.Client{} })

	runner := NewRunner(zap.NewNop())
	cfgA := CDCConfig{S3Region: "us-east-1", S3Endpoint: "http://minio-a:9000"}
	cfgB := CDCConfig{S3Region: "us-east-1", S3Endpoint: "http://minio-b:9000"}

	rtA, err := runner.getOrCreateS3Runtime(context.Background(), cfgA)
	require.NoError(t, err)
	rtB, err := runner.getOrCreateS3Runtime(context.Background(), cfgB)
	require.NoError(t, err)
	require.NotSame(t, rtA, rtB)
	require.Len(t, runner.s3Runtimes, 2)

	rtA2, err := runner.getOrCreateS3Runtime(context.Background(), cfgA)
	require.NoError(t, err)
	require.Same(t, rtA, rtA2)
	require.Equal(t, 2, loadCalls)
}
