package manifestv2

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/require"
)

// fakeHeadClient answers HeadObject as head says, recording what it was
// asked.
type fakeHeadClient struct {
	head        func(key string) (*s3.HeadObjectOutput, error)
	headKeys    []string
	headBuckets []string
}

func (f *fakeHeadClient) HeadObject(_ context.Context, params *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	f.headBuckets = append(f.headBuckets, aws.ToString(params.Bucket))
	f.headKeys = append(f.headKeys, aws.ToString(params.Key))
	if f.head == nil {
		return &s3.HeadObjectOutput{}, nil
	}
	return f.head(aws.ToString(params.Key))
}

// headResponseError is the shape the SDK returns for an HTTP error response
// whose body carries no S3 error code, which *types.NotFound alone misses.
func headResponseError(status int) error {
	return &awshttp.ResponseError{
		ResponseError: &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
			Err:      fmt.Errorf("http response error StatusCode: %d", status),
		},
		RequestID: "req-1",
	}
}

func TestS3ExistsFound(t *testing.T) {
	client := &fakeHeadClient{}
	ok, err := S3Exists(client, "bkt")(context.Background(), "data/7/e1/base/a.parquet")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []string{"bkt"}, client.headBuckets)
	require.Equal(t, []string{"data/7/e1/base/a.parquet"}, client.headKeys)
}

// A confirmed absence is (false, nil): the SDK's NotFound, or a bare 404
// some S3-compatible stores answer a HEAD with.
func TestS3ExistsConfirmedAbsence(t *testing.T) {
	for name, answer := range map[string]error{
		"not found": &types.NotFound{},
		"http 404":  headResponseError(http.StatusNotFound),
	} {
		t.Run(name, func(t *testing.T) {
			client := &fakeHeadClient{head: func(string) (*s3.HeadObjectOutput, error) { return nil, answer }}
			ok, err := S3Exists(client, "bkt")(context.Background(), "data/7/e1/base/gone.parquet")
			require.NoError(t, err)
			require.False(t, ok)
		})
	}
}

// A probe failure is not proof of absence: anything but a confirmed 404
// propagates, so the reader never fabricates a data-loss report.
func TestS3ExistsOtherErrorPropagates(t *testing.T) {
	for name, injected := range map[string]error{
		"transport":  errors.New("connection refused"),
		"http500":    headResponseError(http.StatusInternalServerError),
		"http403":    headResponseError(http.StatusForbidden),
		"apiTimeout": fmt.Errorf("wrapped: %w", errors.New("context deadline exceeded")),
	} {
		t.Run(name, func(t *testing.T) {
			client := &fakeHeadClient{head: func(string) (*s3.HeadObjectOutput, error) { return nil, injected }}
			ok, err := S3Exists(client, "bkt")(context.Background(), "data/7/e1/base/a.parquet")
			require.Error(t, err)
			require.False(t, ok)
			require.ErrorIs(t, err, injected)
			require.ErrorContains(t, err, "data/7/e1/base/a.parquet")
		})
	}
}
