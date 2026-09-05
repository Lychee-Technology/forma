package manifest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"testing/fstest"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/require"
)

// noSuchBucketErr is the shape the SDK returns for a GetObject against a
// missing bucket: NoSuchBucket is not modeled on GetObject, so it arrives as
// a generic API error whose message contains "does not exist" — the exact
// text the old substring classifier mistook for a missing key (#464).
func noSuchBucketErr() error {
	return &smithy.OperationError{
		ServiceID:     "S3",
		OperationName: "GetObject",
		Err: &awshttp.ResponseError{
			ResponseError: &smithyhttp.ResponseError{
				Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusNotFound}},
				Err: &smithy.GenericAPIError{
					Code:    "NoSuchBucket",
					Message: "The specified bucket does not exist",
				},
			},
		},
	}
}

func noSuchKeyErr() error {
	return &smithy.OperationError{
		ServiceID:     "S3",
		OperationName: "GetObject",
		Err: &awshttp.ResponseError{
			ResponseError: &smithyhttp.ResponseError{
				Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusNotFound}},
				Err:      &types.NoSuchKey{},
			},
		},
	}
}

// bare404Err is an HTTP 404 with no deserialized error code at all.
func bare404Err() error {
	return &awshttp.ResponseError{ResponseError: &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusNotFound}},
		Err:      errors.New("404"),
	}}
}

func TestIsNotFound(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"sentinel", ErrObjectNotFound, true},
		{"wrapped sentinel", fmt.Errorf("load manifest: %w", ErrObjectNotFound), true},
		// Text that merely looks like a missing key is not a missing key.
		{"nosuchkey text only", errors.New("NoSuchKey: not found"), false},
		{"does not exist text only", errors.New("The specified bucket does not exist"), false},
		{"NoSuchBucket api error", noSuchBucketErr(), false},
		// The raw SDK error is a store-level detail: only a Store that has
		// translated it into ErrObjectNotFound proves the key is absent.
		{"raw NoSuchKey api error", noSuchKeyErr(), false},
		{"transport error", errors.New("connection reset"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsNotFound(tt.err), "IsNotFound(%v)", tt.err)
		})
	}
}

type failingGetClient struct {
	capturingS3Client
	getErr error
}

func (c *failingGetClient) GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, c.getErr
}

func TestS3StoreLoad_NoSuchKeyClassifiesAsNotFound(t *testing.T) {
	store := &S3Store{Client: &failingGetClient{getErr: noSuchKeyErr()}, Bucket: "bkt"}

	_, _, err := store.Load(context.Background(), "manifest/7.json")
	require.Error(t, err)
	require.True(t, IsNotFound(err), "NoSuchKey must classify as absent, got %v", err)
	var nsk *types.NoSuchKey
	require.ErrorAs(t, err, &nsk, "the SDK error must stay in the chain")
	require.Contains(t, err.Error(), "bkt/manifest/7.json")
}

func TestS3StoreLoad_NoSuchBucketIsNotNotFound(t *testing.T) {
	store := &S3Store{Client: &failingGetClient{getErr: noSuchBucketErr()}, Bucket: "bkt"}

	_, _, err := store.Load(context.Background(), "manifest/7.json")
	require.Error(t, err)
	require.False(t, IsNotFound(err), "NoSuchBucket must surface as a store failure, got %v", err)
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "NoSuchBucket", apiErr.ErrorCode())
}

func TestS3StoreLoad_Bare404IsNotNotFound(t *testing.T) {
	// A 404 status alone cannot separate NoSuchKey from NoSuchBucket; only a
	// deserialized NoSuchKey code proves the key is absent.
	store := &S3Store{Client: &failingGetClient{getErr: bare404Err()}, Bucket: "bkt"}

	_, _, err := store.Load(context.Background(), "manifest/7.json")
	require.Error(t, err)
	require.False(t, IsNotFound(err), "bare 404 must not classify as absent, got %v", err)
}

func TestS3StoreLoad_TransportErrorSurfaces(t *testing.T) {
	cause := errors.New("connection reset")
	store := &S3Store{Client: &failingGetClient{getErr: cause}, Bucket: "bkt"}

	_, _, err := store.Load(context.Background(), "manifest/7.json")
	require.ErrorIs(t, err, cause)
	require.False(t, IsNotFound(err))
}

func TestLoadOrCreate_NoSuchKeyCreatesEmptyManifest(t *testing.T) {
	store := &S3Store{Client: &failingGetClient{getErr: noSuchKeyErr()}, Bucket: "bkt"}

	m, etag, err := LoadOrCreate(context.Background(), store, "manifest/7.json", 7)
	require.NoError(t, err)
	require.Empty(t, etag)
	require.Equal(t, int16(7), m.SchemaID)
	require.Empty(t, m.Files)
}

func TestLoadOrCreate_NoSuchBucketSurfaces(t *testing.T) {
	store := &S3Store{Client: &failingGetClient{getErr: noSuchBucketErr()}, Bucket: "bkt"}

	m, _, err := LoadOrCreate(context.Background(), store, "manifest/7.json", 7)
	require.Error(t, err, "a bucket-level failure must not become an empty manifest")
	require.Nil(t, m)
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "NoSuchBucket", apiErr.ErrorCode())
}

func TestQuerySource_StoreFailureDoesNotFallBackToGlob(t *testing.T) {
	store := &S3Store{Client: &failingGetClient{getErr: noSuchBucketErr()}, Bucket: "bkt"}
	src := testQuerySource(store)
	src.Fallback = func(schemaID int16) string { return fmt.Sprintf("s3://bkt/p/%d/*.parquet", schemaID) }

	paths, _, err := src.Paths(context.Background(), 7)
	require.Error(t, err, "a failed manifest load must fail the read, not take the glob")
	require.Nil(t, paths)
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "NoSuchBucket", apiErr.ErrorCode())
}

func TestFSStoreLoad_MissingFileIsNotFound(t *testing.T) {
	store := &FSStore{Root: fstest.MapFS{}}

	_, _, err := store.Load(context.Background(), "missing.json")
	require.Error(t, err)
	require.True(t, IsNotFound(err), "fs.ErrNotExist must classify as absent, got %v", err)
}
