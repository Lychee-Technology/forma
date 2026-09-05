package production

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifest"
)

// failingStore is a manifest.Store whose Load fails with a fixed error for
// every path; Save is never expected.
type failingStore struct{ err error }

func (s *failingStore) Load(_ context.Context, path string) ([]byte, string, error) {
	return nil, "", fmt.Errorf("load %s: %w", path, s.err)
}

func (s *failingStore) Save(_ context.Context, _ string, _ []byte, _ string) (string, error) {
	return "", errors.New("unexpected Save")
}

// errNotNotFound stands in for any store failure that is not a confirmed
// missing object (NoSuchBucket, permission, transport).
var errNotNotFound = errors.New("The specified bucket does not exist")

var testResolver = manifest.PathResolver{Prefix: "manifests", PathTemplate: "manifest/{{.SchemaID}}.json"}

// The harness helpers wrap manifest.Load and may only treat a CONFIRMED
// missing object as "no manifest yet" (#464). Every other store failure has
// to surface, or a bucket outage reads as an empty manifest map / a plain
// init and the assertions built on top silently lose coverage.
func TestLoadManifestsFrom_MissingManifestIsAbsent(t *testing.T) {
	store := &failingStore{err: manifest.ErrObjectNotFound}
	got, err := loadManifestsFrom(context.Background(), store, testResolver)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestLoadManifestsFrom_OtherStoreFailureSurfaces(t *testing.T) {
	store := &failingStore{err: errNotNotFound}
	got, err := loadManifestsFrom(context.Background(), store, testResolver)
	require.Error(t, err)
	require.ErrorIs(t, err, errNotNotFound)
	require.False(t, manifest.IsNotFound(err))
	require.Nil(t, got)
}

func TestLoadKeptDeltaEntries_MissingManifestKeepsNothing(t *testing.T) {
	store := &failingStore{err: manifest.ErrObjectNotFound}
	entries, err := loadKeptDeltaEntries(context.Background(), store, "manifest/2.json")
	require.NoError(t, err)
	require.Nil(t, entries)
}

func TestLoadKeptDeltaEntries_OtherStoreFailureSurfaces(t *testing.T) {
	store := &failingStore{err: errNotNotFound}
	entries, err := loadKeptDeltaEntries(context.Background(), store, "manifest/2.json")
	require.Error(t, err)
	require.ErrorIs(t, err, errNotNotFound)
	require.Nil(t, entries)
}

// noSuchBucketS3 is an S3 client whose GetObject answers the way the SDK does
// for a missing bucket: a 404 carrying the NoSuchBucket code and the
// "does not exist" text the pre-#464 substring classifier mistook for a
// missing key.
type noSuchBucketS3 struct{}

func (noSuchBucketS3) GetObject(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, &smithy.OperationError{
		ServiceID:     "S3",
		OperationName: "GetObject",
		Err: &awshttp.ResponseError{ResponseError: &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusNotFound}},
			Err:      &smithy.GenericAPIError{Code: "NoSuchBucket", Message: "The specified bucket does not exist"},
		}},
	}
}

func (noSuchBucketS3) PutObject(_ context.Context, _ *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return nil, errors.New("unexpected PutObject")
}

// End to end through the real S3Store: NoSuchBucket must reach the caller,
// not collapse into "no manifest yet".
func TestLoadManifestsFrom_NoSuchBucketSurfaces(t *testing.T) {
	store := &manifest.S3Store{Client: noSuchBucketS3{}, Bucket: "gone"}
	got, err := loadManifestsFrom(context.Background(), store, testResolver)
	require.Error(t, err)
	require.False(t, manifest.IsNotFound(err))
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "NoSuchBucket", apiErr.ErrorCode())
	require.Nil(t, got)
}

func TestLoadKeptDeltaEntries_NoSuchBucketSurfaces(t *testing.T) {
	store := &manifest.S3Store{Client: noSuchBucketS3{}, Bucket: "gone"}
	entries, err := loadKeptDeltaEntries(context.Background(), store, "manifest/2.json")
	require.Error(t, err)
	require.False(t, manifest.IsNotFound(err))
	require.Nil(t, entries)
}
