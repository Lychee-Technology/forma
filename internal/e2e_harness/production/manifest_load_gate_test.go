package production

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// failingStore is a manifestv2.Store whose Get fails with a fixed error for
// every key, and which counts its calls; Put is never expected.
type failingStore struct {
	err   error
	calls int
}

func (s *failingStore) Get(_ context.Context, key string) ([]byte, error) {
	s.calls++
	return nil, fmt.Errorf("get %s: %w", key, s.err)
}

func (s *failingStore) Put(context.Context, string, io.ReadSeeker) error {
	return errors.New("unexpected Put")
}

// errNotNotFound stands in for any store failure that is not a confirmed
// missing object (NoSuchBucket, permission, transport).
var errNotNotFound = errors.New("The specified bucket does not exist")

// publishedRecord is a PUBLISHED record of schema 2 naming a manifest no
// store below holds.
func publishedRecord() publication.Record {
	return publication.Record{
		SchemaID: 2, Epoch: 1, State: publication.Published, Generation: 3, ParentGeneration: 2,
		ManifestKey: "e2e/env/2/e1/manifest/g3.json", ManifestSHA256: "00",
		Lineage: uuid.New(), Token: uuid.New(),
	}
}

// The harness reads a schema's inventory through manifestView, which may
// treat only an UNPUBLISHED record as "nothing published" (#464, #896). A
// published generation whose manifest is missing is loss, and every other
// store failure has to surface too, or a bucket outage reads as an empty
// inventory and the assertions built on top silently lose coverage.
func TestManifestView_UnpublishedIsAbsentWithoutAStoreCall(t *testing.T) {
	store := &failingStore{err: manifestv2.ErrObjectNotFound}
	got, err := manifestView(context.Background(), store, publication.Record{SchemaID: 2, Epoch: 1, State: publication.Unpublished})
	require.NoError(t, err)
	require.Nil(t, got)
	require.Zero(t, store.calls)
}

func TestManifestView_MissingPublishedManifestIsLoss(t *testing.T) {
	store := &failingStore{err: manifestv2.ErrObjectNotFound}
	got, err := manifestView(context.Background(), store, publishedRecord())
	require.ErrorIs(t, err, manifestv2.ErrManifestMissing)
	require.Nil(t, got)
}

func TestManifestView_OtherStoreFailureSurfaces(t *testing.T) {
	store := &failingStore{err: errNotNotFound}
	got, err := manifestView(context.Background(), store, publishedRecord())
	require.ErrorIs(t, err, errNotNotFound)
	require.NotErrorIs(t, err, manifestv2.ErrManifestMissing)
	require.Nil(t, got)
}

func TestManifestView_UnreadableRecordSurfaces(t *testing.T) {
	store := &failingStore{err: manifestv2.ErrObjectNotFound}
	for state, want := range map[publication.State]error{
		publication.AwaitingInit: publication.ErrAwaitingInit,
		publication.Dropped:      publication.ErrDropped,
	} {
		got, err := manifestView(context.Background(), store, publication.Record{SchemaID: 2, Epoch: 1, State: state})
		require.ErrorIs(t, err, want, "state %s", state)
		require.Nil(t, got)
	}
	require.Zero(t, store.calls)
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
// not collapse into a missing manifest.
func TestManifestView_NoSuchBucketSurfaces(t *testing.T) {
	store := &manifestv2.S3Store{Client: noSuchBucketS3{}, Bucket: "gone"}
	got, err := manifestView(context.Background(), store, publishedRecord())
	require.Error(t, err)
	require.NotErrorIs(t, err, manifestv2.ErrManifestMissing)
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "NoSuchBucket", apiErr.ErrorCode())
	require.Nil(t, got)
}
