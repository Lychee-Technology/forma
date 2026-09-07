package federated

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/cdc"
)

// scriptedPrefixClient answers ListObjectsV2 with a fixed page sequence,
// records the token each call forwarded, and records every DeleteObject key.
type scriptedPrefixClient struct {
	pages   []*s3.ListObjectsV2Output
	tokens  []*string
	deleted []string
}

func (c *scriptedPrefixClient) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	c.tokens = append(c.tokens, in.ContinuationToken)
	if len(c.tokens) > len(c.pages) {
		return nil, errors.New("unexpected ListObjectsV2 call")
	}
	return c.pages[len(c.tokens)-1], nil
}

func (c *scriptedPrefixClient) DeleteObject(_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	c.deleted = append(c.deleted, aws.ToString(in.Key))
	return &s3.DeleteObjectOutput{}, nil
}

func page(truncated bool, token *string, keys ...string) *s3.ListObjectsV2Output {
	out := &s3.ListObjectsV2Output{IsTruncated: aws.Bool(truncated), NextContinuationToken: token}
	for _, k := range keys {
		out.Contents = append(out.Contents, types.Object{Key: aws.String(k)})
	}
	return out
}

// A tier spanning more than one ListObjectsV2 page is counted in full, with
// the ".parquet" filter applied on every page (#521: the harness used to
// read only the first page).
func TestListParquetKeys_FollowsEveryPage(t *testing.T) {
	client := &scriptedPrefixClient{pages: []*s3.ListObjectsV2Output{
		page(true, aws.String("page-2"), "p/1/base/a.parquet", "p/1/base/_manifest.json"),
		page(false, nil, "p/1/base/b.parquet"),
	}}

	files, err := listParquetKeys(context.Background(), client, "bkt", "p/1/base/")
	require.NoError(t, err)
	require.Equal(t, []string{"p/1/base/a.parquet", "p/1/base/b.parquet"}, files)
	require.Equal(t, []*string{nil, aws.String("page-2")}, client.tokens)
}

// An unfollowable page fails the listing instead of reporting a short file
// count that a test assertion would then trust.
func TestListParquetKeys_UnfollowableTruncationFailsClosed(t *testing.T) {
	client := &scriptedPrefixClient{pages: []*s3.ListObjectsV2Output{
		page(true, nil, "p/1/base/a.parquet"),
	}}

	files, err := listParquetKeys(context.Background(), client, "bkt", "p/1/base/")
	require.ErrorIs(t, err, cdc.ErrIncompleteObjectListing)
	require.Nil(t, files)
}

// A cleanup deletes objects from every page, so a prefix larger than one
// page cannot leak later-page objects into the next test case (#521).
func TestDeleteAllUnderPrefix_DeletesEveryPage(t *testing.T) {
	client := &scriptedPrefixClient{pages: []*s3.ListObjectsV2Output{
		page(true, aws.String("page-2"), "p/1/base/a.parquet", "p/1/delta/b.parquet"),
		page(false, nil, "p/1/delta/c.parquet"),
	}}

	err := deleteAllUnderPrefix(context.Background(), client, "bkt", "p/")
	require.NoError(t, err)
	require.Equal(t, []string{"p/1/base/a.parquet", "p/1/delta/b.parquet", "p/1/delta/c.parquet"}, client.deleted)
	require.Equal(t, []*string{nil, aws.String("page-2")}, client.tokens)
}

// A cleanup whose listing cannot be followed reports the failure rather than
// deleting a partial set and returning nil.
func TestDeleteAllUnderPrefix_UnfollowableTruncationFailsClosed(t *testing.T) {
	client := &scriptedPrefixClient{pages: []*s3.ListObjectsV2Output{
		page(true, aws.String(""), "p/1/base/a.parquet"),
	}}

	err := deleteAllUnderPrefix(context.Background(), client, "bkt", "p/")
	require.ErrorIs(t, err, cdc.ErrIncompleteObjectListing)
	require.Empty(t, client.deleted, "nothing may be deleted on the strength of an incomplete listing")
}
