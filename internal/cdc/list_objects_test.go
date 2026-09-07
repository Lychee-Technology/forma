package cdc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

// scriptedListClient answers ListObjectsV2 with a fixed page sequence and
// records the token each call forwarded.
type scriptedListClient struct {
	pages  []*s3.ListObjectsV2Output
	tokens []*string
}

func (c *scriptedListClient) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	c.tokens = append(c.tokens, in.ContinuationToken)
	if len(c.tokens) > len(c.pages) {
		return nil, errors.New("unexpected ListObjectsV2 call")
	}
	return c.pages[len(c.tokens)-1], nil
}

// ForEachObject hands the callback every listed object with its metadata
// intact, across pages, forwarding each page's continuation token (#521:
// reconcile needs Size and LastModified, so the shared paginator must not
// flatten objects to keys).
func TestForEachObject_VisitsEveryObjectAcrossPages(t *testing.T) {
	mod := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	client := &scriptedListClient{pages: []*s3.ListObjectsV2Output{
		{
			Contents: []types.Object{
				{Key: aws.String("data/7/a.parquet"), Size: aws.Int64(10), LastModified: aws.Time(mod)},
			},
			IsTruncated:           aws.Bool(true),
			NextContinuationToken: aws.String("page-2"),
		},
		{
			Contents: []types.Object{
				{Key: aws.String("data/7/b.parquet"), Size: aws.Int64(20), LastModified: aws.Time(mod)},
			},
		},
	}}

	var seen []types.Object
	err := ForEachObject(context.Background(), client, "test-bucket", "data/7/", func(obj types.Object) error {
		seen = append(seen, obj)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, seen, 2)
	require.Equal(t, "data/7/a.parquet", aws.ToString(seen[0].Key))
	require.Equal(t, int64(10), aws.ToInt64(seen[0].Size))
	require.True(t, aws.ToTime(seen[0].LastModified).Equal(mod))
	require.Equal(t, "data/7/b.parquet", aws.ToString(seen[1].Key))
	require.Equal(t, int64(20), aws.ToInt64(seen[1].Size))
	require.Equal(t, []*string{nil, aws.String("page-2")}, client.tokens)
}

// A callback error stops the listing at that page and reaches the caller
// unwrapped, so callers keep the context they attached themselves.
func TestForEachObject_CallbackErrorStopsListing(t *testing.T) {
	client := &scriptedListClient{pages: []*s3.ListObjectsV2Output{
		listPage(true, aws.String("page-2"), "data/7/a.parquet", "data/7/b.parquet"),
		listPage(false, nil, "data/7/c.parquet"),
	}}
	stop := errors.New("stop here")

	var seen []string
	err := ForEachObject(context.Background(), client, "test-bucket", "data/7/", func(obj types.Object) error {
		seen = append(seen, aws.ToString(obj.Key))
		return stop
	})
	require.ErrorIs(t, err, stop)
	require.Equal(t, []string{"data/7/a.parquet"}, seen)
	require.Len(t, client.tokens, 1, "the listing must not fetch another page after the callback failed")
}

// ForEachObject carries the fail-closed rule ListObjectKeys is specified
// against, so every paginator routed through it inherits the guard.
func TestForEachObject_UnfollowableTruncationFailsClosed(t *testing.T) {
	client := &scriptedListClient{pages: []*s3.ListObjectsV2Output{
		listPage(true, nil, "data/7/a.parquet"),
	}}
	var seen int
	err := ForEachObject(context.Background(), client, "test-bucket", "data/7/", func(types.Object) error {
		seen++
		return nil
	})
	require.ErrorIs(t, err, ErrIncompleteObjectListing)
	require.Contains(t, err.Error(), "list objects under data/7/: page 1 is truncated but carries no continuation token")
	require.Equal(t, 0, seen, "a page that cannot be followed must not be delivered to the callback")
}
