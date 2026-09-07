package reconcile

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/lychee-technology/forma/internal/cdc"
)

type pagingS3 struct {
	pages   [][]types.Object
	calls   []s3.ListObjectsV2Input
	listErr error
}

func (p *pagingS3) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if p.listErr != nil {
		return nil, p.listErr
	}
	p.calls = append(p.calls, *in)
	page := 0
	if in.ContinuationToken != nil {
		if _, err := fmt.Sscanf(*in.ContinuationToken, "page-%d", &page); err != nil {
			return nil, fmt.Errorf("bad continuation token %q", *in.ContinuationToken)
		}
	}
	out := &s3.ListObjectsV2Output{Contents: p.pages[page]}
	if page+1 < len(p.pages) {
		out.IsTruncated = aws.Bool(true)
		out.NextContinuationToken = aws.String(fmt.Sprintf("page-%d", page+1))
	}
	return out, nil
}

func (p *pagingS3) DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return &s3.DeleteObjectOutput{}, nil
}

func TestS3ObjectStore_ListObjectsPaginates(t *testing.T) {
	mod := time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)
	fake := &pagingS3{pages: [][]types.Object{
		{
			{Key: aws.String("data/7/a.parquet"), Size: aws.Int64(10), LastModified: aws.Time(mod)},
			{Key: aws.String("data/7/b.parquet"), Size: aws.Int64(20), LastModified: aws.Time(mod)},
		},
		{
			{Key: aws.String("data/7/c.parquet"), Size: aws.Int64(30), LastModified: aws.Time(mod)},
		},
	}}

	store := &S3ObjectStore{Client: fake, Bucket: "bkt"}
	objs, err := store.ListObjects(context.Background(), "data/7/")
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}

	assertKeys(t, "listed keys", objectKeys(objs), []string{
		"data/7/a.parquet", "data/7/b.parquet", "data/7/c.parquet",
	})
	if objs[0].Size != 10 || !objs[0].LastModified.Equal(mod) {
		t.Fatalf("ObjectInfo metadata not carried: %+v", objs[0])
	}
	if len(fake.calls) != 2 {
		t.Fatalf("expected 2 paginated calls, got %d", len(fake.calls))
	}
	if got := aws.ToString(fake.calls[0].Prefix); got != "data/7/" {
		t.Fatalf("first call prefix = %q, want data/7/", got)
	}
	if fake.calls[0].ContinuationToken != nil {
		t.Fatalf("first call must not carry a continuation token")
	}
	if got := aws.ToString(fake.calls[1].ContinuationToken); got != "page-1" {
		t.Fatalf("second call token = %q, want page-1", got)
	}
}

// scriptedS3 answers ListObjectsV2 with a fixed sequence of pages, whatever
// token the caller forwards, so malformed pagination can be replayed.
type scriptedS3 struct {
	pages []*s3.ListObjectsV2Output
	calls int
}

func (p *scriptedS3) ListObjectsV2(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if p.calls >= len(p.pages) {
		return nil, fmt.Errorf("unexpected ListObjectsV2 call %d", p.calls+1)
	}
	out := p.pages[p.calls]
	p.calls++
	return out, nil
}

func (p *scriptedS3) DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return &s3.DeleteObjectOutput{}, nil
}

func listPage(truncated bool, token *string, keys ...string) *s3.ListObjectsV2Output {
	out := &s3.ListObjectsV2Output{IsTruncated: aws.Bool(truncated), NextContinuationToken: token}
	for _, k := range keys {
		out.Contents = append(out.Contents, types.Object{Key: aws.String(k)})
	}
	return out
}

// A page that claims to be truncated but carries no usable continuation
// token, or repeats one the listing already used, cannot be followed to the
// end. The store fails closed with cdc.ErrIncompleteObjectListing instead of
// handing the reconciler the objects seen so far as if they were the whole
// prefix (#521): a short listing under-reports orphans and over-reports
// dangling entries while the run still says "ok".
func TestS3ObjectStore_ListObjectsUnfollowableTruncationFailsClosed(t *testing.T) {
	cases := []struct {
		name  string
		pages []*s3.ListObjectsV2Output
		calls int
		want  string
	}{
		{
			name:  "truncated without token",
			pages: []*s3.ListObjectsV2Output{listPage(true, nil, "data/7/a.parquet")},
			calls: 1,
			want:  "page 1 is truncated but carries no continuation token",
		},
		{
			name:  "truncated with empty token",
			pages: []*s3.ListObjectsV2Output{listPage(true, aws.String(""), "data/7/a.parquet")},
			calls: 1,
			want:  "page 1 is truncated but carries no continuation token",
		},
		{
			name: "repeated token",
			pages: []*s3.ListObjectsV2Output{
				listPage(true, aws.String("page-2"), "data/7/a.parquet"),
				listPage(true, aws.String("page-2"), "data/7/b.parquet"),
			},
			calls: 2,
			want:  `page 2 repeats continuation token "page-2"`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake := &scriptedS3{pages: c.pages}
			store := &S3ObjectStore{Client: fake, Bucket: "bkt"}
			objs, err := store.ListObjects(context.Background(), "data/7/")
			if !errors.Is(err, cdc.ErrIncompleteObjectListing) {
				t.Fatalf("ListObjects error = %v, want cdc.ErrIncompleteObjectListing", err)
			}
			want := "list s3 objects in bucket bkt: list objects under data/7/: " + c.want
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("ListObjects error = %q, want it to contain %q", err.Error(), want)
			}
			if objs != nil {
				t.Fatalf("an incomplete listing must not hand back a partial object set, got %v", objectKeys(objs))
			}
			if fake.calls != c.calls {
				t.Fatalf("ListObjectsV2 calls = %d, want %d", fake.calls, c.calls)
			}
		})
	}
}
