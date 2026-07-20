package reconcile

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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
