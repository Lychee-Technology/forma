package production

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lychee-technology/forma/internal/manifestv2"
)

// stubS3 always succeeds, standing in for the real client.
type stubS3 struct{}

func (s *stubS3) CopyObject(_ context.Context, _ *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	return &s3.CopyObjectOutput{}, nil
}

func (s *stubS3) DeleteObject(_ context.Context, _ *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return &s3.DeleteObjectOutput{}, nil
}

func (s *stubS3) HeadObject(_ context.Context, _ *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return &s3.HeadObjectOutput{}, nil
}

func (s *stubS3) GetObject(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return &s3.GetObjectOutput{}, nil
}

func (s *stubS3) PutObject(_ context.Context, _ *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return &s3.PutObjectOutput{}, nil
}

func TestFaultInjectingS3MatchesOpAndKey(t *testing.T) {
	ctx := context.Background()
	f := &FaultInjectingS3{Inner: &stubS3{}, Fault: S3Fault{Op: S3OpPut, KeyContains: "manifest/"}}

	if _, err := f.GetObject(ctx, &s3.GetObjectInput{Key: aws.String("p/manifest/20.json")}); err != nil {
		t.Fatalf("op mismatch must pass through, got %v", err)
	}
	if _, err := f.PutObject(ctx, &s3.PutObjectInput{Key: aws.String("p/20/data.parquet")}); err != nil {
		t.Fatalf("key mismatch must pass through, got %v", err)
	}
	_, err := f.PutObject(ctx, &s3.PutObjectInput{Key: aws.String("p/manifest/20.json")})
	if err == nil {
		t.Fatal("matching PutObject must fail")
	}
	if !strings.Contains(err.Error(), "e2e injected s3 fault") {
		t.Fatalf("unexpected error text: %v", err)
	}
	if f.Injected() != 1 {
		t.Fatalf("Injected() = %d, want 1", f.Injected())
	}
	// A reader treats only a confirmed missing object as ErrObjectNotFound,
	// and a published generation whose manifest is missing as loss: an
	// injected fault must never classify as a missing object (#464).
	g := &FaultInjectingS3{Inner: &stubS3{}, Fault: S3Fault{Op: S3OpGet, KeyContains: "manifest/"}}
	_, err = (&manifestv2.S3Store{Client: g, Bucket: "b"}).Get(ctx, "p/20/e1/manifest/g1.json")
	if err == nil || errors.Is(err, manifestv2.ErrObjectNotFound) {
		t.Fatalf("injected get fault %v must fail and not classify as a missing object", err)
	}
	if g.Injected() != 1 {
		t.Fatalf("get fault Injected() = %d, want 1", g.Injected())
	}
}

func TestFaultInjectingS3SkipMatches(t *testing.T) {
	ctx := context.Background()
	f := &FaultInjectingS3{Inner: &stubS3{}, Fault: S3Fault{Op: S3OpCopy, SkipMatches: 1}}

	if _, err := f.CopyObject(ctx, &s3.CopyObjectInput{Key: aws.String("a.parquet")}); err != nil {
		t.Fatalf("first match must be skipped, got %v", err)
	}
	if _, err := f.CopyObject(ctx, &s3.CopyObjectInput{Key: aws.String("b.parquet")}); err == nil {
		t.Fatal("second match must fail")
	}
	if _, err := f.CopyObject(ctx, &s3.CopyObjectInput{Key: aws.String("c.parquet")}); err == nil {
		t.Fatal("later matches must keep failing")
	}
	if f.Injected() != 2 {
		t.Fatalf("Injected() = %d, want 2", f.Injected())
	}
}
