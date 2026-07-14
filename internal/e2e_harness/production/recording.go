package production

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/lychee-technology/forma/internal/cdc"
)

// RecordingS3 wraps the cluster's real S3 client and counts calls per
// operation so dry-run scenarios can assert that zero mutating calls were
// issued through the Go client (#180). Note DuckDB COPY writes S3 directly
// via httpfs and bypasses this client — the object-inventory diff in the
// test covers that surface.
type RecordingS3 struct {
	Inner cdc.S3FullClient

	mu    sync.Mutex
	calls map[S3Op]int
}

func (r *RecordingS3) record(op S3Op) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.calls == nil {
		r.calls = make(map[S3Op]int)
	}
	r.calls[op]++
}

// MutatingCalls reports how many write-class calls (CopyObject,
// DeleteObject, PutObject) have passed through the client.
func (r *RecordingS3) MutatingCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls[S3OpCopy] + r.calls[S3OpDelete] + r.calls[S3OpPut]
}

func (r *RecordingS3) CopyObject(ctx context.Context, in *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	r.record(S3OpCopy)
	return r.Inner.CopyObject(ctx, in, optFns...)
}

func (r *RecordingS3) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	r.record(S3OpDelete)
	return r.Inner.DeleteObject(ctx, in, optFns...)
}

func (r *RecordingS3) GetObject(ctx context.Context, in *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	r.record(S3OpGet)
	return r.Inner.GetObject(ctx, in, optFns...)
}

func (r *RecordingS3) HeadObject(ctx context.Context, in *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	r.record(S3OpHead)
	return r.Inner.HeadObject(ctx, in, optFns...)
}

func (r *RecordingS3) PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	r.record(S3OpPut)
	return r.Inner.PutObject(ctx, in, optFns...)
}

var _ cdc.S3FullClient = (*RecordingS3)(nil)
