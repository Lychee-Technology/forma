package production

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/lychee-technology/forma/internal/cdc"
)

// PausingS3 wraps the cluster's real S3 client and blocks the first call
// matching Op/KeyContains until Resume is called, holding a real CDC flush
// open mid-pipeline so a test can mutate rows inside the race window (#182).
// Pausing on S3OpCopy suspends the flush after dirty-ID selection, snapshot
// capture, and the DuckDB export, but before MarkFlushedIDsAtSnapshot — the
// window the changed_at <= snapshot guard protects.
//
// Only the first matching call pauses; later matches pass straight through.
// The paused call also unblocks when its context is cancelled, so a timed-out
// flush returns instead of leaking a goroutine.
type PausingS3 struct {
	Inner       cdc.S3FullClient
	Op          S3Op
	KeyContains string

	reached    chan struct{}
	resume     chan struct{}
	pauseOnce  sync.Once
	resumeOnce sync.Once
}

// NewPausingS3 returns a decorator that pauses the first call matching op on
// any key.
func NewPausingS3(inner cdc.S3FullClient, op S3Op) *PausingS3 {
	return &PausingS3{
		Inner:   inner,
		Op:      op,
		reached: make(chan struct{}),
		resume:  make(chan struct{}),
	}
}

// Reached is closed when the paused call has been intercepted; the test waits
// on it before mutating.
func (p *PausingS3) Reached() <-chan struct{} {
	return p.reached
}

// Resume releases the paused call. It is idempotent so tests can register it
// with t.Cleanup as a safety net against hangs on early failures.
func (p *PausingS3) Resume() {
	p.resumeOnce.Do(func() { close(p.resume) })
}

func (p *PausingS3) intercept(ctx context.Context, op S3Op, key string) error {
	if op != p.Op {
		return nil
	}
	if p.KeyContains != "" && !strings.Contains(key, p.KeyContains) {
		return nil
	}
	first := false
	p.pauseOnce.Do(func() {
		first = true
		close(p.reached)
	})
	if !first {
		return nil
	}
	select {
	case <-p.resume:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("paused s3 %s %q: %w", op, key, ctx.Err())
	}
}

func (p *PausingS3) CopyObject(ctx context.Context, in *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	if err := p.intercept(ctx, S3OpCopy, aws.ToString(in.Key)); err != nil {
		return nil, err
	}
	return p.Inner.CopyObject(ctx, in, optFns...)
}

func (p *PausingS3) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if err := p.intercept(ctx, S3OpDelete, aws.ToString(in.Key)); err != nil {
		return nil, err
	}
	return p.Inner.DeleteObject(ctx, in, optFns...)
}

func (p *PausingS3) GetObject(ctx context.Context, in *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if err := p.intercept(ctx, S3OpGet, aws.ToString(in.Key)); err != nil {
		return nil, err
	}
	return p.Inner.GetObject(ctx, in, optFns...)
}

func (p *PausingS3) PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if err := p.intercept(ctx, S3OpPut, aws.ToString(in.Key)); err != nil {
		return nil, err
	}
	return p.Inner.PutObject(ctx, in, optFns...)
}

var _ cdc.S3FullClient = (*PausingS3)(nil)
