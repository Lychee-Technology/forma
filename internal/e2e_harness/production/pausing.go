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
// matching Op until Resume is called, holding a real CDC flush
// open mid-pipeline so a test can mutate rows inside the race window (#182).
// Pausing on S3OpCopy suspends the flush after dirty-ID selection, snapshot
// capture, and the DuckDB export, but before MarkFlushedVersions — the
// window the exact listed-version mark guard protects (#274).
//
// Only the first matching call pauses; later matches pass straight through.
// The paused call also unblocks when its context is cancelled, so a timed-out
// flush returns instead of leaking a goroutine.
type PausingS3 struct {
	Inner cdc.S3FullClient
	Op    S3Op
	// KeyContains narrows the match to keys containing this substring (empty
	// matches any key). Needed when the paused pipeline issues several calls
	// of the same op — e.g. the compaction rewrite copies its new base parquet
	// before the manifest PutObject a test wants to hold open (#188).
	KeyContains string

	reached    chan struct{}
	resume     chan struct{}
	pauseOnce  sync.Once
	resumeOnce sync.Once
}

// NewPausingS3 returns a decorator that pauses the first call matching op on
// any key.
func NewPausingS3(inner cdc.S3FullClient, op S3Op) *PausingS3 {
	return NewPausingS3OnKey(inner, op, "")
}

// NewPausingS3OnKey returns a decorator that pauses the first call matching op
// whose key contains keyContains.
func NewPausingS3OnKey(inner cdc.S3FullClient, op S3Op, keyContains string) *PausingS3 {
	return &PausingS3{
		Inner:       inner,
		Op:          op,
		KeyContains: keyContains,
		reached:     make(chan struct{}),
		resume:      make(chan struct{}),
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
		return ctx.Err()
	}
}

func (p *PausingS3) CopyObject(ctx context.Context, in *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	key := aws.ToString(in.Key)
	if err := p.intercept(ctx, S3OpCopy, key); err != nil {
		return nil, fmt.Errorf("pause CopyObject %q: %w", key, err)
	}
	out, err := p.Inner.CopyObject(ctx, in, optFns...)
	if err != nil {
		return nil, fmt.Errorf("s3 CopyObject %q: %w", key, err)
	}
	return out, nil
}

func (p *PausingS3) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	key := aws.ToString(in.Key)
	if err := p.intercept(ctx, S3OpDelete, key); err != nil {
		return nil, fmt.Errorf("pause DeleteObject %q: %w", key, err)
	}
	out, err := p.Inner.DeleteObject(ctx, in, optFns...)
	if err != nil {
		return nil, fmt.Errorf("s3 DeleteObject %q: %w", key, err)
	}
	return out, nil
}

func (p *PausingS3) GetObject(ctx context.Context, in *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	key := aws.ToString(in.Key)
	if err := p.intercept(ctx, S3OpGet, key); err != nil {
		return nil, fmt.Errorf("pause GetObject %q: %w", key, err)
	}
	out, err := p.Inner.GetObject(ctx, in, optFns...)
	if err != nil {
		return nil, fmt.Errorf("s3 GetObject %q: %w", key, err)
	}
	return out, nil
}

func (p *PausingS3) HeadObject(ctx context.Context, in *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	key := aws.ToString(in.Key)
	if err := p.intercept(ctx, S3OpHead, key); err != nil {
		return nil, fmt.Errorf("pause HeadObject %q: %w", key, err)
	}
	out, err := p.Inner.HeadObject(ctx, in, optFns...)
	if err != nil {
		return nil, fmt.Errorf("s3 HeadObject %q: %w", key, err)
	}
	return out, nil
}

func (p *PausingS3) PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	key := aws.ToString(in.Key)
	if err := p.intercept(ctx, S3OpPut, key); err != nil {
		return nil, fmt.Errorf("pause PutObject %q: %w", key, err)
	}
	out, err := p.Inner.PutObject(ctx, in, optFns...)
	if err != nil {
		return nil, fmt.Errorf("s3 PutObject %q: %w", key, err)
	}
	return out, nil
}

var _ cdc.S3FullClient = (*PausingS3)(nil)
