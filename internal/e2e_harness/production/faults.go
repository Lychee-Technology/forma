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

// S3Op names one cdc.S3FullClient operation for fault matching.
type S3Op string

const (
	// S3OpDelete matches DeleteObject (manifest-reconcile --gc's collection).
	S3OpDelete S3Op = "DeleteObject"
	// S3OpGet matches GetObject: a writer's read of its parent manifest, and
	// a reader's manifest load. KeyContains separates the manifest from
	// parquet objects.
	S3OpGet S3Op = "GetObject"
	// S3OpPut matches PutObject: the conditional put of each staged parquet
	// object and of the generation's manifest.
	S3OpPut S3Op = "PutObject"
	// S3OpHead matches HeadObject (an existence check of a listed object).
	S3OpHead S3Op = "HeadObject"
)

// S3Fault selects which calls fail. A call matches when its operation equals
// Op and its destination key contains KeyContains (empty matches any key).
// The first SkipMatches matching calls pass through; every later match fails.
type S3Fault struct {
	Op          S3Op
	KeyContains string
	SkipMatches int
}

// FaultInjectingS3 wraps the cluster's real S3 client and fails calls
// matching Fault, breaking exactly one step of the CDC flush pipeline
// (#179). The injected error is a plain error, never manifest.ErrObjectNotFound,
// so manifest.LoadOrCreate surfaces it instead of creating a fresh manifest.
type FaultInjectingS3 struct {
	Inner cdc.S3FullClient
	Fault S3Fault

	mu       sync.Mutex
	matched  int
	injected int
}

// Injected reports how many calls have been failed so far; scenarios assert
// it is non-zero as a positive control that the fault actually fired.
func (f *FaultInjectingS3) Injected() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.injected
}

func (f *FaultInjectingS3) intercept(op S3Op, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if op != f.Fault.Op {
		return nil
	}
	if f.Fault.KeyContains != "" && !strings.Contains(key, f.Fault.KeyContains) {
		return nil
	}
	f.matched++
	if f.matched <= f.Fault.SkipMatches {
		return nil
	}
	f.injected++
	return fmt.Errorf("e2e injected s3 fault: %s %s", op, key)
}

func (f *FaultInjectingS3) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	key := aws.ToString(in.Key)
	if err := f.intercept(S3OpDelete, key); err != nil {
		return nil, err
	}
	out, err := f.Inner.DeleteObject(ctx, in, optFns...)
	if err != nil {
		return nil, fmt.Errorf("s3 DeleteObject %q: %w", key, err)
	}
	return out, nil
}

func (f *FaultInjectingS3) GetObject(ctx context.Context, in *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	key := aws.ToString(in.Key)
	if err := f.intercept(S3OpGet, key); err != nil {
		return nil, err
	}
	out, err := f.Inner.GetObject(ctx, in, optFns...)
	if err != nil {
		return nil, fmt.Errorf("s3 GetObject %q: %w", key, err)
	}
	return out, nil
}

func (f *FaultInjectingS3) HeadObject(ctx context.Context, in *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	key := aws.ToString(in.Key)
	if err := f.intercept(S3OpHead, key); err != nil {
		return nil, err
	}
	out, err := f.Inner.HeadObject(ctx, in, optFns...)
	if err != nil {
		return nil, fmt.Errorf("s3 HeadObject %q: %w", key, err)
	}
	return out, nil
}

func (f *FaultInjectingS3) PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	key := aws.ToString(in.Key)
	if err := f.intercept(S3OpPut, key); err != nil {
		return nil, err
	}
	out, err := f.Inner.PutObject(ctx, in, optFns...)
	if err != nil {
		return nil, fmt.Errorf("s3 PutObject %q: %w", key, err)
	}
	return out, nil
}

var _ cdc.S3FullClient = (*FaultInjectingS3)(nil)
