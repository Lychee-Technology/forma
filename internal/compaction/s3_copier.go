package compaction

import (
	"context"
	"fmt"
)

// KeyedCopier defines minimal copy/delete operations (for S3 or compatible storage).
type KeyedCopier interface {
	Copy(ctx context.Context, srcKey, dstKey string) error
	Delete(ctx context.Context, key string) error
}

// CopierAdapter adapts KeyedCopier to S3Copier.
type CopierAdapter struct {
	Impl KeyedCopier
}

func (c *CopierAdapter) CopyObject(ctx context.Context, srcKey, dstKey string) error {
	if c == nil || c.Impl == nil {
		return fmt.Errorf("copier not configured")
	}
	return c.Impl.Copy(ctx, srcKey, dstKey)
}

func (c *CopierAdapter) DeleteObject(ctx context.Context, key string) error {
	if c == nil || c.Impl == nil {
		return fmt.Errorf("copier not configured")
	}
	return c.Impl.Delete(ctx, key)
}
