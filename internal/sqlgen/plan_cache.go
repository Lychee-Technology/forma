package sqlgen

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/lychee-technology/forma/internal/queryplan"
)

// HashShapeParts fingerprints an ordered list of shape components with
// FNV-64a, inserting a separator so part boundaries cannot collide
// ("ab","c" vs "a","bc").
func HashShapeParts(parts ...string) uint64 {
	h := fnv.New64a()
	for _, p := range parts {
		_, _ = h.Write([]byte(p))
		_, _ = h.Write([]byte{0})
	}
	return h.Sum64()
}

// ProjectionCache caches BuildSchemaProjection results per schema (#142):
// the projection depends only on the schema's attribute metadata, which is
// immutable after registry construction. Cached *SchemaProjection values are
// shared — callers must treat them as read-only. Concurrent misses on one
// schema are single-flighted by the underlying queryplan.Store so exactly one
// build runs and `misses` equals builds. The cache is unbounded.
type ProjectionCache struct {
	store *queryplan.Store[int16, *SchemaProjection]
}

// NewProjectionCache creates an empty projection cache.
func NewProjectionCache() *ProjectionCache {
	return &ProjectionCache{store: queryplan.NewStore(queryplan.StoreConfig[int16, *SchemaProjection]{
		Cacheable: func(sp *SchemaProjection) bool { return sp != nil },
		Describe: func(schemaID int16) string {
			return fmt.Sprintf("schema projection build for schema %d", schemaID)
		},
	})}
}

// GetOrBuild returns the cached projection for schemaID, or runs build and
// caches a non-nil, non-error result. Concurrent callers on the same missing
// schema build exactly once (waiters share the result and count as hits).
// A waiter stops waiting when ctx is done and returns the context error; the
// leader's build is never cancelled and its result is still stored. A panic
// in the leader's build surfaces to waiters as an error and is re-raised in
// the leader. The second return reports a cache hit.
func (c *ProjectionCache) GetOrBuild(ctx context.Context, schemaID int16, build func() (*SchemaProjection, error)) (*SchemaProjection, bool, error) {
	if c == nil {
		sp, err := build()
		return sp, false, err
	}
	return c.store.GetOrBuild(ctx, schemaID, build)
}

// Stats returns the cumulative hit and miss counts.
func (c *ProjectionCache) Stats() (hits, misses int64) {
	return c.store.Stats()
}

// Reset drops all cached projections (schema-generation invalidation hook).
func (c *ProjectionCache) Reset() {
	c.store.Reset()
}
