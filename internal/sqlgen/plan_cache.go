package sqlgen

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
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
// shared — callers must treat them as read-only.
type ProjectionCache struct {
	mu          sync.RWMutex
	projections map[int16]*SchemaProjection
	hits        atomic.Int64
	misses      atomic.Int64
}

// NewProjectionCache creates an empty projection cache.
func NewProjectionCache() *ProjectionCache {
	return &ProjectionCache{projections: make(map[int16]*SchemaProjection)}
}

// GetOrBuild returns the cached projection for schemaID, or runs build and
// caches a non-nil, non-error result. The second return reports a cache hit.
func (c *ProjectionCache) GetOrBuild(schemaID int16, build func() (*SchemaProjection, error)) (*SchemaProjection, bool, error) {
	if c == nil {
		sp, err := build()
		return sp, false, err
	}
	c.mu.RLock()
	sp, ok := c.projections[schemaID]
	c.mu.RUnlock()
	if ok {
		c.hits.Add(1)
		return sp, true, nil
	}

	sp, err := build()
	if err != nil || sp == nil {
		return sp, false, err
	}
	c.misses.Add(1)
	c.mu.Lock()
	c.projections[schemaID] = sp
	c.mu.Unlock()
	return sp, false, nil
}

// Stats returns the cumulative hit and miss counts.
func (c *ProjectionCache) Stats() (hits, misses int64) {
	return c.hits.Load(), c.misses.Load()
}

// Reset drops all cached projections (schema-generation invalidation hook).
func (c *ProjectionCache) Reset() {
	c.mu.Lock()
	c.projections = make(map[int16]*SchemaProjection)
	c.mu.Unlock()
}
