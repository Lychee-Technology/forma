package sqlgen

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
)

// RenderCache caches rendered SQL text keyed by a query-shape fingerprint.
// It exists to eliminate the per-request text/template execution cost on hot
// query paths (#142): the key must cover every input that influences the
// rendered output, so a hit is correct by construction. Values never enter
// the key — value binding stays per-request.
//
// Entries are valid for the lifetime of the owning component because the
// schema metadata they derive from is immutable after construction; Reset
// exists for future reload scenarios and for tests.
type RenderCache struct {
	mu       sync.RWMutex
	entries  map[uint64]string
	capacity int
	hits     atomic.Int64
	misses   atomic.Int64
}

// NewRenderCache creates a cache holding at most capacity rendered texts.
// When the cap is reached the cache is cleared wholesale: query shapes are
// bounded in practice, so eviction sophistication is not worth the locking.
func NewRenderCache(capacity int) *RenderCache {
	if capacity <= 0 {
		capacity = 1024
	}
	return &RenderCache{entries: make(map[uint64]string), capacity: capacity}
}

// GetOrRender returns the cached text for key, or runs render, stores the
// result, and returns it. The second return reports whether it was a hit.
// Render errors are returned without caching.
func (c *RenderCache) GetOrRender(key uint64, render func() (string, error)) (string, bool, error) {
	if c == nil {
		// Zero-value construction (tests building repositories as struct
		// literals) degrades to uncached rendering.
		text, err := render()
		return text, false, err
	}
	c.mu.RLock()
	text, ok := c.entries[key]
	c.mu.RUnlock()
	if ok {
		c.hits.Add(1)
		return text, true, nil
	}

	text, err := render()
	if err != nil {
		return "", false, err
	}
	c.misses.Add(1)

	c.mu.Lock()
	if len(c.entries) >= c.capacity {
		c.entries = make(map[uint64]string)
	}
	c.entries[key] = text
	c.mu.Unlock()
	return text, false, nil
}

// Stats returns the cumulative hit and miss counts.
func (c *RenderCache) Stats() (hits, misses int64) {
	return c.hits.Load(), c.misses.Load()
}

// Reset drops all cached entries (schema-generation invalidation hook).
func (c *RenderCache) Reset() {
	c.mu.Lock()
	c.entries = make(map[uint64]string)
	c.mu.Unlock()
}

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
