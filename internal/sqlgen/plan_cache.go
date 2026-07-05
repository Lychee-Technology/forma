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
// shared — callers must treat them as read-only. Concurrent misses on one
// schema are single-flighted so exactly one build runs and `misses` equals
// builds.
type ProjectionCache struct {
	mu          sync.RWMutex
	projections map[int16]*SchemaProjection
	hits        atomic.Int64
	misses      atomic.Int64

	flightMu sync.Mutex
	inflight map[int16]*projectionFlight
}

type projectionFlight struct {
	wg  sync.WaitGroup
	sp  *SchemaProjection
	err error
}

// NewProjectionCache creates an empty projection cache.
func NewProjectionCache() *ProjectionCache {
	return &ProjectionCache{
		projections: make(map[int16]*SchemaProjection),
		inflight:    make(map[int16]*projectionFlight),
	}
}

// GetOrBuild returns the cached projection for schemaID, or runs build and
// caches a non-nil, non-error result. Concurrent callers on the same missing
// schema build exactly once (waiters share the result and count as hits).
// The second return reports a cache hit.
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

	c.flightMu.Lock()
	// Re-check under the flight lock: the previous flight may have stored
	// the entry between our read miss and here.
	c.mu.RLock()
	sp, ok = c.projections[schemaID]
	c.mu.RUnlock()
	if ok {
		c.flightMu.Unlock()
		c.hits.Add(1)
		return sp, true, nil
	}
	if fl, running := c.inflight[schemaID]; running {
		c.flightMu.Unlock()
		fl.wg.Wait()
		if fl.err != nil || fl.sp == nil {
			return fl.sp, false, fl.err
		}
		c.hits.Add(1)
		return fl.sp, true, nil
	}
	fl := &projectionFlight{}
	fl.wg.Add(1)
	c.inflight[schemaID] = fl
	c.flightMu.Unlock()

	sp, err := build()
	fl.sp, fl.err = sp, err
	if err == nil && sp != nil {
		c.misses.Add(1)
		c.mu.Lock()
		c.projections[schemaID] = sp
		c.mu.Unlock()
	}

	c.flightMu.Lock()
	delete(c.inflight, schemaID)
	c.flightMu.Unlock()
	fl.wg.Done()
	return sp, false, err
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
