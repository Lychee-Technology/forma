package sqlgen

import (
	"context"
	"fmt"
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

// projectionFlight is one in-progress build; waiters block on done then read
// sp/err. done is closed by the leader's deferred release, so a panicking
// build still frees its waiters (#467).
type projectionFlight struct {
	done chan struct{}
	sp   *SchemaProjection
	err  error
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
// A waiter stops waiting when ctx is done and returns the context error; the
// leader's build is never cancelled and its result is still stored. A panic
// in the leader's build surfaces to waiters as an error and is re-raised in
// the leader. The second return reports a cache hit.
func (c *ProjectionCache) GetOrBuild(ctx context.Context, schemaID int16, build func() (*SchemaProjection, error)) (*SchemaProjection, bool, error) {
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
		return c.awaitFlight(ctx, schemaID, fl)
	}
	fl := &projectionFlight{done: make(chan struct{})}
	c.inflight[schemaID] = fl
	c.flightMu.Unlock()
	defer c.releaseFlight(schemaID, fl)

	sp, err := build()
	fl.sp, fl.err = sp, err
	if err == nil && sp != nil {
		c.misses.Add(1)
		c.mu.Lock()
		c.projections[schemaID] = sp
		c.mu.Unlock()
	}
	return sp, false, err
}

// awaitFlight waits for another caller's build of schemaID, or for ctx. A
// shared success counts as a hit; a cancelled wait touches no counter.
func (c *ProjectionCache) awaitFlight(ctx context.Context, schemaID int16, fl *projectionFlight) (*SchemaProjection, bool, error) {
	select {
	case <-fl.done:
	case <-ctx.Done():
		return nil, false, fmt.Errorf("wait for in-flight schema projection build for schema %d: %w", schemaID, ctx.Err())
	}
	if fl.err != nil || fl.sp == nil {
		return fl.sp, false, fl.err
	}
	c.hits.Add(1)
	return fl.sp, true, nil
}

// releaseFlight is deferred by the leader: it drops the in-flight slot and
// wakes waiters whether build returned or panicked. A panic is recorded as
// the flight's error so waiters fail instead of hanging, then re-raised so
// the leader's own failure mode is unchanged (#467).
func (c *ProjectionCache) releaseFlight(schemaID int16, fl *projectionFlight) {
	if r := recover(); r != nil {
		fl.sp, fl.err = nil, fmt.Errorf("schema projection build for schema %d panicked: %v", schemaID, r)
		defer panic(r)
	}
	c.flightMu.Lock()
	delete(c.inflight, schemaID)
	c.flightMu.Unlock()
	close(fl.done)
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
