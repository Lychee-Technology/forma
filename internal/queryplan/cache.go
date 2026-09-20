package queryplan

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// Key isolates cached planning artifacts. Every field participates in
// equality: a plan is only reused when kind, schema content (fingerprint),
// schema identity, query shape, and rendering scope all match.
type Key struct {
	// Kind names the artifact family, e.g. "duckdb_federated",
	// "postgres_optimized_template", "dual_clause_plan".
	Kind string
	// SchemaVersion is the deterministic schema-metadata fingerprint (or
	// load-generation token) — the invalidation lever: content changes
	// produce a different fingerprint, orphaning old entries.
	SchemaVersion string
	SchemaID      int16
	ShapeHash     string
	ScopeHash     string
}

// Cache is a concurrency-safe store for compiled planning artifacts, shared
// across engines/repositories so its lifetime spans repeated requests (the
// benchmark and production reuse lifecycle — #142 design review finding 1).
// Concurrent misses on one key are single-flighted: exactly one goroutine
// builds, the rest wait and share the result, so `misses` equals builds and
// the benchmark artifact counters stay trustworthy.
type Cache struct {
	mu      sync.RWMutex
	entries map[Key]any
	cap     int
	hits    atomic.Int64
	misses  atomic.Int64

	flightMu sync.Mutex
	inflight map[Key]*flightCall
}

// flightCall is one in-progress build; waiters block on done then read v/err.
// done is closed by the leader's deferred release, so a panicking build still
// frees its waiters (#467).
type flightCall struct {
	done chan struct{}
	v    any
	err  error
}

// NewCache creates a cache bounded to capacity entries; at the bound the
// cache clears wholesale (shape/scope combinations are bounded in practice).
func NewCache(capacity int) *Cache {
	if capacity <= 0 {
		capacity = 4096
	}
	return &Cache{entries: make(map[Key]any), cap: capacity, inflight: make(map[Key]*flightCall)}
}

// GetOrBuild returns the cached artifact for key or builds, stores, and
// returns it. Build errors are returned without caching. Concurrent callers
// on the same missing key build exactly once (waiters share the result and
// count as hits). A waiter stops waiting when ctx is done and returns the
// context error; the leader's build is never cancelled and its result is
// still stored. A panic in the leader's build surfaces to waiters as an error
// and is re-raised in the leader. A nil *Cache degrades to always building
// (zero-value construction in tests).
func (c *Cache) GetOrBuild(ctx context.Context, key Key, build func() (any, error)) (any, bool, error) {
	if c == nil {
		v, err := build()
		return v, false, err
	}
	c.mu.RLock()
	v, ok := c.entries[key]
	c.mu.RUnlock()
	if ok {
		c.hits.Add(1)
		return v, true, nil
	}

	c.flightMu.Lock()
	// Re-check under the flight lock: the previous flight may have stored
	// the entry between our read miss and here.
	c.mu.RLock()
	v, ok = c.entries[key]
	c.mu.RUnlock()
	if ok {
		c.flightMu.Unlock()
		c.hits.Add(1)
		return v, true, nil
	}
	if fc, running := c.inflight[key]; running {
		c.flightMu.Unlock()
		return c.awaitFlight(ctx, key, fc)
	}
	fc := &flightCall{done: make(chan struct{})}
	c.inflight[key] = fc
	c.flightMu.Unlock()
	defer c.releaseFlight(key, fc)

	v, err := build()
	fc.v, fc.err = v, err
	if err == nil && v != nil {
		c.misses.Add(1)
		c.mu.Lock()
		if len(c.entries) >= c.cap {
			c.entries = make(map[Key]any)
		}
		c.entries[key] = v
		c.mu.Unlock()
	}
	return v, false, err
}

// awaitFlight waits for another caller's build of key, or for ctx. A shared
// success counts as a hit; a cancelled wait touches no counter.
func (c *Cache) awaitFlight(ctx context.Context, key Key, fc *flightCall) (any, bool, error) {
	select {
	case <-fc.done:
	case <-ctx.Done():
		return nil, false, fmt.Errorf("wait for in-flight %s plan build for schema %d: %w", key.Kind, key.SchemaID, ctx.Err())
	}
	if fc.err != nil || fc.v == nil {
		return fc.v, false, fc.err
	}
	c.hits.Add(1)
	return fc.v, true, nil
}

// releaseFlight is deferred by the leader: it drops the in-flight slot and
// wakes waiters whether build returned or panicked. A panic is recorded as
// the flight's error so waiters fail instead of hanging, then re-raised so
// the leader's own failure mode is unchanged (#467).
func (c *Cache) releaseFlight(key Key, fc *flightCall) {
	if r := recover(); r != nil {
		fc.v, fc.err = nil, fmt.Errorf("%s plan build for schema %d panicked: %v", key.Kind, key.SchemaID, r)
		defer panic(r)
	}
	c.flightMu.Lock()
	delete(c.inflight, key)
	c.flightMu.Unlock()
	close(fc.done)
}

// Stats returns cumulative hit and miss counts (benchmark evidence hook).
func (c *Cache) Stats() (hits, misses int64) {
	if c == nil {
		return 0, 0
	}
	return c.hits.Load(), c.misses.Load()
}

// Reset drops all entries; the schema-generation invalidation hook for
// future metadata reload scenarios.
func (c *Cache) Reset() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.entries = make(map[Key]any)
	c.mu.Unlock()
}
