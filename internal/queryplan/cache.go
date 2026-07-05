package queryplan

import (
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

// flightCall is one in-progress build; waiters block on wg then read v/err.
type flightCall struct {
	wg  sync.WaitGroup
	v   any
	err error
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
// count as hits). A nil *Cache degrades to always building (zero-value
// construction in tests).
func (c *Cache) GetOrBuild(key Key, build func() (any, error)) (any, bool, error) {
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
		fc.wg.Wait()
		if fc.err != nil || fc.v == nil {
			return fc.v, false, fc.err
		}
		c.hits.Add(1)
		return fc.v, true, nil
	}
	fc := &flightCall{}
	fc.wg.Add(1)
	c.inflight[key] = fc
	c.flightMu.Unlock()

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

	c.flightMu.Lock()
	delete(c.inflight, key)
	c.flightMu.Unlock()
	fc.wg.Done()
	return v, false, err
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
