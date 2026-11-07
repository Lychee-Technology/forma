package queryplan

import (
	"context"
	"fmt"
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
// Concurrent misses on one key are single-flighted by the underlying Store:
// exactly one goroutine builds, the rest wait and share the result, so
// `misses` equals builds and the benchmark artifact counters stay
// trustworthy.
type Cache struct {
	store *Store[Key, any]
}

// NewCache creates a cache bounded to capacity entries; at the bound the
// cache clears wholesale (shape/scope combinations are bounded in practice).
func NewCache(capacity int) *Cache {
	if capacity <= 0 {
		capacity = 4096
	}
	return &Cache{store: NewStore(StoreConfig[Key, any]{
		Capacity: capacity,
		// An interface comparison against nil never panics, even when the
		// artifact's dynamic type is uncomparable. A typed nil pointer
		// inside the interface is not == nil and is cached, as before #602.
		Cacheable: func(v any) bool { return v != nil },
		Describe: func(key Key) string {
			return fmt.Sprintf("%s plan build for schema %d", key.Kind, key.SchemaID)
		},
	})}
}

// GetOrBuild returns the cached artifact for key or builds, stores, and
// returns it. Build errors and nil artifacts are returned without caching.
// Concurrent callers on the same missing key build exactly once (waiters
// share the result and count as hits). A waiter stops waiting when ctx is
// done and returns the context error; the leader's build is never cancelled
// and its result is still stored. A panic in the leader's build surfaces to
// waiters as an error and is re-raised in the leader. A nil *Cache degrades
// to always building (zero-value construction in tests).
func (c *Cache) GetOrBuild(ctx context.Context, key Key, build func() (any, error)) (any, bool, error) {
	if c == nil {
		v, err := build()
		return v, false, err
	}
	return c.store.GetOrBuild(ctx, key, build)
}

// Stats returns cumulative hit and miss counts (benchmark evidence hook).
func (c *Cache) Stats() (hits, misses int64) {
	if c == nil {
		return 0, 0
	}
	return c.store.Stats()
}

// Reset drops all entries; the schema-generation invalidation hook for
// future metadata reload scenarios.
func (c *Cache) Reset() {
	if c == nil {
		return
	}
	c.store.Reset()
}
