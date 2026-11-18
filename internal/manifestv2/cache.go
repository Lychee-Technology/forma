package manifestv2

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
)

// Cache is a process-wide cache of verified manifests keyed by manifest key.
// A manifest object never changes once written, so an entry is never stale
// and nothing invalidates it. The cache holds at most a fixed number of
// entries and evicts the least recently used one, which bounds memory and
// costs only a refetch.
//
// Only a verified manifest enters it. A fetch that fails, for a missing
// object, a manifest that fails verification or a failed store call, leaves
// nothing behind, so the next Get fetches again. Every hit is checked
// against the caller's Ref too: a reference that names a cached key with
// another digest or identity is refused, just as a fetch would be.
type Cache struct {
	store Store
	max   int

	mu       sync.Mutex
	entries  map[string]*list.Element
	order    *list.List // of *entry, most recently used first
	inflight map[string]*fetch
}

type entry struct {
	key      string
	manifest *Manifest
	digest   string
}

// fetch is a fetch in flight. Concurrent Gets of one key wait for it rather
// than fetching the object again.
type fetch struct {
	done  chan struct{}
	entry *entry
}

// NewCache answers a cache over st holding at most maxEntries manifests.
func NewCache(st Store, maxEntries int) (*Cache, error) {
	if st == nil || maxEntries <= 0 {
		return nil, fmt.Errorf("a manifest cache needs a store and a positive capacity, got %d", maxEntries)
	}
	return &Cache{
		store: st, max: maxEntries,
		entries: make(map[string]*list.Element), order: list.New(), inflight: make(map[string]*fetch),
	}, nil
}

// Get answers r's manifest, verified (Verify). A missing object or one that
// fails verification is a *ConsistencyError. Any other error is the store's,
// and a later Get may succeed.
func (c *Cache) Get(ctx context.Context, r Ref) (*Manifest, error) {
	for {
		c.mu.Lock()
		if el, ok := c.entries[r.ManifestKey]; ok {
			c.order.MoveToFront(el)
			e := el.Value.(*entry)
			c.mu.Unlock()
			return answer(e, r)
		}
		if f, ok := c.inflight[r.ManifestKey]; ok {
			c.mu.Unlock()
			select {
			case <-f.done:
			case <-ctx.Done():
				return nil, fmt.Errorf("wait for manifest %s: %w", r.ManifestKey, ctx.Err())
			}
			if f.entry != nil {
				return answer(f.entry, r)
			}
			// The fetch in flight failed, maybe for its caller's Ref
			// alone, or its caller's context: fetch again.
			continue
		}
		f := &fetch{done: make(chan struct{})}
		c.inflight[r.ManifestKey] = f
		c.mu.Unlock()
		return c.fetch(ctx, r, f)
	}
}

func answer(e *entry, r Ref) (*Manifest, error) {
	if err := r.check(e.manifest, e.digest); err != nil {
		return nil, err
	}
	return e.manifest, nil
}

// fetch gets and verifies r's manifest, and caches it once verified.
func (c *Cache) fetch(ctx context.Context, r Ref, f *fetch) (*Manifest, error) {
	m, digest, err := fetchVerified(ctx, c.store, r)
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.inflight, r.ManifestKey)
	defer close(f.done)
	if err != nil {
		return nil, err
	}
	f.entry = &entry{key: r.ManifestKey, manifest: m, digest: digest}
	c.entries[r.ManifestKey] = c.order.PushFront(f.entry)
	for c.order.Len() > c.max {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.entries, oldest.Value.(*entry).key)
	}
	return m, nil
}

// Fetch gets r's manifest from st and verifies it (Verify), with no cache. A
// missing object or one that fails verification is a *ConsistencyError; any
// other error is the store's.
func Fetch(ctx context.Context, st Store, r Ref) (*Manifest, error) {
	m, _, err := fetchVerified(ctx, st, r)
	return m, err
}

func fetchVerified(ctx context.Context, st Store, r Ref) (*Manifest, string, error) {
	raw, err := st.Get(ctx, r.ManifestKey)
	if errors.Is(err, ErrObjectNotFound) {
		return nil, "", r.fail(fmt.Errorf("%w: %w", ErrManifestMissing, err))
	}
	if err != nil {
		return nil, "", fmt.Errorf("fetch manifest %s of schema %d generation %d: %w", r.ManifestKey, r.SchemaID, r.Generation, err)
	}
	m, err := Verify(raw, r)
	if err != nil {
		return nil, "", err
	}
	return m, Digest(raw), nil
}

// Len answers how many manifests the cache holds.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}
