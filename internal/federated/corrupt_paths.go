package federated

import (
	"sync"
	"time"
)

// corruptParquetCache remembers parquet objects that failed per-file
// verification (#251) so path resolution can exclude them without re-failing
// the scan every query. Entries expire after ttl — a terminal verdict must
// never be memoized forever (#326 lesson): a repaired object, a compaction
// that retires the key, or a manifest reconcile self-heals only through
// re-verification after expiry.
type corruptParquetCache struct {
	mu      sync.Mutex
	ttl     time.Duration
	now     func() time.Time
	expires map[string]time.Time
}

func newCorruptParquetCache(ttl time.Duration) *corruptParquetCache {
	return &corruptParquetCache{ttl: ttl, now: time.Now, expires: map[string]time.Time{}}
}

// Add records paths as corrupt until ttl elapses. It also sweeps every
// expired entry from the map — Split evicts only paths it is asked about, so
// an entry whose path is never rescanned (retired by compaction or dropped by
// a manifest reconcile) would otherwise linger until process restart. Add
// runs only on the rare failure path, so the full-map sweep is free in
// practice and keeps the map bounded by live corrupt objects rather than by
// every corrupt object ever seen.
func (c *corruptParquetCache) Add(paths []string) {
	if c == nil || len(paths) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	for p, exp := range c.expires {
		if !now.Before(exp) {
			delete(c.expires, p)
		}
	}
	exp := now.Add(c.ttl)
	for _, p := range paths {
		c.expires[p] = exp
	}
}

// Split partitions paths into kept (not known-corrupt) and excluded
// (known-corrupt, unexpired), evicting expired entries on the way.
func (c *corruptParquetCache) Split(paths []string) (kept, excluded []string) {
	if c == nil {
		return paths, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	for _, p := range paths {
		exp, ok := c.expires[p]
		if ok && now.Before(exp) {
			excluded = append(excluded, p)
			continue
		}
		if ok {
			delete(c.expires, p)
		}
		kept = append(kept, p)
	}
	return kept, excluded
}
