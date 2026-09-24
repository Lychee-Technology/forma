package federated

import (
	"maps"
	"time"
)

// Bounds of the validator's cache (#466). The cache is a pure performance
// memo — every miss falls through to the stamp check or footer probe — so
// these trade probe cost against heap and never correctness.
const (
	// defaultValidatedParquetIdleTTL is how long an entry survives without
	// being looked up. A live object that queries keep scanning stays warm;
	// one that compaction retired is never looked up again and ages out.
	defaultValidatedParquetIdleTTL = 30 * time.Minute
	// defaultValidatedParquetCapacity is the hard entry bound that holds even
	// inside one TTL window, so a caller-chosen path set (an explicit
	// S3ParquetPathTemplate) cannot grow the heap without limit.
	defaultValidatedParquetCapacity = 8192
)

// validatedParquetCache stores the validator's per-path verdicts, bounded by
// an idle TTL and a capacity (#466). The original map had neither and grew
// with every object ever validated, retired or not. The bounds follow the
// sibling caches — corruptParquetCache's TTL sweep and queryplan.Cache's
// capacity — with one difference: at capacity it evicts a single arbitrary
// entry instead of clearing wholesale, because a wholesale clear would send
// every live object back to a footer probe (a network round trip) at once.
//
// It has no lock of its own; parquetSchemaValidator.mu guards every call.
type validatedParquetCache struct {
	ttl       time.Duration
	capacity  int
	now       func() time.Time
	entries   map[string]validatedParquet
	nextSweep time.Time
}

// validatedParquet is one cache entry: the columns the path was validated
// with, the manifest stamp that validation happened under, and when the entry
// stops being served unless a lookup refreshes it. A lookup only hits while
// the current stamp still equals stamp — nil==nil for a path that was
// probe-validated with no stamp in play.
type validatedParquet struct {
	cols    map[string]string
	stamp   map[string]string
	expires time.Time
}

func newValidatedParquetCache(ttl time.Duration, capacity int) *validatedParquetCache {
	return &validatedParquetCache{
		ttl:      ttl,
		capacity: capacity,
		now:      time.Now,
		entries:  map[string]validatedParquet{},
	}
}

// get answers from the cache only while the entry is unexpired and its
// recorded stamp still equals the one currently in play. A changed stamp
// (including one appearing on, or disappearing from, a previously validated
// path) is a miss, so the caller re-validates and overwrites the entry. A hit
// slides the idle deadline; an expired entry is deleted on the way.
func (c *validatedParquetCache) get(path string, stamp map[string]string) (map[string]string, bool) {
	entry, ok := c.entries[path]
	if !ok {
		return nil, false
	}
	now := c.now()
	if !now.Before(entry.expires) {
		delete(c.entries, path)
		return nil, false
	}
	if !maps.Equal(entry.stamp, stamp) {
		return nil, false
	}
	entry.expires = now.Add(c.ttl)
	c.entries[path] = entry
	return entry.cols, true
}

// put records cols for path together with the stamp they were validated
// under; stamp must already be a clone the cache can own. It first sweeps
// expired entries — at most once per TTL, so a burst of cold objects does not
// pay a full scan each — because get evicts only paths it is asked about, and
// a retired object's path is never asked about again. A new path arriving at
// capacity then evicts one arbitrary entry (Go map order).
func (c *validatedParquetCache) put(path string, cols, stamp map[string]string) {
	now := c.now()
	if !now.Before(c.nextSweep) {
		c.sweep(now)
		c.nextSweep = now.Add(c.ttl)
	}
	if _, exists := c.entries[path]; !exists && len(c.entries) >= c.capacity {
		for victim := range c.entries {
			delete(c.entries, victim)
			break
		}
	}
	c.entries[path] = validatedParquet{cols: cols, stamp: stamp, expires: now.Add(c.ttl)}
}

// sweep deletes every entry whose idle deadline has passed.
func (c *validatedParquetCache) sweep(now time.Time) {
	for path, entry := range c.entries {
		if !now.Before(entry.expires) {
			delete(c.entries, path)
		}
	}
}

// len reports the number of stored entries, expired or not.
func (c *validatedParquetCache) len() int {
	return len(c.entries)
}
