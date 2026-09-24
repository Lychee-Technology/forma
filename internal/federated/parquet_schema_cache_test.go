package federated

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The validator's cache used to be a process-lifetime map: every object ever
// validated kept an entry, retired or not, so heap grew with total objects
// written (#466). These tests pin the two bounds that replaced it.

func newTestValidatedParquetCache(ttl time.Duration, capacity int) (*validatedParquetCache, *fakeClock) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	c := newValidatedParquetCache(ttl, capacity)
	c.now = clock.now
	return c, clock
}

func cachePath(i int) string {
	return fmt.Sprintf("s3://b/7/delta/%d.parquet", i)
}

// TestValidatedParquetCacheStaysBoundedAcrossDistinctPaths is the required
// regression: many distinct validated paths — the caller-chosen-path case,
// all inside one TTL window — never push the cache past its capacity.
func TestValidatedParquetCacheStaysBoundedAcrossDistinctPaths(t *testing.T) {
	c, _ := newTestValidatedParquetCache(time.Hour, 16)
	for i := 0; i < 1000; i++ {
		c.put(cachePath(i), stampWith(), nil)
		require.LessOrEqual(t, c.len(), 16, "the cache grew past its capacity after %d distinct paths", i+1)
	}
	cols, ok := c.get(cachePath(999), nil)
	require.True(t, ok, "the entry just stored must survive its own insertion")
	require.Contains(t, cols, "row_id")
}

// TestValidatedParquetCacheRewritingAPathAtCapacityEvictsNothing guards the
// capacity check: re-validating an existing path is an overwrite, not a new
// entry, and must not evict an unrelated live object.
func TestValidatedParquetCacheRewritingAPathAtCapacityEvictsNothing(t *testing.T) {
	c, _ := newTestValidatedParquetCache(time.Hour, 2)
	c.put(cachePath(0), stampWith(), nil)
	c.put(cachePath(1), stampWith(), nil)
	c.put(cachePath(1), stampWith("city"), stampWith("city"))

	_, ok := c.get(cachePath(0), nil)
	require.True(t, ok, "overwriting an existing path evicted a different entry")
	require.Equal(t, 2, c.len())
}

// TestValidatedParquetCacheSweepsRetiredEntries is the retired-object case: a
// path compaction retired is never looked up again, so only a sweep can
// reclaim it. Once idle past the TTL, the next insert removes it.
func TestValidatedParquetCacheSweepsRetiredEntries(t *testing.T) {
	c, clock := newTestValidatedParquetCache(time.Minute, 100)
	for i := 0; i < 10; i++ {
		c.put(cachePath(i), stampWith(), nil)
	}
	require.Equal(t, 10, c.len())

	clock.advance(time.Minute)
	c.put(cachePath(100), stampWith(), nil)
	require.Equal(t, 1, c.len(), "retired entries idle past the TTL must be swept, not kept forever")
}

// TestValidatedParquetCacheHitOnlyTrafficSweepsRetiredEntries covers steady
// state with no new objects: every query hits a live path and nothing is
// inserted. The retired path is never looked up again, so if only inserts
// swept, its entry would be kept for as long as the hits continue.
func TestValidatedParquetCacheHitOnlyTrafficSweepsRetiredEntries(t *testing.T) {
	c, clock := newTestValidatedParquetCache(time.Minute, 100)
	retired, live := cachePath(0), cachePath(1)
	c.put(retired, stampWith(), nil)
	c.put(live, stampWith(), nil)

	for i := 0; i < 3; i++ {
		clock.advance(40 * time.Second)
		_, ok := c.get(live, nil)
		require.True(t, ok, "the live path must stay warm (lookup %d)", i)
	}
	require.Equal(t, 1, c.len(), "a retired entry idle past the TTL must be swept by hit-only traffic")
}

// TestValidatedParquetCacheHitRefreshesIdleDeadline pins the steady-state
// cost: a live object that queries keep scanning never ages out, so it keeps
// costing zero probes.
func TestValidatedParquetCacheHitRefreshesIdleDeadline(t *testing.T) {
	c, clock := newTestValidatedParquetCache(time.Minute, 100)
	c.put(cachePath(0), stampWith(), nil)
	for i := 0; i < 5; i++ {
		clock.advance(40 * time.Second)
		_, ok := c.get(cachePath(0), nil)
		require.True(t, ok, "a hit must slide the idle deadline (lookup %d)", i)
	}
	clock.advance(time.Minute)
	_, ok := c.get(cachePath(0), nil)
	require.False(t, ok, "an entry idle past the TTL must be a miss")
	require.Equal(t, 0, c.len(), "a lookup that finds an expired entry must delete it")
}

// TestValidatorReProbesAfterIdleExpiry drives expiry through the validator:
// an evicted entry is only a cache miss, so the path is re-validated by a
// fresh footer probe and still contributes its columns.
func TestValidatorReProbesAfterIdleExpiry(t *testing.T) {
	v := newParquetSchemaValidator()
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	v.valid.now = clock.now
	duck := &fakeDuckDBExecutor{describeCols: map[string][][2]string{
		stampCachePath: describeRowsFor(stampWith("city")),
	}}

	validate := func() map[string]string {
		union, complete, err := v.Validate(context.Background(), duck, []string{stampCachePath}, nil)
		require.NoError(t, err)
		require.True(t, complete)
		return union.types
	}
	require.Contains(t, validate(), "city")
	require.Contains(t, validate(), "city")
	require.Equal(t, 1, duck.describeCalls, "a live entry must spare the repeat query its probe")

	clock.advance(defaultValidatedParquetIdleTTL)
	require.Contains(t, validate(), "city", "an expired entry must re-validate, not drop the path")
	require.Equal(t, 2, duck.describeCalls, "an expired entry must be re-probed")
}
