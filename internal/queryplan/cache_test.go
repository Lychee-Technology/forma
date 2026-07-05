package queryplan

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func k(shape string) Key {
	return Key{Kind: "test", SchemaVersion: "fp-1", SchemaID: 7, ShapeHash: shape, ScopeHash: "scope"}
}

func TestCacheHitMissAndStats(t *testing.T) {
	c := NewCache(8)
	builds := 0
	build := func() (any, error) { builds++; return "artifact", nil }

	v, hit, err := c.GetOrBuild(k("s1"), build)
	require.NoError(t, err)
	require.False(t, hit)
	require.Equal(t, "artifact", v)

	_, hit, err = c.GetOrBuild(k("s1"), build)
	require.NoError(t, err)
	require.True(t, hit)
	require.Equal(t, 1, builds)

	hits, misses := c.Stats()
	require.Equal(t, int64(1), hits)
	require.Equal(t, int64(1), misses)
}

func TestCacheKeyIsolation(t *testing.T) {
	c := NewCache(8)
	build := func() (any, error) { return "x", nil }
	_, _, _ = c.GetOrBuild(k("s1"), build)

	variants := []Key{
		{Kind: "other", SchemaVersion: "fp-1", SchemaID: 7, ShapeHash: "s1", ScopeHash: "scope"},
		{Kind: "test", SchemaVersion: "fp-2", SchemaID: 7, ShapeHash: "s1", ScopeHash: "scope"},
		{Kind: "test", SchemaVersion: "fp-1", SchemaID: 8, ShapeHash: "s1", ScopeHash: "scope"},
		{Kind: "test", SchemaVersion: "fp-1", SchemaID: 7, ShapeHash: "s2", ScopeHash: "scope"},
		{Kind: "test", SchemaVersion: "fp-1", SchemaID: 7, ShapeHash: "s1", ScopeHash: "other"},
	}
	for _, key := range variants {
		_, hit, err := c.GetOrBuild(key, build)
		require.NoError(t, err)
		require.False(t, hit, "key variant %+v must not hit", key)
	}
}

func TestCacheSchemaVersionInvalidation(t *testing.T) {
	c := NewCache(8)
	build := func() (any, error) { return "plan", nil }
	_, _, _ = c.GetOrBuild(k("s1"), build)

	// A schema-content change produces a new fingerprint: same shape misses.
	newGen := k("s1")
	newGen.SchemaVersion = "fp-2"
	_, hit, err := c.GetOrBuild(newGen, build)
	require.NoError(t, err)
	require.False(t, hit)
}

func TestCacheErrorAndNilNotCached(t *testing.T) {
	c := NewCache(8)
	calls := 0
	_, _, err := c.GetOrBuild(k("e"), func() (any, error) { calls++; return nil, fmt.Errorf("boom") })
	require.Error(t, err)
	_, _, err = c.GetOrBuild(k("e"), func() (any, error) { calls++; return nil, nil })
	require.NoError(t, err)
	_, hit, _ := c.GetOrBuild(k("e"), func() (any, error) { calls++; return "v", nil })
	require.False(t, hit)
	require.Equal(t, 3, calls)
}

func TestCacheNilReceiverDegrades(t *testing.T) {
	var c *Cache
	v, hit, err := c.GetOrBuild(k("s"), func() (any, error) { return "v", nil })
	require.NoError(t, err)
	require.False(t, hit)
	require.Equal(t, "v", v)
	c.Reset()
}

func TestCacheConcurrent(t *testing.T) {
	c := NewCache(64)
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				key := k(fmt.Sprintf("s%d", i%8))
				v, _, err := c.GetOrBuild(key, func() (any, error) {
					return "v-" + key.ShapeHash, nil
				})
				if err != nil || v != "v-"+key.ShapeHash {
					t.Errorf("got %v err %v", v, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}
