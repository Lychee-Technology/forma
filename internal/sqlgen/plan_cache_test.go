package sqlgen

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRenderCacheHitAndMiss(t *testing.T) {
	c := NewRenderCache(8)
	renders := 0
	render := func() (string, error) { renders++; return "SQL", nil }

	text, hit, err := c.GetOrRender(1, render)
	require.NoError(t, err)
	require.False(t, hit)
	require.Equal(t, "SQL", text)

	text, hit, err = c.GetOrRender(1, render)
	require.NoError(t, err)
	require.True(t, hit)
	require.Equal(t, "SQL", text)
	require.Equal(t, 1, renders)

	hits, misses := c.Stats()
	require.Equal(t, int64(1), hits)
	require.Equal(t, int64(1), misses)
}

func TestRenderCacheErrorNotCached(t *testing.T) {
	c := NewRenderCache(8)
	calls := 0
	failing := func() (string, error) { calls++; return "", fmt.Errorf("boom") }

	_, _, err := c.GetOrRender(7, failing)
	require.Error(t, err)
	_, _, err = c.GetOrRender(7, failing)
	require.Error(t, err)
	require.Equal(t, 2, calls)
}

func TestRenderCacheCapacityClearsWholesale(t *testing.T) {
	c := NewRenderCache(2)
	for key := uint64(0); key < 3; key++ {
		k := key
		_, _, err := c.GetOrRender(k, func() (string, error) { return fmt.Sprintf("sql-%d", k), nil })
		require.NoError(t, err)
	}
	// Keys 0 and 1 were dropped when key 2 hit the cap; key 2 survives.
	_, hit, err := c.GetOrRender(2, func() (string, error) { return "sql-2", nil })
	require.NoError(t, err)
	require.True(t, hit)
	_, hit, err = c.GetOrRender(0, func() (string, error) { return "sql-0", nil })
	require.NoError(t, err)
	require.False(t, hit)
}

func TestRenderCacheReset(t *testing.T) {
	c := NewRenderCache(8)
	_, _, err := c.GetOrRender(1, func() (string, error) { return "SQL", nil })
	require.NoError(t, err)
	c.Reset()
	_, hit, err := c.GetOrRender(1, func() (string, error) { return "SQL", nil })
	require.NoError(t, err)
	require.False(t, hit, "Reset must invalidate cached entries")
}

func TestRenderCacheConcurrentAccess(t *testing.T) {
	c := NewRenderCache(64)
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				key := uint64(i % 8)
				text, _, err := c.GetOrRender(key, func() (string, error) {
					return fmt.Sprintf("sql-%d", key), nil
				})
				if err != nil || text != fmt.Sprintf("sql-%d", key) {
					t.Errorf("goroutine %d: got %q err %v", g, text, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestHashShapePartsBoundaries(t *testing.T) {
	require.NotEqual(t, HashShapeParts("ab", "c"), HashShapeParts("a", "bc"))
	require.Equal(t, HashShapeParts("a", "b"), HashShapeParts("a", "b"))
	require.NotEqual(t, HashShapeParts("a"), HashShapeParts("a", ""))
}
