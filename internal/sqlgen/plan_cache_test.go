package sqlgen

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHashShapePartsBoundaries(t *testing.T) {
	require.NotEqual(t, HashShapeParts("ab", "c"), HashShapeParts("a", "bc"))
	require.Equal(t, HashShapeParts("a", "b"), HashShapeParts("a", "b"))
	require.NotEqual(t, HashShapeParts("a"), HashShapeParts("a", ""))
}

// TestProjectionCacheSingleFlight mirrors the queryplan.Cache contract from
// the PR #148 review: one build per schema under concurrent misses.
func TestProjectionCacheSingleFlight(t *testing.T) {
	c := NewProjectionCache()
	var builds atomic.Int64
	var wg sync.WaitGroup
	const goroutines = 32
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sp, _, err := c.GetOrBuild(7, func() (*SchemaProjection, error) {
				builds.Add(1)
				time.Sleep(10 * time.Millisecond)
				return &SchemaProjection{}, nil
			})
			require.NoError(t, err)
			require.NotNil(t, sp)
		}()
	}
	wg.Wait()
	require.Equal(t, int64(1), builds.Load())
	hits, misses := c.Stats()
	require.Equal(t, int64(1), misses)
	require.Equal(t, int64(goroutines-1), hits)
}
