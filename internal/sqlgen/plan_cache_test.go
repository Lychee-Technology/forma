package sqlgen

import (
	"context"
	"fmt"
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
			sp, _, err := c.GetOrBuild(context.Background(), 7, func() (*SchemaProjection, error) {
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

// TestProjectionCacheLeaderPanicFreesFollowers pins #467 for the projection
// cache: a panic inside the leader's build releases the slot, so a waiting
// follower gets an error instead of hanging, the leader still observes its
// own panic, and the next caller rebuilds normally.
func TestProjectionCacheLeaderPanicFreesFollowers(t *testing.T) {
	c := NewProjectionCache()
	leaderStarted := make(chan struct{})
	release := make(chan struct{})

	leaderPanic := make(chan any, 1)
	go func() {
		defer func() { leaderPanic <- recover() }()
		_, _, _ = c.GetOrBuild(context.Background(), 7, func() (*SchemaProjection, error) {
			close(leaderStarted)
			<-release
			panic("boom")
		})
	}()
	<-leaderStarted

	followerErr := make(chan error, 1)
	go func() {
		_, _, err := c.GetOrBuild(context.Background(), 7, func() (*SchemaProjection, error) {
			return nil, fmt.Errorf("follower built itself")
		})
		followerErr <- err
	}()
	// Give the follower time to reach the wait before the leader panics; a
	// follower that arrives late would build itself and fail the assertion
	// below with a distinct error.
	time.Sleep(50 * time.Millisecond)
	close(release)

	select {
	case err := <-followerErr:
		require.ErrorContains(t, err, "panicked")
	case <-time.After(2 * time.Second):
		t.Fatal("follower deadlocked after leader panic")
	}
	require.Equal(t, "boom", <-leaderPanic, "leader must still observe its own panic")

	c.flightMu.Lock()
	require.Empty(t, c.inflight, "panicking build must release its slot")
	c.flightMu.Unlock()

	sp, hit, err := c.GetOrBuild(context.Background(), 7, func() (*SchemaProjection, error) { return &SchemaProjection{}, nil })
	require.NoError(t, err)
	require.False(t, hit)
	require.NotNil(t, sp)
}

// TestProjectionCacheFollowerWaitHonoursContext pins #467: a follower waiting
// on a slow leader returns when its context is cancelled, and the leader's
// result is still stored for later callers.
func TestProjectionCacheFollowerWaitHonoursContext(t *testing.T) {
	c := NewProjectionCache()
	leaderStarted := make(chan struct{})
	release := make(chan struct{})
	leaderDone := make(chan struct{})
	built := &SchemaProjection{}
	go func() {
		defer close(leaderDone)
		_, _, _ = c.GetOrBuild(context.Background(), 7, func() (*SchemaProjection, error) {
			close(leaderStarted)
			<-release
			return built, nil
		})
	}()
	<-leaderStarted

	ctx, cancel := context.WithCancel(context.Background())
	followerErr := make(chan error, 1)
	go func() {
		_, _, err := c.GetOrBuild(ctx, 7, func() (*SchemaProjection, error) {
			return nil, fmt.Errorf("follower built itself")
		})
		followerErr <- err
	}()
	cancel()
	select {
	case err := <-followerErr:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("follower ignored context cancellation")
	}

	close(release)
	<-leaderDone
	sp, hit, err := c.GetOrBuild(context.Background(), 7, func() (*SchemaProjection, error) {
		return nil, fmt.Errorf("must not rebuild")
	})
	require.NoError(t, err)
	require.True(t, hit, "leader result must be cached despite the follower's cancellation")
	require.Same(t, built, sp)
	hits, misses := c.Stats()
	require.Equal(t, int64(1), hits)
	require.Equal(t, int64(1), misses, "a cancelled follower must not count as a miss")
}
