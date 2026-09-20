package queryplan

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func k(shape string) Key {
	return Key{Kind: "test", SchemaVersion: "fp-1", SchemaID: 7, ShapeHash: shape, ScopeHash: "scope"}
}

func TestCacheHitMissAndStats(t *testing.T) {
	c := NewCache(8)
	builds := 0
	build := func() (any, error) { builds++; return "artifact", nil }

	v, hit, err := c.GetOrBuild(context.Background(), k("s1"), build)
	require.NoError(t, err)
	require.False(t, hit)
	require.Equal(t, "artifact", v)

	_, hit, err = c.GetOrBuild(context.Background(), k("s1"), build)
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
	_, _, _ = c.GetOrBuild(context.Background(), k("s1"), build)

	variants := []Key{
		{Kind: "other", SchemaVersion: "fp-1", SchemaID: 7, ShapeHash: "s1", ScopeHash: "scope"},
		{Kind: "test", SchemaVersion: "fp-2", SchemaID: 7, ShapeHash: "s1", ScopeHash: "scope"},
		{Kind: "test", SchemaVersion: "fp-1", SchemaID: 8, ShapeHash: "s1", ScopeHash: "scope"},
		{Kind: "test", SchemaVersion: "fp-1", SchemaID: 7, ShapeHash: "s2", ScopeHash: "scope"},
		{Kind: "test", SchemaVersion: "fp-1", SchemaID: 7, ShapeHash: "s1", ScopeHash: "other"},
	}
	for _, key := range variants {
		_, hit, err := c.GetOrBuild(context.Background(), key, build)
		require.NoError(t, err)
		require.False(t, hit, "key variant %+v must not hit", key)
	}
}

func TestCacheSchemaVersionInvalidation(t *testing.T) {
	c := NewCache(8)
	build := func() (any, error) { return "plan", nil }
	_, _, _ = c.GetOrBuild(context.Background(), k("s1"), build)

	// A schema-content change produces a new fingerprint: same shape misses.
	newGen := k("s1")
	newGen.SchemaVersion = "fp-2"
	_, hit, err := c.GetOrBuild(context.Background(), newGen, build)
	require.NoError(t, err)
	require.False(t, hit)
}

func TestCacheErrorAndNilNotCached(t *testing.T) {
	c := NewCache(8)
	calls := 0
	_, _, err := c.GetOrBuild(context.Background(), k("e"), func() (any, error) { calls++; return nil, fmt.Errorf("boom") })
	require.Error(t, err)
	_, _, err = c.GetOrBuild(context.Background(), k("e"), func() (any, error) { calls++; return nil, nil })
	require.NoError(t, err)
	_, hit, _ := c.GetOrBuild(context.Background(), k("e"), func() (any, error) { calls++; return "v", nil })
	require.False(t, hit)
	require.Equal(t, 3, calls)
}

func TestCacheNilReceiverDegrades(t *testing.T) {
	var c *Cache
	v, hit, err := c.GetOrBuild(context.Background(), k("s"), func() (any, error) { return "v", nil })
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
				v, _, err := c.GetOrBuild(context.Background(), key, func() (any, error) {
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

// TestCacheSingleFlight pins the review fix on PR #148: concurrent misses on
// one key run build exactly once; waiters share the result. misses therefore
// equals builds, keeping benchmark artifact counters trustworthy.
func TestCacheSingleFlight(t *testing.T) {
	c := NewCache(8)
	var builds atomic.Int64
	var wg sync.WaitGroup
	const goroutines = 32
	results := make([]any, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, _, err := c.GetOrBuild(context.Background(), k("sf"), func() (any, error) {
				builds.Add(1)
				time.Sleep(10 * time.Millisecond) // widen the race window
				return "artifact", nil
			})
			require.NoError(t, err)
			results[i] = v
		}(g)
	}
	wg.Wait()

	require.Equal(t, int64(1), builds.Load(), "concurrent misses must build exactly once")
	for _, v := range results {
		require.Equal(t, "artifact", v)
	}
	hits, misses := c.Stats()
	require.Equal(t, int64(1), misses, "misses must equal builds")
	require.Equal(t, int64(goroutines-1), hits, "waiters and re-checks count as hits")
}

// TestCacheSingleFlightErrorNotShareableForever pins that a failed flight
// does not poison the key: the next caller rebuilds.
func TestCacheSingleFlightErrorRetries(t *testing.T) {
	c := NewCache(8)
	calls := 0
	_, _, err := c.GetOrBuild(context.Background(), k("err"), func() (any, error) { calls++; return nil, fmt.Errorf("boom") })
	require.Error(t, err)
	v, hit, err := c.GetOrBuild(context.Background(), k("err"), func() (any, error) { calls++; return "ok", nil })
	require.NoError(t, err)
	require.False(t, hit)
	require.Equal(t, "ok", v)
	require.Equal(t, 2, calls)
}

// TestCacheLeaderPanicFreesFollowers pins #467: a panic inside the leader's
// build must release the single-flight slot. A follower already waiting on
// the key gets an error (not a hang), the leader still observes its own
// panic, and the next caller for the key rebuilds normally.
func TestCacheLeaderPanicFreesFollowers(t *testing.T) {
	c := NewCache(8)
	key := k("panic")
	leaderStarted := make(chan struct{})
	release := make(chan struct{})

	leaderPanic := make(chan any, 1)
	go func() {
		defer func() { leaderPanic <- recover() }()
		_, _, _ = c.GetOrBuild(context.Background(), key, func() (any, error) {
			close(leaderStarted)
			<-release
			panic("boom")
		})
	}()
	<-leaderStarted

	followerErr := make(chan error, 1)
	go func() {
		_, _, err := c.GetOrBuild(context.Background(), key, func() (any, error) {
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

	v, hit, err := c.GetOrBuild(context.Background(), key, func() (any, error) { return "ok", nil })
	require.NoError(t, err)
	require.False(t, hit)
	require.Equal(t, "ok", v)
}

// TestCacheFollowerWaitHonoursContext pins #467: a follower waiting on a slow
// leader returns when its context is cancelled, and the leader's result is
// still stored for later callers.
func TestCacheFollowerWaitHonoursContext(t *testing.T) {
	c := NewCache(8)
	key := k("slow")
	leaderStarted := make(chan struct{})
	release := make(chan struct{})
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		_, _, _ = c.GetOrBuild(context.Background(), key, func() (any, error) {
			close(leaderStarted)
			<-release
			return "built", nil
		})
	}()
	<-leaderStarted

	ctx, cancel := context.WithCancel(context.Background())
	followerErr := make(chan error, 1)
	go func() {
		_, _, err := c.GetOrBuild(ctx, key, func() (any, error) { return "never", nil })
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
	v, hit, err := c.GetOrBuild(context.Background(), key, func() (any, error) { return "never", nil })
	require.NoError(t, err)
	require.True(t, hit, "leader result must be cached despite the follower's cancellation")
	require.Equal(t, "built", v)
	hits, misses := c.Stats()
	require.Equal(t, int64(1), hits)
	require.Equal(t, int64(1), misses, "a cancelled follower must not count as a miss")
}
