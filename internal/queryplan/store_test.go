package queryplan

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func intStore(capacity int) *Store[int, *int] {
	return NewStore(StoreConfig[int, *int]{
		Capacity:  capacity,
		Cacheable: func(v *int) bool { return v != nil },
		Describe:  func(key int) string { return fmt.Sprintf("int build %d", key) },
	})
}

func intPtr(v int) *int { return &v }

// TestStoreCapacityClearsWholesale pins queryplan.Cache's bound: storing
// into a full store drops every entry, then stores the new one.
func TestStoreCapacityClearsWholesale(t *testing.T) {
	s := intStore(2)
	for key := 0; key < 3; key++ {
		_, hit, err := s.GetOrBuild(context.Background(), key, func() (*int, error) { return intPtr(key), nil })
		require.NoError(t, err)
		require.False(t, hit)
	}
	_, hit, _ := s.GetOrBuild(context.Background(), 2, func() (*int, error) { return intPtr(2), nil })
	require.True(t, hit, "the entry stored after the clear survives")
	for key := 0; key < 2; key++ {
		_, hit, _ = s.GetOrBuild(context.Background(), key, func() (*int, error) { return intPtr(key), nil })
		require.False(t, hit, "key %d must have been cleared at capacity", key)
	}
}

// TestStoreUnboundedWhenCapacityZero pins sqlgen.ProjectionCache's
// unbounded store: no entry is ever evicted.
func TestStoreUnboundedWhenCapacityZero(t *testing.T) {
	s := intStore(0)
	const n = 100
	for key := 0; key < n; key++ {
		_, _, err := s.GetOrBuild(context.Background(), key, func() (*int, error) { return intPtr(key), nil })
		require.NoError(t, err)
	}
	for key := 0; key < n; key++ {
		v, hit, err := s.GetOrBuild(context.Background(), key, func() (*int, error) { return nil, fmt.Errorf("must not rebuild") })
		require.NoError(t, err)
		require.True(t, hit)
		require.Equal(t, key, *v)
	}
	hits, misses := s.Stats()
	require.Equal(t, int64(n), hits)
	require.Equal(t, int64(n), misses)
}

// TestStoreCacheableRejectsResult pins the per-wrapper "not cached" rule: a
// result Cacheable rejects is returned to the leader and its waiters without
// being stored or counted, and the next caller rebuilds.
func TestStoreCacheableRejectsResult(t *testing.T) {
	s := intStore(0)
	type result struct {
		v   *int
		hit bool
		err error
	}
	leaderStarted := make(chan struct{})
	release := make(chan struct{})
	leader := make(chan result, 1)
	go func() {
		v, hit, err := s.GetOrBuild(context.Background(), 1, func() (*int, error) {
			close(leaderStarted)
			<-release
			return nil, nil
		})
		leader <- result{v, hit, err}
	}()
	<-leaderStarted

	follower := make(chan result, 1)
	go func() {
		v, hit, err := s.GetOrBuild(context.Background(), 1, func() (*int, error) {
			return nil, fmt.Errorf("follower built itself")
		})
		follower <- result{v, hit, err}
	}()
	time.Sleep(50 * time.Millisecond) // let the follower reach the wait
	close(release)

	for _, got := range []result{<-leader, <-follower} {
		require.NoError(t, got.err)
		require.False(t, got.hit, "a rejected result is never a hit")
		require.Nil(t, got.v)
	}

	hits, misses := s.Stats()
	require.Zero(t, hits)
	require.Zero(t, misses)
	v, hit, err := s.GetOrBuild(context.Background(), 1, func() (*int, error) { return intPtr(5), nil })
	require.NoError(t, err)
	require.False(t, hit)
	require.Equal(t, 5, *v)
}

// TestStoreDescribeNamesTheBuild pins that the waiter-cancellation and panic
// errors carry the wrapper's description of the build.
func TestStoreDescribeNamesTheBuild(t *testing.T) {
	s := intStore(0)
	leaderStarted := make(chan struct{})
	release := make(chan struct{})
	leaderPanic := make(chan any, 1)
	go func() {
		defer func() { leaderPanic <- recover() }()
		_, _, _ = s.GetOrBuild(context.Background(), 9, func() (*int, error) {
			close(leaderStarted)
			<-release
			panic("boom")
		})
	}()
	<-leaderStarted

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := s.GetOrBuild(ctx, 9, func() (*int, error) { return nil, fmt.Errorf("follower built itself") })
	require.ErrorIs(t, err, context.Canceled)
	require.EqualError(t, err, "wait for in-flight int build 9: context canceled")

	followerErr := make(chan error, 1)
	go func() {
		_, _, err := s.GetOrBuild(context.Background(), 9, func() (*int, error) {
			return nil, fmt.Errorf("follower built itself")
		})
		followerErr <- err
	}()
	time.Sleep(50 * time.Millisecond) // let the follower reach the wait
	close(release)
	require.EqualError(t, <-followerErr, "int build 9 panicked: boom")
	require.Equal(t, "boom", <-leaderPanic)
	require.Zero(t, s.InFlight())
}

func TestNewStoreRequiresPredicates(t *testing.T) {
	describe := func(int) string { return "x" }
	cacheable := func(*int) bool { return true }
	require.Panics(t, func() { NewStore(StoreConfig[int, *int]{Describe: describe}) })
	require.Panics(t, func() { NewStore(StoreConfig[int, *int]{Cacheable: cacheable}) })
}

// TestCacheUncomparableArtifact pins that Cache's nil predicate tolerates an
// artifact whose dynamic type is uncomparable (a slice): it is stored and
// shared rather than panicking on comparison.
func TestCacheUncomparableArtifact(t *testing.T) {
	c := NewCache(8)
	_, _, err := c.GetOrBuild(context.Background(), k("slice"), func() (any, error) { return []string{"a"}, nil })
	require.NoError(t, err)
	v, hit, err := c.GetOrBuild(context.Background(), k("slice"), func() (any, error) { return nil, fmt.Errorf("must not rebuild") })
	require.NoError(t, err)
	require.True(t, hit)
	require.Equal(t, []string{"a"}, v)
}

// TestCacheWaitErrorText pins Cache's error wording through the Store.
func TestCacheWaitErrorText(t *testing.T) {
	c := NewCache(8)
	key := k("text")
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
	cancel()
	_, _, err := c.GetOrBuild(ctx, key, func() (any, error) { return "never", nil })
	require.EqualError(t, err, "wait for in-flight test plan build for schema 7: context canceled")
	close(release)
	<-leaderDone
}
