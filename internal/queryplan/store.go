package queryplan

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// StoreConfig parameterises a Store for one cache wrapper.
type StoreConfig[K comparable, V any] struct {
	// Capacity bounds the entry count: storing into a full store clears it
	// wholesale first. Zero or negative means unbounded.
	Capacity int
	// Cacheable reports whether a successful build result may be stored and
	// shared with waiters. It is the wrapper's "nil result is not cached"
	// rule: V may be an interface holding a typed nil or an uncomparable
	// dynamic type, so the store cannot decide this itself. Required.
	Cacheable func(V) bool
	// Describe names the build for key in waiter-cancellation and panic
	// errors, e.g. "duckdb_federated plan build for schema 7". Required.
	Describe func(K) string
}

// Store is the generic single-flight cache behind queryplan.Cache and
// sqlgen.ProjectionCache (#602). Concurrent misses on one key are
// single-flighted: exactly one goroutine builds, the rest wait and share the
// result, so `misses` equals builds and waiters count as hits.
type Store[K comparable, V any] struct {
	mu      sync.RWMutex
	entries map[K]V
	cfg     StoreConfig[K, V]
	hits    atomic.Int64
	misses  atomic.Int64

	flightMu sync.Mutex
	inflight map[K]*storeFlight[V]
}

// storeFlight is one in-progress build; waiters block on done then read
// v/err. done is closed by the leader's deferred release, so a panicking
// build still frees its waiters (#467).
type storeFlight[V any] struct {
	done chan struct{}
	v    V
	err  error
}

// NewStore creates an empty store. It panics when cfg lacks Cacheable or
// Describe: both are programming errors in the wrapper, not runtime input.
func NewStore[K comparable, V any](cfg StoreConfig[K, V]) *Store[K, V] {
	if cfg.Cacheable == nil || cfg.Describe == nil {
		panic("queryplan.NewStore: StoreConfig.Cacheable and StoreConfig.Describe are required")
	}
	return &Store[K, V]{
		entries:  make(map[K]V),
		cfg:      cfg,
		inflight: make(map[K]*storeFlight[V]),
	}
}

// GetOrBuild returns the stored value for key or builds, stores, and returns
// it. Build errors and results Cacheable rejects are returned without being
// stored. Concurrent callers on the same missing key build exactly once
// (waiters share the result and count as hits). A waiter stops waiting when
// ctx is done and returns the context error; the leader's build is never
// cancelled and its result is still stored. A panic in the leader's build
// surfaces to waiters as an error and is re-raised in the leader. The second
// return reports a cache hit.
func (s *Store[K, V]) GetOrBuild(ctx context.Context, key K, build func() (V, error)) (V, bool, error) {
	if v, ok := s.lookup(key); ok {
		s.hits.Add(1)
		return v, true, nil
	}

	s.flightMu.Lock()
	// Re-check under the flight lock: the previous flight may have stored
	// the entry between our read miss and here.
	if v, ok := s.lookup(key); ok {
		s.flightMu.Unlock()
		s.hits.Add(1)
		return v, true, nil
	}
	if fl, running := s.inflight[key]; running {
		s.flightMu.Unlock()
		return s.awaitFlight(ctx, key, fl)
	}
	fl := &storeFlight[V]{done: make(chan struct{})}
	s.inflight[key] = fl
	s.flightMu.Unlock()
	defer s.releaseFlight(key, fl)

	v, err := build()
	fl.v, fl.err = v, err
	if err == nil && s.cfg.Cacheable(v) {
		s.misses.Add(1)
		s.store(key, v)
	}
	return v, false, err
}

func (s *Store[K, V]) lookup(key K) (V, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.entries[key]
	return v, ok
}

func (s *Store[K, V]) store(key K, v V) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cfg.Capacity > 0 && len(s.entries) >= s.cfg.Capacity {
		s.entries = make(map[K]V)
	}
	s.entries[key] = v
}

// awaitFlight waits for another caller's build of key, or for ctx. A shared
// success counts as a hit; a cancelled wait touches no counter.
func (s *Store[K, V]) awaitFlight(ctx context.Context, key K, fl *storeFlight[V]) (V, bool, error) {
	select {
	case <-fl.done:
	case <-ctx.Done():
		var zero V
		return zero, false, fmt.Errorf("wait for in-flight %s: %w", s.cfg.Describe(key), ctx.Err())
	}
	if fl.err != nil || !s.cfg.Cacheable(fl.v) {
		return fl.v, false, fl.err
	}
	s.hits.Add(1)
	return fl.v, true, nil
}

// releaseFlight is deferred by the leader: it drops the in-flight slot and
// wakes waiters whether build returned or panicked. A panic is recorded as
// the flight's error so waiters fail instead of hanging, then re-raised so
// the leader's own failure mode is unchanged (#467).
func (s *Store[K, V]) releaseFlight(key K, fl *storeFlight[V]) {
	if r := recover(); r != nil {
		var zero V
		fl.v, fl.err = zero, fmt.Errorf("%s panicked: %v", s.cfg.Describe(key), r)
		defer panic(r)
	}
	s.flightMu.Lock()
	delete(s.inflight, key)
	s.flightMu.Unlock()
	close(fl.done)
}

// Stats returns cumulative hit and miss counts: misses equals stored builds,
// and waiters that shared a leader's result count as hits.
func (s *Store[K, V]) Stats() (hits, misses int64) {
	return s.hits.Load(), s.misses.Load()
}

// Reset drops all entries (schema-generation invalidation hook). Counters
// and in-flight builds are untouched.
func (s *Store[K, V]) Reset() {
	s.mu.Lock()
	s.entries = make(map[K]V)
	s.mu.Unlock()
}

// InFlight reports how many builds currently hold a single-flight slot; tests
// use it to pin that a finished or panicked build released its slot.
func (s *Store[K, V]) InFlight() int {
	s.flightMu.Lock()
	defer s.flightMu.Unlock()
	return len(s.inflight)
}
