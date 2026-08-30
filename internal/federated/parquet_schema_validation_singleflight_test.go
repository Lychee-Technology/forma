package federated

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The footer probe deliberately runs OUTSIDE the validator's mutex — a DESCRIBE
// is a network round trip, and holding the lock across it would serialize every
// path of every concurrent query. The cost of that choice is that concurrent
// misses on the same object each issue their own DESCRIBE, and a slower one can
// publish its cache entry after a faster, newer one. These tests pin the
// per-key single-flight that closes it.

// slowDescribeExecutor answers DESCRIBE probes after a deliberate delay, wide
// enough that a second caller arriving concurrently is guaranteed to find the
// first still in flight. It counts probes atomically and can gate on a release
// channel so a test can hold the leader mid-probe.
type slowDescribeExecutor struct {
	describes atomic.Int32
	delay     time.Duration
	// release, when non-nil, blocks every probe until it is closed.
	release chan struct{}
	// cols is the DESCRIBE answer; it satisfies the parquetcheck invariant.
	cols [][2]string
}

func newSlowDescribeExecutor(delay time.Duration) *slowDescribeExecutor {
	return &slowDescribeExecutor{
		delay: delay,
		cols: [][2]string{
			{"row_id", "UUID"}, {"changed_at", "BIGINT"}, {"deleted_at", "BIGINT"}, {"ltbase_created_at", "BIGINT"}, {"city", "VARCHAR"},
		},
	}
}

func (s *slowDescribeExecutor) Query(ctx context.Context, sql string, args ...any) (duckDBRowsIterator, error) {
	if !strings.HasPrefix(sql, "DESCRIBE ") {
		return nil, fmt.Errorf("unexpected non-describe query: %s", sql)
	}
	s.describes.Add(1)
	if s.release != nil {
		<-s.release
	}
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	return &fakeDescribeRows{cols: s.cols}, nil
}

// validateConcurrently runs n concurrent Validate calls for the given path and
// stamp and returns once all have finished, failing on any error.
func validateConcurrently(t *testing.T, v *parquetSchemaValidator, duck DuckDBQueryExecutor,
	n int, path string, stamp map[string]string,
) {
	t.Helper()
	stamps := map[string]map[string]string{}
	if stamp != nil {
		stamps[path] = stamp
	}
	var wg sync.WaitGroup
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, _, err := v.Validate(context.Background(), duck, []string{path}, stamps)
			errs[i] = err
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "concurrent Validate #%d", i)
	}
}

// TestValidatorSingleFlightsConcurrentProbes is the P2 fix's load-bearing
// assertion: N concurrent misses on the same (path, stamp) cost exactly ONE
// DESCRIBE. Without the single-flight this is N.
func TestValidatorSingleFlightsConcurrentProbes(t *testing.T) {
	v := newParquetSchemaValidator()
	duck := newSlowDescribeExecutor(50 * time.Millisecond)

	validateConcurrently(t, v, duck, 8, stampCachePath, nil)

	require.Equal(t, int32(1), duck.describes.Load(),
		"concurrent misses on one (path, stamp) must collapse to a single footer probe")
}

// TestValidatorSingleFlightDoesNotCollapseDistinctStamps pins the other half:
// the key is (path, stamp), not path. Two queries racing on the same object
// under DIFFERENT manifest generations must each get their own probe —
// collapsing them would let one generation's columns answer the other's.
//
// Both stamps here deliberately FAIL the invariant (no row_id), which is what
// forces the probe fallthrough while still leaving distinct stamps in play.
//
// The count is asserted while both leaders are parked inside their probe,
// which is the only deterministic window: once they publish, the two
// generations overwrite each other's single cache entry for this path, so
// woken followers legitimately re-probe (measured: a post-join total ranges
// over 2..8 across runs, which is why nothing here asserts a final total).
// In-flight is what the single-flight controls, so in-flight is what this
// measures — exactly 2 probes and exactly 2 leaders for 8 callers across 2
// generations.
//
// The parked window needs no wall-clock sampling to be safe. beginProbe admits
// one leader per key under the mutex, and a follower cannot probe before its
// leader's done channel closes, which finishProbe only reaches after release.
// So while the gate is shut the counts are structurally pinned, and one
// snapshot taken under the validator's own mutex is the whole assertion.
func TestValidatorSingleFlightDoesNotCollapseDistinctStamps(t *testing.T) {
	v := newParquetSchemaValidator()
	duck := newSlowDescribeExecutor(0)
	duck.release = make(chan struct{})

	stampA := map[string]string{"changed_at": "BIGINT", "city": "VARCHAR"}
	stampB := map[string]string{"changed_at": "BIGINT", "score": "BIGINT"}

	var wg sync.WaitGroup
	for _, stamp := range []map[string]string{stampA, stampB} {
		wg.Add(1)
		go func(stamp map[string]string) {
			defer wg.Done()
			validateConcurrently(t, v, duck, 4, stampCachePath, stamp)
		}(stamp)
	}

	require.Eventually(t, func() bool { return duck.describes.Load() == 2 }, 2*time.Second, time.Millisecond,
		"two distinct stamp generations on one path must EACH get a probe")

	// Both leaders are now parked on the gate. Snapshot the leadership map and
	// the probe count together: 2 in-flight keys means the 6 followers took
	// leadership of nothing, and the count still at 2 means none of them
	// probed on its own.
	v.mu.Lock()
	inflight := len(v.inflight)
	v.mu.Unlock()
	require.Equal(t, 2, inflight,
		"one leader per (path, stamp) — the 6 followers must hold no probe of their own")
	require.Equal(t, int32(2), duck.describes.Load(),
		"the other 6 callers must have collapsed onto the two leaders, not probed on their own")

	close(duck.release)
	wg.Wait()
}

// TestValidatorSingleFlightKeyDistinguishesStamps is the direct proof that the
// fingerprint cannot alias. It is the assertion that would catch a naive
// separator (an empty or "=" join collapses {"a":"b=c"} and {"a=b":"c"}).
func TestValidatorSingleFlightKeyDistinguishesStamps(t *testing.T) {
	require.NotEqual(t,
		probeKey("s3://b/x.parquet", map[string]string{"a": "b=c"}),
		probeKey("s3://b/x.parquet", map[string]string{"a=b": "c"}),
		"the stamp fingerprint must not alias two distinct column maps")
	require.NotEqual(t,
		probeKey("s3://b/x.parquet", nil),
		probeKey("s3://b/x.parquet", map[string]string{"row_id": "UUID"}),
		"an absent stamp is a different generation from any present one")

	// Map iteration order must not leak into the key.
	stamp := map[string]string{"row_id": "UUID", "changed_at": "BIGINT", "deleted_at": "BIGINT", "ltbase_created_at": "BIGINT", "city": "VARCHAR"}
	first := probeKey("s3://b/x.parquet", stamp)
	for i := 0; i < 20; i++ {
		require.Equal(t, first, probeKey("s3://b/x.parquet", stamp),
			"the fingerprint must be deterministic across map iteration orders")
	}
}

// TestValidatorSingleFlightFollowerHonoursCancellation pins that a follower
// waiting on a leader is not held hostage by it: with the leader parked
// mid-probe, a follower whose context is cancelled must return promptly on the
// inconclusive path (complete=false, no error) — exactly what a failed probe
// does today — rather than blocking until the leader finishes.
func TestValidatorSingleFlightFollowerHonoursCancellation(t *testing.T) {
	v := newParquetSchemaValidator()
	duck := newSlowDescribeExecutor(0)
	duck.release = make(chan struct{})

	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		_, _, err := v.Validate(context.Background(), duck, []string{stampCachePath}, nil)
		require.NoError(t, err)
	}()

	// Wait until the leader is actually inside the probe, so the follower is
	// guaranteed to take the wait branch rather than becoming a leader itself.
	require.Eventually(t, func() bool { return duck.describes.Load() == 1 }, 2*time.Second, time.Millisecond,
		"leader never entered the probe")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	followerDone := make(chan struct{})
	go func() {
		defer close(followerDone)
		_, complete, err := v.Validate(ctx, duck, []string{stampCachePath}, nil)
		require.NoError(t, err, "a cancelled follower takes the inconclusive path, not an error")
		require.False(t, complete, "an inconclusive path must not claim a complete union (#255)")
	}()

	select {
	case <-followerDone:
	case <-time.After(5 * time.Second):
		close(duck.release)
		t.Fatal("a follower with a cancelled context blocked on the leader's probe")
	}

	close(duck.release)
	<-leaderDone
	require.Equal(t, int32(1), duck.describes.Load(),
		"the cancelled follower must not have issued a probe of its own")
}

// TestValidatorSingleFlightFollowerStillSeesViolation guards the one way a
// single-flight could quietly weaken the contract. When the leader's probe
// raises an invariant violation it publishes nothing, so a follower woken by it
// finds no cache entry. It must then probe itself and reach the SAME loud
// failure — not inherit the leader's silence and downgrade to "inconclusive".
func TestValidatorSingleFlightFollowerStillSeesViolation(t *testing.T) {
	v := newParquetSchemaValidator()
	duck := newSlowDescribeExecutor(20 * time.Millisecond)
	duck.cols = [][2]string{{"changed_at", "BIGINT"}, {"deleted_at", "BIGINT"}} // no row_id

	const n = 6
	var wg sync.WaitGroup
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, _, errs[i] = v.Validate(context.Background(), duck, []string{stampCachePath}, nil)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		require.Error(t, err, "caller #%d must see the invariant violation, not a silent downgrade", i)
		require.ErrorIs(t, err, ErrFederatedReadFailed)
		require.Contains(t, err.Error(), "row_id")
	}
}
