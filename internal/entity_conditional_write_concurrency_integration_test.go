package internal

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Concurrency proofs of conditional mutations (#649) on real PostgreSQL. Each
// writer runs on its own goroutine, connection and transaction; writers start
// together on a closed channel, and the one wait on server state polls
// pg_locks. Nothing sleeps for a fixed time.

// runTogether runs n writers released at once and answers their errors in
// writer order.
func runTogether(n int, write func(i int) error) []error {
	start := make(chan struct{})
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs[i] = write(i)
		}()
	}
	close(start)
	wg.Wait()
	return errs
}

// holdRowLock takes rowID's row lock on its own transaction and answers a
// release that commits it.
func (e *condPG) holdRowLock(t *testing.T, rowID uuid.UUID) (release func()) {
	t.Helper()
	tx, err := e.pool.Begin(e.ctx)
	require.NoError(t, err)
	_, err = tx.Exec(e.ctx, "SELECT pg_advisory_xact_lock($1)", rowVersionLockKey(condSchemaID, rowID))
	require.NoError(t, err)
	return sync.OnceFunc(func() { require.NoError(t, tx.Commit(e.ctx)) })
}

// rowLockWaiters counts the transactions queued on rowID's row lock.
func (e *condPG) rowLockWaiters(rowID uuid.UUID) int {
	key := uint64(rowVersionLockKey(condSchemaID, rowID))
	var n int
	err := e.pool.QueryRow(e.ctx, `SELECT count(*) FROM pg_locks
		WHERE locktype = 'advisory' AND NOT granted AND classid::bigint = $1 AND objid::bigint = $2 AND objsubid = 1`,
		int64(key>>32), int64(key&0xffffffff)).Scan(&n)
	if err != nil {
		return -1
	}
	return n
}

func versionEquals(v int64) forma.Condition {
	return kv("version", fmt.Sprintf("equals:%d", v))
}

func (e *condPG) version(t *testing.T, rowID uuid.UUID) int64 {
	t.Helper()
	v, ok := e.get(t, rowID)["version"].(int64)
	require.True(t, ok, "version is a bigint")
	return v
}

// TestSameExpectedVersionAdmitsOneWriter_PostgresOnly is special scenario 2:
// two writers both expect version 7 and increment it. Each iteration queues
// both on the row lock before either runs, so both pass intake against
// version 7 and the loser evaluates its precondition after the winner
// commits. Exactly one commits, the other is ErrPreconditionFailed (the
// PostgreSQL assertion; the backend-neutral one is "not both"), the version
// moves 7 to 8, and the row carries one new change_log stamp.
func TestSameExpectedVersionAdmitsOneWriter_PostgresOnly(t *testing.T) {
	const iterations = 60
	e := newCondPG(t)
	rowID := e.create(t, map[string]any{"version": int64(7), "name": "contended"})

	for iter := range iterations {
		_, err := e.em.Update(e.ctx, condUpdate(rowID, nil, map[string]any{"version": int64(7)}))
		require.NoError(t, err)
		stampBefore, _ := e.hotStamp(t, rowID)

		release := e.holdRowLock(t, rowID)
		results := make(chan []error, 1)
		go func() {
			results <- runTogether(2, func(int) error {
				_, err := e.em.Update(e.ctx, condUpdate(rowID, versionEquals(7), nil, forma.Increment("version", 1)))
				return err
			})
		}()
		require.Eventually(t, func() bool { return e.rowLockWaiters(rowID) == 2 }, 20*time.Second, time.Millisecond,
			"iteration %d: both writers queue on the row lock", iter)
		release()
		errs := <-results

		var won, lost int
		for _, err := range errs {
			switch {
			case err == nil:
				won++
			case errors.Is(err, forma.ErrPreconditionFailed) && !errors.Is(err, forma.ErrNotFound):
				lost++
			default:
				t.Fatalf("iteration %d: a writer failed with neither success nor ErrPreconditionFailed: %v", iter, err)
			}
		}
		require.Equal(t, 1, won, "iteration %d: exactly one writer commits", iter)
		require.Equal(t, 1, lost, "iteration %d: the other is refused", iter)
		require.EqualValues(t, 8, e.version(t, rowID), "iteration %d: one increment", iter)
		stampAfter, hot := e.hotStamp(t, rowID)
		require.Greater(t, stampAfter, stampBefore, "iteration %d: the winner stamped the row", iter)
		require.Equal(t, []int64{stampAfter}, hot, "iteration %d: one hot change_log stamp, the winner's", iter)
	}
}

// TestConcurrentIncrementsLoseNothing runs 32 unconditional increments of
// an EAV and a column-bound attribute at once: each reads the locked row, so
// the total is exact.
func TestConcurrentIncrementsLoseNothing(t *testing.T) {
	const writers = 32
	e := newCondPG(t)
	rowID := e.create(t, map[string]any{"counter": int64(100), "rank": int64(-5)})

	errs := runTogether(writers, func(int) error {
		_, err := e.em.Update(e.ctx, condUpdate(rowID, nil, nil, forma.Increment("counter", 1), forma.Increment("rank", 1)))
		return err
	})
	for i, err := range errs {
		require.NoError(t, err, "writer %d", i)
	}
	got := e.get(t, rowID)
	assert.EqualValues(t, 100+writers, got["counter"], "EAV integer")
	assert.EqualValues(t, -5+writers, got["rank"], "column-bound integer")
}

// TestVersionRetryLoopLosesNoUpdate runs N writers that each read the
// version, increment it under "version equals what I read", and on
// ErrPreconditionFailed read again. Every refusal is that error, never
// ErrNotFound or another, and every writer lands exactly once. Each write
// also sets counter to the version it read plus one, a value computed from
// the read: the increment alone reads the locked row and would total right
// even if two writers passed one precondition, but counter would then lag.
func TestVersionRetryLoopLosesNoUpdate(t *testing.T) {
	const writers = 12
	const initial = 40
	e := newCondPG(t)
	rowID := e.create(t, map[string]any{"version": int64(initial), "counter": int64(initial)})

	var mu sync.Mutex
	refusals := 0
	errs := runTogether(writers, func(int) error {
		for attempt := 0; attempt < 1000; attempt++ {
			record, err := e.em.Get(e.ctx, &forma.QueryRequest{SchemaName: condSchemaName, RowID: &rowID})
			if err != nil {
				return fmt.Errorf("read version: %w", err)
			}
			seen, ok := record.Attributes["version"].(int64)
			if !ok {
				return fmt.Errorf("version is %T", record.Attributes["version"])
			}
			_, err = e.em.Update(e.ctx, condUpdate(rowID, versionEquals(seen), map[string]any{"counter": seen + 1}, forma.Increment("version", 1)))
			if err == nil {
				return nil
			}
			if !errors.Is(err, forma.ErrPreconditionFailed) || errors.Is(err, forma.ErrNotFound) {
				return fmt.Errorf("a refusal other than ErrPreconditionFailed: %w", err)
			}
			mu.Lock()
			refusals++
			mu.Unlock()
		}
		return errors.New("no attempt succeeded")
	})
	for i, err := range errs {
		require.NoError(t, err, "writer %d", i)
	}
	assert.EqualValues(t, initial+writers, e.version(t, rowID), "every writer's increment landed once")
	assert.EqualValues(t, initial+writers, e.get(t, rowID)["counter"], "no write computed from a stale read landed")
	t.Logf("%d writers, %d precondition refusals retried", writers, refusals)
}
