package internal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Concurrency proofs of Claim (#653) on real PostgreSQL. Each worker runs
// its own Tx on its own connection; they synchronize on channels and
// barriers, and every wait on server state polls pg_locks with a deadline.
// Nothing sleeps for a fixed time. Each test relies on PostgreSQL's
// non-blocking skip or asserts outcomes DSQL may answer as
// ErrRetryableConflict, so each is _PostgresOnly.

// claimedRow is one entry of a worker's result log.
type claimedRow struct {
	row        uuid.UUID
	generation int64
}

// drain claims topic in Tx after Tx of limit rows, committing each, until a
// claim answers nothing, and answers what it claimed.
func (e *outboxPG) drain(topic, worker string, limit int) ([]claimedRow, error) {
	var log []claimedRow
	for {
		records, err := e.claim(claimLeasing(topic, worker, limit, heldLease))
		if err != nil {
			return log, err
		}
		if len(records) == 0 {
			return log, nil
		}
		for _, record := range records {
			g, ok := record.Attributes["lock_generation"].(int64)
			if !ok {
				return log, fmt.Errorf("row %s: lock_generation is %T", record.RowID, record.Attributes["lock_generation"])
			}
			log = append(log, claimedRow{record.RowID, g})
		}
	}
}

// #593 claim list 1, special scenario 6: concurrent claimers never receive
// the same row, and together they claim every row once.
func TestClaimNeverHandsOutARowTwice_PostgresOnly(t *testing.T) {
	for _, workers := range []int{2, 8} {
		t.Run(fmt.Sprintf("%d workers", workers), func(t *testing.T) {
			env := newOutboxPG(t)
			for iteration := range 20 {
				topic := fmt.Sprintf("it-%d", iteration)
				ids := env.createMessages(t, topic, 200)
				logs := make([][]claimedRow, workers)
				errs := race(workers, func(w int) error {
					var err error
					logs[w], err = env.drain(topic, fmt.Sprintf("w%d", w), 5)
					return err
				})
				owner := map[uuid.UUID]int{}
				for w, err := range errs {
					require.NoError(t, err, "worker %d", w)
					for _, c := range logs[w] {
						if first, dup := owner[c.row]; dup {
							t.Fatalf("iteration %d: row %s claimed by workers %d and %d", iteration, c.row, first, w)
						}
						owner[c.row] = w
						require.EqualValues(t, 1, c.generation, "one claim per row")
					}
				}
				require.Len(t, owner, len(ids), "iteration %d: the union is every row", iteration)
				for _, id := range ids {
					require.Contains(t, owner, id)
				}
			}
		})
	}
}

// #593 claim list 2: at most one claim holds a live lease on a row. Rounds
// of four racing claimers each yield exactly one claim, a later claim gets
// nothing while that lease is live, and the expiry CAS on the winner's
// generation passes the row to the next round.
func TestClaimAtMostOneActiveClaimant_PostgresOnly(t *testing.T) {
	env := newOutboxPG(t)
	row := env.create(t, pendingMessage("t", 0))
	for round := range 10 {
		records := make([][]*forma.DataRecord, 4)
		errs := race(4, func(w int) error {
			var err error
			records[w], err = env.claim(claimLeasing("t", fmt.Sprintf("w%d", w), 1, heldLease))
			return err
		})
		var won []*forma.DataRecord
		for w, err := range errs {
			require.NoError(t, err, "round %d worker %d", round, w)
			won = append(won, records[w]...)
		}
		require.Len(t, won, 1, "round %d: exactly one claimer gets the row", round)
		g := generationOf(t, won[0])
		require.EqualValues(t, round+1, g, "generations rise by one per claim")

		late, err := env.claim(claimOf("t", "late", 1))
		require.NoError(t, err)
		require.Empty(t, late, "round %d: the live lease is not claimable", round)
		s := env.stored(t, row)
		require.Equal(t, g, s.generation)
		require.Greater(t, *s.lockedUntil, time.Now().UnixMilli(), "the stored lease is the winner's and live")

		env.expireLease(t, row, g)
	}
}

// #593 claim list 8, special scenario 9: a claim whose Tx rolls back leaves
// the row as it was, holds no lock afterwards, and another worker claims the
// row at once. The observer connection is checked out before the Tx, so the
// Tx runs on another backend; B claims while the rolled-back backend is
// checked out, so B runs on a third. A lock that outlived the rollback would
// show in pg_locks and make B skip the row.
func TestClaimRollbackReleasesTheRow_PostgresOnly(t *testing.T) {
	env := newOutboxPG(t)
	row := env.create(t, pendingMessage("t", 0))
	before := env.snapshot(t, row)
	failed := errors.New("fn failed after the claim")
	observer, err := env.pool.Acquire(env.ctx)
	require.NoError(t, err)
	defer observer.Release()

	var pid uint32
	err = env.inTx(func(ctx context.Context, tx forma.Tx) error {
		pid = tx.(*txSession).tx.Conn().PgConn().PID()
		result, err := tx.Claim(ctx, claimOf("t", "A", 1))
		require.NoError(t, err)
		require.Len(t, result.Records, 1)
		return failed
	})
	require.ErrorIs(t, err, failed)
	require.NotEqual(t, observer.Conn().PgConn().PID(), pid, "the Tx ran on another backend")
	assert.Equal(t, before, env.snapshot(t, row), "entity_main, EAV and change_log are as they were")
	var locks int
	require.NoError(t, observer.QueryRow(env.ctx, "SELECT count(*) FROM pg_locks WHERE pid = $1", int32(pid)).Scan(&locks))
	assert.Zero(t, locks, "the rolled-back backend holds no lock")

	rolledBack := env.acquireBackend(t, pid)
	defer rolledBack.Release()
	b := env.claimOne(t, "t", "B")
	assert.Equal(t, row, b.RowID)
	assert.EqualValues(t, 1, generationOf(t, b), "the rolled-back claim left no generation")
}

// acquireBackend checks out the pool's connection to backend pid, so the
// next Tx runs on another backend.
func (e *outboxPG) acquireBackend(t *testing.T, pid uint32) *pgxpool.Conn {
	t.Helper()
	var others []*pgxpool.Conn
	defer func() {
		for _, c := range others {
			c.Release()
		}
	}()
	for range e.pool.Config().MaxConns {
		c, err := e.pool.Acquire(e.ctx)
		require.NoError(t, err)
		if c.Conn().PgConn().PID() == pid {
			return c
		}
		others = append(others, c)
	}
	t.Fatalf("backend %d is not in the pool", pid)
	return nil
}

// The non-blocking proof: with r1 held by another transaction, a claim
// skips it, claims r2..r5, and returns while the holder is still open, and
// its backend never waits on a lock.
func TestClaimSkipsHeldRowsWithoutWaiting_PostgresOnly(t *testing.T) {
	holders := map[string]func(ctx context.Context, tx forma.Tx, r1 uuid.UUID) error{
		"an update": func(ctx context.Context, tx forma.Tx, r1 uuid.UUID) error {
			_, err := tx.Update(ctx, &forma.EntityOperation{
				EntityIdentifier: forma.EntityIdentifier{SchemaName: outboxSchemaName, RowID: r1},
				Updates:          map[string]any{"locked_by": "holder"}})
			return err
		},
		"a row assertion": func(ctx context.Context, tx forma.Tx, r1 uuid.UUID) error {
			return tx.AssertRow(ctx, outboxSchemaName, r1, nil)
		},
		"another claim": func(ctx context.Context, tx forma.Tx, r1 uuid.UUID) error {
			result, err := tx.Claim(ctx, claimOf("t", "holder", 1))
			if err == nil && (len(result.Records) != 1 || result.Records[0].RowID != r1) {
				err = fmt.Errorf("the holder claimed %v, not r1", rowIDsOf(result.Records))
			}
			return err
		},
	}
	for name, hold := range holders {
		t.Run(name, func(t *testing.T) {
			env := newOutboxPG(t)
			ids := env.createMessages(t, "t", 5)
			held, release, holder := env.holdingTx(func(ctx context.Context, tx forma.Tx) error { return hold(ctx, tx, ids[0]) })
			defer release()
			await(t, held, holder, "the holder")

			result, waited := env.claimWatched(t, claimOf("t", "B", 10))
			select {
			case err := <-holder:
				t.Fatalf("the holder ended before the claim was judged: %v", err)
			default:
			}
			assert.Equal(t, ids[1:], rowIDsOf(result), "r1 is skipped, r2..r5 are claimed")
			assert.False(t, waited, "the claim's backend never had an ungranted lock")
			release()
			require.NoError(t, <-holder)
		})
	}
}

// A row lock taken outside Forma, without the advisory lock, is skipped by
// the re-check's SKIP LOCKED: the claim takes r2..r5 and returns while that
// lock is held, and claims r1 once it is released.
func TestClaimSkipsARowLockedOutsideForma_PostgresOnly(t *testing.T) {
	env := newOutboxPG(t)
	ids := env.createMessages(t, "t", 5)
	raw := env.lockRowOutsideForma(t, ids[0])

	result, waited := env.claimWatched(t, claimOf("t", "B", 10))
	assert.Equal(t, ids[1:], rowIDsOf(result), "r1 is skipped, r2..r5 are claimed")
	assert.False(t, waited, "the claim's backend never had an ungranted lock")
	require.NoError(t, raw.Rollback(env.ctx))
	records, err := env.claim(claimOf("t", "B", 10))
	require.NoError(t, err)
	assert.Equal(t, ids[:1], rowIDsOf(records), "once released, r1 is claimable")
}

// claimWatched runs a claim in its own Tx while a monitor polls pg_locks for
// an ungranted lock of the claim's backend, and answers the claim's records
// and whether the monitor saw one. The claim starts only once the monitor
// has polled.
func (e *outboxPG) claimWatched(t *testing.T, req *forma.ClaimRequest) ([]*forma.DataRecord, bool) {
	t.Helper()
	pids, watching, finished := make(chan uint32, 1), make(chan struct{}), make(chan struct{})
	var waited bool
	var monitor sync.WaitGroup
	stop := sync.OnceFunc(func() {
		close(finished)
		monitor.Wait()
	})
	defer stop()
	monitor.Add(1)
	go func() {
		defer monitor.Done()
		var pid int32
		select {
		case p := <-pids:
			pid = int32(p)
		case <-finished:
			return
		}
		for first := true; ; first = false {
			var n int
			if err := e.pool.QueryRow(e.ctx, "SELECT count(*) FROM pg_locks WHERE pid = $1 AND NOT granted", pid).Scan(&n); err == nil && n > 0 {
				waited = true
			}
			if first {
				close(watching)
			}
			select {
			case <-finished:
				return
			default:
			}
		}
	}()
	var records []*forma.DataRecord
	done := make(chan error, 1)
	go func() {
		done <- e.inTx(func(ctx context.Context, tx forma.Tx) error {
			pids <- tx.(*txSession).tx.Conn().PgConn().PID()
			<-watching
			result, err := tx.Claim(ctx, req)
			if err == nil {
				records = result.Records
			}
			return err
		})
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(lockWaitDeadline):
		t.Fatal("the claim did not return while the holder was open")
	}
	stop()
	return records, waited
}

// Concurrent completions after a takeover: B claims R at g+1 and completes
// inside its claim's Tx while A's completion at g races it from a barrier.
// A always answers ErrPreconditionFailed and B commits.
func TestClaimConcurrentCompletionsFenceTheStaleHolder_PostgresOnly(t *testing.T) {
	env := newOutboxPG(t)
	for iteration := range 20 {
		topic := fmt.Sprintf("it-%d", iteration)
		row := env.create(t, pendingMessage(topic, 0))
		g := generationOf(t, env.claimOne(t, topic, "A"))
		env.expireLease(t, row, g)

		claimed, barrier := make(chan struct{}), make(chan struct{})
		bDone, aDone := make(chan error, 1), make(chan error, 1)
		go func() {
			bDone <- env.inTx(func(ctx context.Context, tx forma.Tx) error {
				result, err := tx.Claim(ctx, claimOf(topic, "B", 1))
				if err != nil || len(result.Records) != 1 {
					close(claimed)
					return fmt.Errorf("B's claim answered %v, %v", result, err)
				}
				close(claimed)
				<-barrier
				_, err = tx.Update(ctx, complete(row, g+1))
				return err
			})
		}()
		go func() {
			<-claimed
			<-barrier
			_, err := env.em.Update(env.ctx, complete(row, g))
			aDone <- err
		}()
		<-claimed
		close(barrier)
		require.NoError(t, <-bDone, "iteration %d: B completes", iteration)
		require.ErrorIs(t, <-aDone, forma.ErrPreconditionFailed, "iteration %d: A's completion is fenced", iteration)
		s := env.stored(t, row)
		assert.Equal(t, "published", s.status)
		assert.Equal(t, g+1, s.generation)
	}
}

// Rollback under contention: A alternates claims it rolls back with claims
// it commits while B drains. Every row A rolled back is claimed later, and
// the committed claims cover the fixture once.
func TestClaimRollbackUnderContentionLosesNoRow_PostgresOnly(t *testing.T) {
	env := newOutboxPG(t)
	ids := env.createMessages(t, "t", 60)
	rollback := errors.New("A rolls back")
	var rolledBack, aCommitted, bCommitted []claimedRow
	errs := race(2, func(w int) error {
		if w == 1 {
			var err error
			bCommitted, err = env.drain("t", "B", 5)
			return err
		}
		for {
			var taken []*forma.DataRecord
			err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
				result, err := tx.Claim(ctx, claimLeasing("t", "A", 5, heldLease))
				if err != nil {
					return err
				}
				taken = result.Records
				return rollback
			})
			if !errors.Is(err, rollback) {
				return fmt.Errorf("A's rolled-back claim answered %w", err)
			}
			for _, record := range taken {
				rolledBack = append(rolledBack, claimedRow{row: record.RowID})
			}
			committed, err := env.claim(claimLeasing("t", "A", 5, heldLease))
			if err != nil {
				return err
			}
			for _, record := range committed {
				aCommitted = append(aCommitted, claimedRow{row: record.RowID})
			}
			if len(taken) == 0 && len(committed) == 0 {
				return nil
			}
		}
	})
	require.NoError(t, errs[0], "A")
	require.NoError(t, errs[1], "B")

	claims := map[uuid.UUID]int{}
	for _, c := range append(aCommitted, bCommitted...) {
		claims[c.row]++
	}
	for _, id := range ids {
		assert.Equal(t, 1, claims[id], "row %s is claimed exactly once", id)
		assert.EqualValues(t, 1, env.stored(t, id).generation, "row %s: rollbacks left no generation", id)
	}
	assert.Len(t, claims, len(ids))
	for _, c := range rolledBack {
		assert.Contains(t, claims, c.row, "a row A rolled back was claimed later")
	}
	assert.NotEmpty(t, rolledBack, "A rolled back at least one claim")
}

// The claim's try-lock and lockRowVersion share one key: a row whose
// version lock another transaction holds is skipped, and a row a claim holds
// is one lockRowVersion would wait for.
func TestClaimTryLockSharesTheRowVersionLockKey_PostgresOnly(t *testing.T) {
	env := newOutboxPG(t)
	ids := env.createMessages(t, "t", 2)

	locker, err := env.pool.Begin(env.ctx)
	require.NoError(t, err)
	defer func() { _ = locker.Rollback(env.ctx) }()
	require.NoError(t, lockRowVersion(env.ctx, locker, outboxSchemaID, ids[0]))
	records, err := env.claim(claimOf("t", "B", 10))
	require.NoError(t, err)
	assert.Equal(t, ids[1:], rowIDsOf(records), "the row lockRowVersion holds is skipped")
	require.NoError(t, locker.Rollback(env.ctx))

	held, release, holder := env.holdingTx(func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Claim(ctx, claimOf("t", "A", 1))
		return err
	})
	defer release()
	await(t, held, holder, "the claim")
	assert.True(t, env.versionLockHeld(t, ids[0]), "the claimed row's version lock is held")
	release()
	require.NoError(t, <-holder)
}
