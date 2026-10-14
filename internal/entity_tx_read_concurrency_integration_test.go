package internal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Concurrency proofs of the row assertion (#650) on real PostgreSQL: special
// scenario 3 of the fence contract, two asserters, and lock order. Each
// transaction runs on its own goroutine and connection; they synchronize on
// channels, and every wait on server state polls pg_locks. Nothing sleeps for
// a fixed time. Each test depends on PostgreSQL blocking a lock waiter, or
// asserts ErrPreconditionFailed where DSQL may answer ErrRetryableConflict,
// so each is _PostgresOnly.

const lockWaitDeadline = 20 * time.Second

// condDependent is a row a fencing Tx writes instead of the fenced row, the
// way LTFlow writes history while fencing an instance.
func condDependent() *forma.EntityOperation {
	return &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: condSchemaName},
		Data: map[string]any{"name": "dependent"}}
}

func condAdvance(rowID uuid.UUID) *forma.EntityOperation {
	return condUpdate(rowID, nil, nil, forma.Increment("version", 1))
}

// awaitRowLockWaiters polls pg_locks until n transactions queue on rowID's
// row lock.
func (e *condPG) awaitRowLockWaiters(t *testing.T, rowID uuid.UUID, n int, what string) {
	t.Helper()
	require.Eventually(t, func() bool { return e.rowLockWaiters(rowID) == n }, lockWaitDeadline, time.Millisecond, what)
}

// holdingTx runs fn in a Tx on its own goroutine, then holds the Tx open
// until release. held closes once fn returned nil; finished answers InTx.
func (e *condPG) holdingTx(fn func(ctx context.Context, tx forma.Tx) error) (held <-chan struct{}, release func(), finished <-chan error) {
	heldCh, releaseCh, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- e.inTx(func(ctx context.Context, tx forma.Tx) error {
			if err := fn(ctx, tx); err != nil {
				return err
			}
			close(heldCh)
			<-releaseCh
			return nil
		})
	}()
	return heldCh, sync.OnceFunc(func() { close(releaseCh) }), done
}

// locksHeldElsewhere probes, from another transaction, whether rowID's
// advisory lock and its entity_main row lock are taken.
func (e *condPG) locksHeldElsewhere(t *testing.T, rowID uuid.UUID) (advisory, row bool) {
	t.Helper()
	probe, err := e.pool.Begin(e.ctx)
	require.NoError(t, err)
	defer func() { _ = probe.Rollback(e.ctx) }()
	var free bool
	require.NoError(t, probe.QueryRow(e.ctx, "SELECT pg_try_advisory_xact_lock($1)", rowVersionLockKey(condSchemaID, rowID)).Scan(&free))
	_, err = probe.Exec(e.ctx, fmt.Sprintf("SELECT 1 FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2 FOR UPDATE NOWAIT",
		sanitizeIdentifier(e.tables.EntityMain)), condSchemaID, rowID)
	var pgErr *pgconn.PgError
	if err != nil {
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, "55P03", pgErr.Code, "the row probe fails only for the lock")
	}
	return !free, err != nil
}

// Get with ForUpdate and AssertRow take both locks the fence relies on, the
// advisory lock every Forma writer takes and the entity_main row lock DSQL's
// conflict detection relies on, and hold them until the Tx ends.
func TestLockingReadsHoldBothLocksUntilTheTxEnds_PostgresOnly(t *testing.T) {
	env := newCondPG(t)
	row := env.create(t, map[string]any{"name": "fenced", "version": int64(4)})
	locking := map[string]func(ctx context.Context, tx forma.Tx) error{
		"AssertRow": func(ctx context.Context, tx forma.Tx) error {
			return tx.AssertRow(ctx, condSchemaName, row, versionEquals(4))
		},
		"Get ForUpdate": func(ctx context.Context, tx forma.Tx) error {
			_, err := tx.Get(ctx, condGet(row), forma.ForUpdate())
			return err
		},
	}
	for name, lock := range locking {
		t.Run(name, func(t *testing.T) {
			held, release, finished := env.holdingTx(lock)
			defer release()
			await(t, held, finished, "the lock")
			advisory, rowLock := env.locksHeldElsewhere(t, row)
			assert.True(t, advisory, "the advisory lock is held")
			assert.True(t, rowLock, "the entity_main row lock is held")

			release()
			require.NoError(t, <-finished)
			advisory, rowLock = env.locksHeldElsewhere(t, row)
			assert.False(t, advisory, "commit released the advisory lock")
			assert.False(t, rowLock, "commit released the row lock")
		})
	}
}

// A locking read of a missing row takes no entity_main row lock but keeps
// the per-row advisory lock, so another write of that row ID waits for the
// Tx.
func TestLockingReadOfAMissingRowKeepsTheAdvisoryLock_PostgresOnly(t *testing.T) {
	env := newCondPG(t)
	missing := uuid.New()
	locking := map[string]func(ctx context.Context, tx forma.Tx) error{
		"AssertRow": func(ctx context.Context, tx forma.Tx) error {
			if err := tx.AssertRow(ctx, condSchemaName, missing, nil); !errors.Is(err, forma.ErrNotFound) {
				return fmt.Errorf("AssertRow of a missing row: want ErrNotFound, got %w", err)
			}
			return nil
		},
		"Get ForUpdate": func(ctx context.Context, tx forma.Tx) error {
			if _, err := tx.Get(ctx, condGet(missing), forma.ForUpdate()); !errors.Is(err, forma.ErrNotFound) {
				return fmt.Errorf("Get ForUpdate of a missing row: want ErrNotFound, got %w", err)
			}
			return nil
		},
	}
	for name, lock := range locking {
		t.Run(name, func(t *testing.T) {
			held, release, finished := env.holdingTx(lock)
			defer release()
			await(t, held, finished, "the lock")
			advisory, rowLock := env.locksHeldElsewhere(t, missing)
			assert.True(t, advisory, "the advisory lock of the missing row is held")
			assert.False(t, rowLock, "there is no entity_main row to lock")

			release()
			require.NoError(t, <-finished)
			advisory, _ = env.locksHeldElsewhere(t, missing)
			assert.False(t, advisory, "commit released the advisory lock")
		})
	}
}

// Get with ForUpdate behind an in-flight writer waits for it, then reads
// what it committed.
func TestGetForUpdateWaitsForAnInFlightWriter_PostgresOnly(t *testing.T) {
	env := newCondPG(t)
	row := env.create(t, map[string]any{"name": "fenced", "version": int64(4)})

	advanced, release, writer := env.holdingTx(func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Update(ctx, condAdvance(row))
		return err
	})
	defer release()
	await(t, advanced, writer, "the advance")

	read := make(chan *forma.DataRecord, 1)
	reader := make(chan error, 1)
	go func() {
		reader <- env.inTx(func(ctx context.Context, tx forma.Tx) error {
			record, err := tx.Get(ctx, condGet(row), forma.ForUpdate())
			if err != nil {
				return err
			}
			read <- record
			return nil
		})
	}()
	env.awaitRowLockWaiters(t, row, 1, "the locking read waits behind the in-flight writer")

	release()
	require.NoError(t, <-writer)
	require.NoError(t, <-reader)
	assert.EqualValues(t, 5, (<-read).Attributes["version"], "the locking read saw the committed advance")
}

// Special scenario 3 (a), assert first: a writer of the asserted row waits
// until the asserter commits, then commits on top of it.
func TestAssertFirstHoldsOffAWriterUntilCommit_PostgresOnly(t *testing.T) {
	env := newCondPG(t)
	row := env.create(t, map[string]any{"name": "fenced", "version": int64(4)})

	var dependent uuid.UUID
	asserted, release, asserter := env.holdingTx(func(ctx context.Context, tx forma.Tx) error {
		if err := tx.AssertRow(ctx, condSchemaName, row, versionEquals(4)); err != nil {
			return err
		}
		d, err := tx.Create(ctx, condDependent())
		if err != nil {
			return err
		}
		dependent = d.RowID
		return nil
	})
	defer release()
	await(t, asserted, asserter, "the assertion")

	writer := make(chan error, 1)
	go func() {
		_, err := env.em.Update(env.ctx, condAdvance(row))
		writer <- err
	}()
	env.awaitRowLockWaiters(t, row, 1, "the writer waits on the asserted row")
	select {
	case err := <-writer:
		t.Fatalf("the writer finished while the asserter was open: %v", err)
	default:
	}

	release()
	require.NoError(t, <-asserter, "the asserter commits")
	require.NoError(t, <-writer, "then the writer commits")
	assert.EqualValues(t, 5, env.version(t, row))
	dependentStamp, _ := env.hotStamp(t, dependent)
	rowStamp, _ := env.hotStamp(t, row)
	assert.GreaterOrEqual(t, rowStamp, dependentStamp,
		"the writer stamped the row after it got the lock, which the asserter's commit released")
}

// Special scenario 3 (b), advance committed first: the assertion reads the
// committed advance and fails, and fn returning that failure rolls back the
// Tx's dependent write, as LTFlow's FenceVersion caller does.
func TestAssertAfterACommittedAdvanceFailsAndCommitsNothing_PostgresOnly(t *testing.T) {
	env := newCondPG(t)
	row := env.create(t, map[string]any{"name": "fenced", "version": int64(4)})

	var dependent uuid.UUID
	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		d, err := tx.Create(ctx, condDependent())
		if err != nil {
			return err
		}
		dependent = d.RowID
		_, err = env.em.Update(env.ctx, condAdvance(row))
		require.NoError(t, err, "another transaction advances the row and commits")
		return tx.AssertRow(ctx, condSchemaName, row, versionEquals(4))
	})
	require.ErrorIs(t, err, forma.ErrPreconditionFailed)
	assert.EqualValues(t, 5, env.version(t, row))
	assert.Empty(t, env.xmin(t, dependent), "the dependent row was rolled back")
}

// Special scenario 3 (c), advance in flight: the asserter waits for the
// advancing writer, then reads its committed version and fails.
func TestAssertBehindAnInFlightAdvanceWaitsThenFails_PostgresOnly(t *testing.T) {
	env := newCondPG(t)
	row := env.create(t, map[string]any{"name": "fenced", "version": int64(4)})

	advanced, release, writer := env.holdingTx(func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Update(ctx, condAdvance(row))
		return err
	})
	defer release()
	await(t, advanced, writer, "the advance")

	var dependent uuid.UUID
	asserter := make(chan error, 1)
	go func() {
		asserter <- env.inTx(func(ctx context.Context, tx forma.Tx) error {
			d, err := tx.Create(ctx, condDependent())
			if err != nil {
				return err
			}
			dependent = d.RowID
			return tx.AssertRow(ctx, condSchemaName, row, versionEquals(4))
		})
	}()
	env.awaitRowLockWaiters(t, row, 1, "the asserter waits behind the in-flight advance")

	release()
	require.NoError(t, <-writer)
	require.ErrorIs(t, <-asserter, forma.ErrPreconditionFailed, "the asserter read the committed advance")
	assert.EqualValues(t, 5, env.version(t, row))
	assert.Empty(t, env.xmin(t, dependent), "the asserter's dependent row was rolled back")
}

// Two asserters of one row do not invalidate each other: they serialize on
// the per-row advisory lock and both commit, and the row is unchanged.
func TestTwoAssertersSerializeAndBothCommit_PostgresOnly(t *testing.T) {
	env := newCondPG(t)
	row := env.create(t, map[string]any{"name": "fenced", "version": int64(4)})
	before := env.snapshot(t, row)
	assertion := func(ctx context.Context, tx forma.Tx) error {
		return tx.AssertRow(ctx, condSchemaName, row, versionEquals(4))
	}

	asserted, release, first := env.holdingTx(assertion)
	defer release()
	await(t, asserted, first, "the first assertion")
	second := make(chan error, 1)
	go func() { second <- env.inTx(assertion) }()
	env.awaitRowLockWaiters(t, row, 1, "the second asserter waits on the first")

	release()
	require.NoError(t, <-first)
	require.NoError(t, <-second)
	assert.Equal(t, before, env.snapshot(t, row))
}

// fenceThenWrite asserts fence, signals mine, waits for theirs, then updates
// target: crossed with a peer it forms a lock cycle.
func (e *condPG) fenceThenWrite(fence, target uuid.UUID, mine chan<- struct{}, theirs <-chan struct{}) error {
	return e.inTx(func(ctx context.Context, tx forma.Tx) error {
		if err := tx.AssertRow(ctx, condSchemaName, fence, nil); err != nil {
			return err
		}
		close(mine)
		select {
		case <-theirs:
		case <-time.After(lockWaitDeadline):
			return errors.New("the peer never asserted its row")
		}
		_, err := tx.Update(ctx, condUpdate(target, nil, map[string]any{"name": "written"}))
		return err
	})
}

// An assertion takes the lock a writer takes, in the writer's order, so it
// adds no deadlock class: crossed with a peer (T1 asserts A then updates B,
// T2 asserts B then updates A) it deadlocks exactly as two crossed writers
// do, and PostgreSQL ends one as ErrRetryableConflict. In canonical order
// (both A then B) both commit.
func TestCrossedAssertAndUpdateIsOneRetryableConflict_PostgresOnly(t *testing.T) {
	env := newCondPG(t)
	a := env.create(t, map[string]any{"name": "a"})
	b := env.create(t, map[string]any{"name": "b"})

	assertedA, assertedB := make(chan struct{}), make(chan struct{})
	errs := runTogether(2, func(i int) error {
		if i == 0 {
			return env.fenceThenWrite(a, b, assertedA, assertedB)
		}
		return env.fenceThenWrite(b, a, assertedB, assertedA)
	})
	var committed, conflicted int
	for i, err := range errs {
		switch {
		case err == nil:
			committed++
		case errors.Is(err, forma.ErrRetryableConflict):
			conflicted++
		default:
			t.Fatalf("crossed transaction %d failed with neither success nor ErrRetryableConflict: %v", i, err)
		}
	}
	assert.Equal(t, 1, committed, "one crossed transaction commits")
	assert.Equal(t, 1, conflicted, "the deadlock victim is ErrRetryableConflict")

	errs = runTogether(2, func(i int) error {
		return env.inTx(func(ctx context.Context, tx forma.Tx) error {
			if err := tx.AssertRow(ctx, condSchemaName, a, nil); err != nil {
				return err
			}
			_, err := tx.Update(ctx, condUpdate(b, nil, map[string]any{"name": fmt.Sprintf("canonical %d", i)}))
			return err
		})
	})
	assert.NoError(t, errs[0], "in canonical order both commit")
	assert.NoError(t, errs[1], "in canonical order both commit")
}
