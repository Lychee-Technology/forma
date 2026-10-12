package internal

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Concurrency proofs of InTx (#648) on real PostgreSQL. Each transaction runs
// on its own goroutine and pool connection; they synchronize on channels, and
// the one wait on server state polls pg_locks. Nothing sleeps for a fixed
// time.

// await answers the first of ch and a finished transaction, failing the test
// if the transaction finished first or neither came.
func await(t *testing.T, ch <-chan struct{}, finished <-chan error, what string) {
	t.Helper()
	select {
	case <-ch:
	case err := <-finished:
		t.Fatalf("the transaction finished before %s: %v", what, err)
	case <-time.After(20 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestTxWritesBecomeVisibleTogetherAtCommit(t *testing.T) {
	env := setupTxEnv(t)
	a := env.mustCreate(t, txSchemaA, txActivity("old"))
	b := env.mustCreate(t, txSchemaB, txVisit("scheduled"))

	written, release := make(chan struct{}), make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	defer releaseOnce()
	finished := make(chan error, 1)
	go func() {
		finished <- env.inTx(func(ctx context.Context, tx forma.Tx) error {
			if _, err := tx.Update(ctx, txUpdateOp(txSchemaA, a, map[string]any{"summary": "new"})); err != nil {
				return err
			}
			if _, err := tx.Update(ctx, txUpdateOp(txSchemaB, b, map[string]any{"status": "visited"})); err != nil {
				return err
			}
			close(written)
			<-release
			return nil
		})
	}()

	await(t, written, finished, "both writes")
	assert.Equal(t, "old", env.attrs(t, txSchemaA, a)["summary"], "another connection sees no uncommitted write")
	assert.Equal(t, "scheduled", env.attrs(t, txSchemaB, b)["status"])

	releaseOnce()
	require.NoError(t, <-finished)
	assert.Equal(t, "new", env.attrs(t, txSchemaA, a)["summary"], "after commit both writes are visible")
	assert.Equal(t, "visited", env.attrs(t, txSchemaB, b)["status"])
}

func TestTxSecondWriterWaitsAndMergesOntoTheFirst_PostgresOnly(t *testing.T) {
	env := setupTxEnv(t)
	row := env.mustCreate(t, txSchemaA, txActivity("original"))

	held, release := make(chan struct{}), make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	defer releaseOnce()
	first := make(chan error, 1)
	go func() {
		first <- env.inTx(func(ctx context.Context, tx forma.Tx) error {
			if _, err := tx.Update(ctx, txUpdateOp(txSchemaA, row, map[string]any{"summary": "from tx1"})); err != nil {
				return err
			}
			close(held)
			<-release
			return nil
		})
	}()
	await(t, held, first, "tx1 to hold the row")

	backend := make(chan uint32, 1)
	second := make(chan error, 1)
	go func() {
		second <- env.inTx(func(ctx context.Context, tx forma.Tx) error {
			backend <- tx.(*txSession).tx.Conn().PgConn().PID()
			_, err := tx.Update(ctx, txUpdateOp(txSchemaA, row, map[string]any{"userId": "from tx2"}))
			return err
		})
	}()
	pid := <-backend
	require.Eventually(t, func() bool { return env.lockWaiting(pid) }, 20*time.Second, 5*time.Millisecond,
		"tx2 never waited on the row's advisory lock")
	select {
	case err := <-second:
		t.Fatalf("tx2 finished while tx1 held the row: %v", err)
	default:
	}

	releaseOnce()
	require.NoError(t, <-first)
	require.NoError(t, <-second)
	got := env.attrs(t, txSchemaA, row)
	assert.Equal(t, "from tx1", got["summary"], "tx2 merged onto tx1's commit: no lost update")
	assert.Equal(t, "from tx2", got["userId"])
}

func TestTxDeadlockAbortsExactlyOneAsRetryable_PostgresOnly(t *testing.T) {
	env := setupTxEnv(t)
	a := env.mustCreate(t, txSchemaA, txActivity("original"))
	b := env.mustCreate(t, txSchemaA, txActivity("original"))

	type outcome struct {
		label string
		err   error
	}
	holds := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	outcomes := make(chan outcome, 2)
	run := func(i int, firstRow, secondRow uuid.UUID, label string) {
		err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
			if _, err := tx.Update(ctx, txUpdateOp(txSchemaA, firstRow, map[string]any{"summary": label})); err != nil {
				return err
			}
			close(holds[i])
			// The barrier: request the second lock only once the other
			// transaction holds its first.
			select {
			case <-holds[1-i]:
			case <-ctx.Done():
				return ctx.Err()
			}
			_, err := tx.Update(ctx, txUpdateOp(txSchemaA, secondRow, map[string]any{"summary": label}))
			return err
		})
		outcomes <- outcome{label: label, err: err}
	}
	go run(0, a, b, "tx1")
	go run(1, b, a, "tx2")

	results := []outcome{<-outcomes, <-outcomes}
	var winner string
	retryable := 0
	for _, r := range results {
		if r.err == nil {
			winner = r.label
			continue
		}
		require.True(t, forma.IsRetryable(r.err), "%s failed with a non-retryable error: %v", r.label, r.err)
		require.ErrorIs(t, r.err, forma.ErrRetryableConflict)
		retryable++
	}
	require.Equal(t, 1, retryable, "exactly one transaction is the deadlock victim")
	require.NotEmpty(t, winner, "the other transaction commits")
	assert.Equal(t, winner, env.attrs(t, txSchemaA, a)["summary"], "the final state is the committed transaction's alone")
	assert.Equal(t, winner, env.attrs(t, txSchemaA, b)["summary"])
}

func TestInTxReturnsItsConnectionOnEveryExitPath(t *testing.T) {
	env := setupTxEnv(t)
	pool := env.postgresPool
	baseline := pool.Stat().AcquiredConns()
	write := func(ctx context.Context, tx forma.Tx) {
		_, err := tx.Create(ctx, txCreateOp(txSchemaA, txActivity("never committed")))
		assert.NoError(t, err)
	}
	exits := []func(){
		func() {
			_ = env.inTx(func(ctx context.Context, tx forma.Tx) error {
				write(ctx, tx)
				return errors.New("bail out")
			})
		},
		func() {
			defer func() { _ = recover() }()
			_ = env.inTx(func(ctx context.Context, tx forma.Tx) error {
				write(ctx, tx)
				panic("bail out")
			})
		},
		func() {
			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = env.inTx(func(ctx context.Context, tx forma.Tx) error {
					write(ctx, tx)
					runtime.Goexit()
					return nil
				})
			}()
			<-done
		},
		func() {
			ctx, cancel := context.WithCancel(env.ctx)
			defer cancel()
			err := env.txr.InTx(ctx, func(ctx context.Context, tx forma.Tx) error {
				write(ctx, tx)
				cancel()
				return nil
			})
			assert.ErrorIs(t, err, context.Canceled)
		},
	}
	for i := range 100 {
		exits[i%len(exits)]()
	}
	require.Eventually(t, func() bool { return pool.Stat().AcquiredConns() == baseline },
		10*time.Second, 5*time.Millisecond, "every InTx returned its connection")
	assert.Zero(t, env.schemaRowCount(t, txSchemaA), "none of the 100 transactions committed")
}

// A caller bug: fn starts an operation on another goroutine and returns while
// it is still waiting on a lock. InTx must not commit under the running
// statement, nor hang until the lock is released: it poisons the Tx, cancels
// the statement, waits for the call, and rolls back.
func TestInTxCancelsACallStillRunningWhenFnReturns_PostgresOnly(t *testing.T) {
	env := setupTxEnv(t)
	row := env.mustCreate(t, txSchemaA, txActivity("original"))
	other := env.mustCreate(t, txSchemaB, txVisit("scheduled"))

	held, release := make(chan struct{}), make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	defer releaseOnce()
	holder := make(chan error, 1)
	go func() {
		holder <- env.inTx(func(ctx context.Context, tx forma.Tx) error {
			if _, err := tx.Update(ctx, txUpdateOp(txSchemaA, row, map[string]any{"summary": "holder"})); err != nil {
				return err
			}
			close(held)
			<-release
			return nil
		})
	}()
	await(t, held, holder, "the holder to lock the row")

	stray := make(chan error, 1)
	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		if _, err := tx.Update(ctx, txUpdateOp(txSchemaB, other, map[string]any{"status": "visited"})); err != nil {
			return err
		}
		pid := tx.(*txSession).tx.Conn().PgConn().PID()
		go func() {
			_, err := tx.Update(ctx, txUpdateOp(txSchemaA, row, map[string]any{"summary": "stray"}))
			stray <- err
		}()
		require.Eventually(t, func() bool { return env.lockWaiting(pid) }, 20*time.Second, 5*time.Millisecond,
			"the stray call never waited on the row's lock")
		return nil // returns with the stray call still waiting
	})
	require.ErrorIs(t, err, forma.ErrTxAborted)
	require.ErrorContains(t, err, "still running")
	require.Error(t, <-stray, "the stray call was cancelled")
	assert.Equal(t, "scheduled", env.attrs(t, txSchemaB, other)["status"], "nothing the Tx wrote committed")

	releaseOnce()
	require.NoError(t, <-holder)
	assert.Equal(t, "holder", env.attrs(t, txSchemaA, row)["summary"])
}

// lockWaiting reports whether backend pid waits on an advisory lock.
func (e *txEnv) lockWaiting(pid uint32) bool {
	var waiting bool
	err := e.postgresPool.QueryRow(e.ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND locktype = 'advisory' AND NOT granted)",
		int32(pid)).Scan(&waiting)
	return err == nil && waiting
}
