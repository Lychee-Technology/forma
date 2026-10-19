package coordinationtest

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Synchronization without fixed sleeps: a held transaction parks on a
// channel, a race starts its workers on one closed channel, and a wait for
// a blocked statement polls pg_locks for this test's sessions only.

// held is a transaction parked inside InTx until it is released.
type held struct {
	release chan struct{}
	done    chan error
}

// hold runs fn in a transaction of the adapter, waits until fn has
// returned, and parks the transaction before its commit. fn's error is
// what the transaction answers when it is released.
func (e *env) hold(t *testing.T, fn func(UnitOfWork) error) *held {
	t.Helper()
	return e.holdIn(t, func(park func(error) error) error {
		return e.inTx(func(uow UnitOfWork) error { return park(fn(uow)) })
	})
}

// holdTx is hold for a function of the Forma transaction itself, for a
// transaction no adapter store expresses, such as a lock-order round's gate.
func (e *env) holdTx(t *testing.T, fn func(context.Context, forma.Tx) error) *held {
	t.Helper()
	return e.holdIn(t, func(park func(error) error) error {
		return e.store.tr.InTx(e.ctx, func(ctx context.Context, tx forma.Tx) error { return park(fn(ctx, tx)) })
	})
}

// holdIn runs a transaction through run, whose body hands its result to
// park: park says the body has returned, and returns the result once the
// transaction is released.
func (e *env) holdIn(t *testing.T, run func(park func(error) error) error) *held {
	t.Helper()
	h := &held{release: make(chan struct{}), done: make(chan error, 1)}
	ready := make(chan struct{})
	go func() {
		h.done <- run(func(err error) error {
			close(ready)
			<-h.release
			return err
		})
	}()
	select {
	case <-ready:
	case err := <-h.done:
		t.Fatalf("the held transaction ended before it parked: %v", err)
	case <-e.ctx.Done():
		t.Fatal("the held transaction never parked")
	}
	t.Cleanup(h.finish)
	return h
}

// commit releases the held transaction and answers its outcome.
func (h *held) commit() error {
	h.finish()
	return <-h.done
}

func (h *held) finish() {
	select {
	case <-h.release:
	default:
		close(h.release)
	}
}

// race runs n workers that start together and answers each worker's error.
func race(n int, worker func(i int) error) []error {
	errs := make([]error, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = worker(i)
		}(i)
	}
	close(start)
	wg.Wait()
	return errs
}

// waitingBackends answers how many of this test's backends wait on a lock
// they have not been granted.
func (e *env) waitingBackends(t *testing.T) int {
	t.Helper()
	n, err := e.lockWaits()
	require.NoError(t, err)
	return n
}

func (e *env) lockWaits() (int, error) {
	var n int
	err := e.pool.QueryRow(e.ctx, `SELECT count(DISTINCT l.pid) FROM pg_locks l
		JOIN pg_stat_activity a ON a.pid = l.pid
		WHERE NOT l.granted AND a.application_name = $1`, e.app).Scan(&n)
	return n, err
}

// requireBlocked waits until n of this test's backends wait on a lock.
func (e *env) requireBlocked(t *testing.T, n int) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		waits, err := e.lockWaits()
		require.NoError(c, err)
		require.GreaterOrEqual(c, waits, n)
	}, 30*time.Second, 10*time.Millisecond, "%d backends wait on a lock", n)
}

// result answers a goroutine's error, or fails when it has not answered
// within the env's deadline.
func (e *env) result(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-e.ctx.Done():
		t.Fatal("the operation never answered")
		return nil
	}
}

// async runs fn in a goroutine and answers the channel of its error.
func async(fn func() error) chan error {
	done := make(chan error, 1)
	go func() { done <- fn() }()
	return done
}

// requireUnanswered fails when done has answered already.
func requireUnanswered(t *testing.T, done chan error) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("the blocked operation answered early: %v", err)
	default:
	}
}

// settle waits until the operation behind done has answered or until one
// of this test's backends waits on a lock. It answers whether the operation
// answered, and its error when it did. Lock waits are read from pg_locks
// on any backend that has it, whatever its name, so a contract assertion
// does not hang on a backend that blocks writers; on one without it, only
// the answer ends the wait.
func (e *env) settle(t *testing.T, done chan error) (answered bool, err error) {
	t.Helper()
	require.Eventually(t, func() bool {
		select {
		case err = <-done:
			answered = true
			return true
		default:
		}
		n, lockErr := e.lockWaits()
		return lockErr == nil && n > 0
	}, 30*time.Second, 10*time.Millisecond, "the operation answers, or waits on a lock")
	return answered, err
}

// outcome is how one transaction of an interleaving ended: whether it had
// answered when the interleaving's other transaction was released, and its
// error.
type outcome struct {
	early bool
	err   error
}

// collect answers the outcome of the operation behind done, which settle
// reported as answered or not.
func (e *env) collect(t *testing.T, done chan error, answered bool, err error) outcome {
	t.Helper()
	if !answered {
		err = e.result(t, done)
	}
	return outcome{early: answered, err: err}
}

// statementTracer records the SQL and arguments of every statement sent
// while it is on: a pgx tracer, so it sees exactly what Forma sends.
type statementTracer struct {
	on         atomic.Bool
	mu         sync.Mutex
	statements []tracedStatement
}

type tracedStatement struct {
	sql  string
	args []any
}

func (s *statementTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if s.on.Load() {
		s.mu.Lock()
		s.statements = append(s.statements, tracedStatement{sql: data.SQL, args: slices.Clone(data.Args)})
		s.mu.Unlock()
	}
	return ctx
}

func (s *statementTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// record runs fn with the tracer on and answers what it recorded.
func (s *statementTracer) record(fn func()) []tracedStatement {
	s.mu.Lock()
	s.statements = nil
	s.mu.Unlock()
	s.on.Store(true)
	fn()
	s.on.Store(false)
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.statements)
}
