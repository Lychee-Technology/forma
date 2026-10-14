package internal

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/lychee-technology/forma/internal/txseal"

	"github.com/lychee-technology/forma"
)

// txSessionKey marks the context InTx hands to fn, which is how a nested
// InTx is detected.
type txSessionKey struct{}

// txSession is Forma's forma.Tx: one InTx call's handle on its transaction
// (#648). Its methods run the manager's CRUD and query services bound to the
// session's transaction, so they validate, lock, merge, stamp and read
// exactly as EntityManager does; the session adds the transaction's own rules
// around each call: the done and poisoned states, the operation limit, and
// concurrent-use detection.
type txSession struct {
	txseal.Seal

	// ctx is InTx's context, deadline included, and stop cancels it. Every
	// operation is bound to it (see bind).
	ctx  context.Context
	stop context.CancelFunc
	// crud and query are the manager's services running on tx, and rows
	// the repository they run on.
	crud   *entityCRUDService
	query  *entityQueryService
	rows   *txBoundRepository
	tx     *meteredTx
	meter  *txMeter
	maxOps int
	ops    int

	// busy is held for the length of one method call; a second goroutine
	// calling in at the same time fails its TryLock, which is how concurrent
	// use is detected.
	busy sync.Mutex
	// state guards done and cause. It is separate from busy so that the
	// detected concurrent caller can poison the session while the first call
	// is still running.
	state sync.Mutex
	done  bool
	cause error
}

var _ forma.Tx = (*txSession)(nil)

func (s *txSession) Create(ctx context.Context, op *forma.EntityOperation) (*forma.DataRecord, error) {
	var record *forma.DataRecord
	err := s.do(ctx, "Create", func(ctx context.Context) (err error) {
		record, err = s.crud.Create(ctx, op)
		return err
	})
	return record, err
}

func (s *txSession) Update(ctx context.Context, op *forma.EntityOperation) (*forma.DataRecord, error) {
	var record *forma.DataRecord
	err := s.do(ctx, "Update", func(ctx context.Context) (err error) {
		record, err = s.crud.Update(ctx, op)
		return err
	})
	return record, err
}

func (s *txSession) Delete(ctx context.Context, op *forma.EntityOperation) error {
	return s.do(ctx, "Delete", func(ctx context.Context) error {
		return s.crud.Delete(ctx, op)
	})
}

// do runs one operation under the session's rules. An operation that does
// not return (a panic or runtime.Goexit inside it) poisons the session
// without recovering: whatever it wrote stays in the transaction, and fn
// might recover the panic and return nil.
func (s *txSession) do(ctx context.Context, method string, op func(context.Context) error) error {
	if !s.busy.TryLock() {
		return s.refuseConcurrent(method)
	}
	defer s.busy.Unlock()
	if err := s.usable(method); err != nil {
		return err
	}
	if ctx == nil {
		return forma.InvalidInputf("%s needs a non-nil context", method)
	}
	s.ops++
	if s.ops > s.maxOps {
		err := forma.InvalidInputf(
			"operation %d would exceed the transaction's limit of %d operations; "+
				"split the work across transactions or raise the limit with WithTxMaxOperations",
			s.ops, s.maxOps)
		s.poison(err)
		return err
	}

	opCtx, release := s.bind(ctx)
	defer release()
	sent, failed := s.meter.statements, s.meter.failures
	returned := false
	defer func() {
		if !returned {
			s.poison(fmt.Errorf("%s did not return: it panicked or its goroutine exited", method))
		}
	}()
	err := op(opCtx)
	returned = true
	if err != nil && poisons(err, s.meter.statements > sent, s.meter.failures > failed) {
		s.poison(err)
	}
	return err
}

// poisons reports whether an operation's error poisons the transaction.
// sent says the operation sent SQL, failed that one of its statements
// failed. The expected outcomes are the ones that leave nothing half-written:
// a lookup that found no row, a precondition or unique check answered
// without a failed statement, and invalid input refused before any SQL.
// A failed statement always poisons: PostgreSQL has aborted the transaction,
// or the connection is gone. An ErrInvalidInput raised after SQL was sent
// poisons too, because the operation may have written part of its rows.
func poisons(err error, sent, failed bool) bool {
	if failed {
		return true
	}
	switch {
	case errors.Is(err, forma.ErrNotFound),
		errors.Is(err, forma.ErrPreconditionFailed),
		errors.Is(err, forma.ErrUniqueConflict):
		return false
	case errors.Is(err, forma.ErrInvalidInput):
		return sent
	}
	return true
}

// bind returns ctx, cancelled as well when the session's context ends, so
// the InTx deadline bounds every statement even when the caller passes a
// context that does not derive from the one fn received.
func (s *txSession) bind(ctx context.Context) (context.Context, func()) {
	bound, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(s.ctx, func() { cancel(context.Cause(s.ctx)) })
	return bound, func() {
		stop()
		cancel(nil)
	}
}

// usable answers the error a call gets on a finished or poisoned session.
func (s *txSession) usable(method string) error {
	s.state.Lock()
	defer s.state.Unlock()
	if s.done {
		return fmt.Errorf("%w: %s called after InTx returned", forma.ErrTxDone, method)
	}
	if s.cause != nil {
		return s.abortError()
	}
	return nil
}

// refuseConcurrent answers a call that found another call running: a caller
// bug, which poisons the session. The running call completes normally.
func (s *txSession) refuseConcurrent(method string) error {
	s.state.Lock()
	defer s.state.Unlock()
	if s.done {
		return fmt.Errorf("%w: %s called after InTx returned", forma.ErrTxDone, method)
	}
	err := forma.InvalidInputf("%s was called while another call on the same transaction was running; "+
		"a Tx must not be used from two goroutines at once", method)
	if s.cause == nil {
		s.cause = err
	}
	return err
}

// poison records err as the session's poisoning cause unless an earlier
// error already is.
func (s *txSession) poison(err error) {
	s.state.Lock()
	defer s.state.Unlock()
	if s.cause == nil {
		s.cause = err
	}
}

// abortError is what every call answers once the session is poisoned. The
// caller holds state.
func (s *txSession) abortError() error {
	return fmt.Errorf("%w by an earlier error: %w", forma.ErrTxAborted, s.cause)
}

// finish ends the session and answers its poisoning cause, if any. A call
// still running on another goroutine when fn returned is a caller bug: it
// poisons the session, its statements are cancelled, and finish waits for it
// so that InTx never settles the transaction under a running statement.
func (s *txSession) finish() error {
	if !s.busy.TryLock() {
		s.poison(errors.New("fn returned while a call on its transaction was still running"))
		s.stop()
		s.busy.Lock()
	}
	defer s.busy.Unlock()
	s.state.Lock()
	defer s.state.Unlock()
	s.done = true
	return s.cause
}
