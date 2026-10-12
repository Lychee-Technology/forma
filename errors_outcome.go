package forma

import (
	"errors"
	"fmt"
)

// Storage outcomes of a write (#645). A caller that coordinates through Forma
// must react to four failure outcomes differently, and before these sentinels
// the only way to tell them apart was to inspect driver errors that the public
// API does not expose and that differ between PostgreSQL and Aurora DSQL:
//
//   - ErrUniqueConflict: stop, the identity already exists.
//   - ErrPreconditionFailed: stop or re-read, another writer changed the row.
//   - ErrRetryableConflict: the database rolled the transaction back, so
//     running the whole transaction again is safe.
//   - ErrCommitOutcomeUnknown: the transaction may or may not have committed,
//     so a blind retry is unsafe unless the operation is idempotent.
//
// Forma never retries on the caller's behalf. Retry only when IsRetryable
// reports true; never retry blindly on ErrCommitOutcomeUnknown. See
// docs/error-handling.md, "Storage outcomes on the write path".

// ErrUniqueConflict marks a write that would give two live rows the same
// identity: a RowID or a declared unique key. errors.Is(err, ErrConflict) is
// also true, so callers that predate it keep matching. Build it with
// UniqueConflictf.
var ErrUniqueConflict error = &conflictSentinel{text: "unique conflict"}

// ErrPreconditionFailed marks a mutation whose precondition did not hold
// against the current row: another writer changed it first. errors.Is(err,
// ErrConflict) is also true. Build it with PreconditionFailedf.
var ErrPreconditionFailed error = &conflictSentinel{text: "precondition failed"}

// ErrRetryableConflict marks a transaction the database aborted because of
// concurrent activity: a serialization failure (SQLSTATE 40001, which is how
// Aurora DSQL reports an optimistic-concurrency conflict), a deadlock (40P01)
// or a lock that was not available in time (55P03). The transaction is known
// to be rolled back, so re-running the whole transaction may succeed.
//
// It deliberately does not satisfy ErrConflict: it means "run the transaction
// again", not "the request conflicts with stored state". Forma builds it
// itself, keeping the driver error in the chain for operators, and never
// publishes the driver's text.
var ErrRetryableConflict = errors.New("retryable conflict")

// ErrCommitOutcomeUnknown marks a COMMIT that was sent and never answered:
// the connection was lost, or the context ended while the reply was in
// flight. The transaction may or may not have committed. It is not
// retryable without idempotency, does not satisfy ErrConflict, and takes
// precedence over context.DeadlineExceeded or context.Canceled in the same
// chain: knowing the write may have landed matters more than why the wait
// ended.
var ErrCommitOutcomeUnknown = errors.New("commit outcome unknown")

// conflictSentinel is a species of ErrConflict. Its Is method, rather than a
// package-level fmt.Errorf("%w", ErrConflict), is what makes errors.Is(err,
// ErrConflict) hold: a bare sentinel wrap is exactly what
// TestNoBareSentinelWraps exists to catch.
type conflictSentinel struct{ text string }

func (e *conflictSentinel) Error() string { return e.text }

// Is reports the species relation. errors.Is compares the receiver itself
// first, so each species still matches only itself among the four outcomes.
func (e *conflictSentinel) Is(target error) bool { return target == ErrConflict }

// UniqueConflictf builds a client error classified by ErrUniqueConflict
// (and therefore ErrConflict) whose formatted message is published verbatim
// (#313). Error() renders "<message>: unique conflict". The message must not
// name a physical column or constraint; attach those with
// WithOperatorDetail.
func UniqueConflictf(format string, args ...any) error {
	return &clientError{sentinel: ErrUniqueConflict, public: fmt.Sprintf(format, args...)}
}

// PreconditionFailedf builds a client error classified by
// ErrPreconditionFailed (and therefore ErrConflict). Error() renders
// "<message>: precondition failed".
func PreconditionFailedf(format string, args ...any) error {
	return &clientError{sentinel: ErrPreconditionFailed, public: fmt.Sprintf(format, args...)}
}

// IsRetryable reports whether err says the database rolled the transaction
// back because of concurrent activity, so that running the whole transaction
// again is safe. It is false for every other error, and false whenever the
// chain also carries ErrCommitOutcomeUnknown: an outcome that may have
// committed is never a safe retry.
func IsRetryable(err error) bool {
	return errors.Is(err, ErrRetryableConflict) && !errors.Is(err, ErrCommitOutcomeUnknown)
}
