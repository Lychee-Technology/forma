package internal

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lychee-technology/forma"
)

// The SQLSTATEs the write paths classify (#645). Classification reads the
// code, never the message text (#307, #312). Aurora DSQL reports an
// optimistic-concurrency conflict as 40001 with an OC000/OC001 code in its
// message; the SQLSTATE alone decides, so the OC code is never parsed.
const (
	sqlstateUniqueViolation      = "23505"
	sqlstateSerializationFailure = "40001"
	sqlstateDeadlockDetected     = "40P01"
	sqlstateLockNotAvailable     = "55P03"
)

// rowIdentityConflictMessage is what a 23505 publishes. pgErr.Detail names
// physical columns and constraint layout ("Key (ltbase_schema_id,
// ltbase_row_id)=(…) already exists"), which must not cross a public
// transport (#313); the whole driver error, Detail included, stays in the
// chain as operator detail.
const rowIdentityConflictMessage = "the write conflicts with a row that already exists"

// storageOutcome carries one of the two outcomes Forma builds itself,
// forma.ErrRetryableConflict or forma.ErrCommitOutcomeUnknown, with the
// driver error that proved it. Neither publishes a message, so every public
// transport redacts it, while Error() and the log keep the driver text. It is
// a carrier rather than forma.WithOperatorDetail because that decorator only
// attaches to an error that publishes a client message.
type storageOutcome struct {
	sentinel error
	cause    error
}

func (e *storageOutcome) Error() string   { return e.sentinel.Error() + ": " + e.cause.Error() }
func (e *storageOutcome) Unwrap() []error { return []error{e.sentinel, e.cause} }

// classifyStatementError maps the error of a statement that ran inside a
// write transaction to its storage outcome:
//
//   - 23505 is forma.ErrUniqueConflict, published as a curated summary;
//   - 40001, 40P01 and 55P03 are forma.ErrRetryableConflict: the server
//     aborted the statement, the transaction cannot commit, and inWriteTx
//     rolls it back;
//   - anything else, including an error that already carries an outcome, is
//     returned unchanged.
//
// It looks through the whole chain, so it can run once at the transaction
// boundary over whatever the body returned.
func classifyStatementError(err error) error {
	if err == nil || carriesStorageOutcome(err) {
		return err
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err
	}
	if classified, ok := classifySQLState(err, pgErr); ok {
		return classified
	}
	return err
}

// classifyCommitError maps the error tx.Commit returned. Only a failure that
// proves the transaction did not commit may be reported as anything other
// than forma.ErrCommitOutcomeUnknown:
//
//   - a FATAL or PANIC server error means the session ended, and a pooler
//     reports a backend lost after the COMMIT went out exactly that way, so
//     it is unknown whatever its code. Retryable must mean rolled back, and
//     an ended session proves nothing about the commit;
//   - a server error of ERROR severity means the server rejected the COMMIT
//     and aborted the transaction, so it is classified by SQLSTATE like a
//     statement (a 40001 at COMMIT is retryable, never unknown), and any
//     other code is a known not-committed error;
//   - pgx.ErrTxCommitRollback (the server answered ROLLBACK) and an error
//     pgconn.SafeToRetry proves was raised before any byte was sent are
//     known not-committed errors;
//   - anything else, a lost connection or a context that ended while the
//     reply was in flight, is unknown. The chain may still hold
//     context.Canceled or context.DeadlineExceeded; the outcome wins.
func classifyCommitError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if sessionEnded(pgErr) {
			return &storageOutcome{sentinel: forma.ErrCommitOutcomeUnknown, cause: err}
		}
		if classified, ok := classifySQLState(err, pgErr); ok {
			return classified
		}
		return fmt.Errorf("transaction not committed: %w", err)
	}
	if errors.Is(err, pgx.ErrTxCommitRollback) || pgconn.SafeToRetry(err) {
		return fmt.Errorf("transaction not committed: %w", err)
	}
	return &storageOutcome{sentinel: forma.ErrCommitOutcomeUnknown, cause: err}
}

// classifySQLState is the code table both classifiers share. ok is false for
// a code it does not classify.
func classifySQLState(err error, pgErr *pgconn.PgError) (error, bool) {
	switch pgErr.Code {
	case sqlstateUniqueViolation:
		// Detail is prefixed into the operator text explicitly: PgError.Error()
		// renders severity/message/SQLSTATE only, so without this the
		// offending key would vanish from the log line too.
		return forma.WithOperatorDetail(
			forma.UniqueConflictf(rowIdentityConflictMessage),
			fmt.Errorf("%s: %w", pgErr.Detail, err)), true
	case sqlstateSerializationFailure, sqlstateDeadlockDetected, sqlstateLockNotAvailable:
		return &storageOutcome{sentinel: forma.ErrRetryableConflict, cause: err}, true
	}
	return nil, false
}

// sessionEnded reports whether a server error ended the session rather than
// only the statement. It reads the unlocalized severity field, a protocol
// enumeration rather than message text, and falls back to the localized one
// for a server that predates the field (PostgreSQL < 9.6).
func sessionEnded(pgErr *pgconn.PgError) bool {
	severity := pgErr.SeverityUnlocalized
	if severity == "" {
		severity = pgErr.Severity
	}
	return severity == "FATAL" || severity == "PANIC"
}

// carriesStorageOutcome reports whether err was already classified, which
// keeps the classifiers idempotent.
func carriesStorageOutcome(err error) bool {
	return errors.Is(err, forma.ErrUniqueConflict) ||
		errors.Is(err, forma.ErrPreconditionFailed) ||
		errors.Is(err, forma.ErrRetryableConflict) ||
		errors.Is(err, forma.ErrCommitOutcomeUnknown)
}
