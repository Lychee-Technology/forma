package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pgError builds the server error pgx returns for an ErrorResponse. The
// severity fields default to ERROR, which is what every statement-level abort
// carries.
func pgError(code, message string) *pgconn.PgError {
	return &pgconn.PgError{Severity: "ERROR", SeverityUnlocalized: "ERROR", Code: code, Message: message}
}

// bothShapes runs check on err bare and behind two plain wraps: the
// classifier runs once at the transaction boundary over whatever chain the
// write path built, so the wrap depth must not matter.
func bothShapes(t *testing.T, err error, check func(t *testing.T, err error)) {
	t.Helper()
	t.Run("bare", func(t *testing.T) { check(t, err) })
	t.Run("wrapped", func(t *testing.T) {
		check(t, fmt.Errorf("insert main row for r: %w", fmt.Errorf("insert entity_main: %w", err)))
	})
}

func TestClassifyStatementErrorMapsUniqueViolation(t *testing.T) {
	detail := "Key (ltbase_schema_id, ltbase_row_id)=(7, abc) already exists."
	driver := &pgconn.PgError{Severity: "ERROR", Code: "23505", Detail: detail,
		Message: `duplicate key value violates unique constraint "entity_main_pkey"`, ConstraintName: "entity_main_pkey",
		ColumnName: "ltbase_row_id", TableName: "entity_main"}
	bothShapes(t, driver, func(t *testing.T, input error) {
		err := classifyStatementError(input)
		require.ErrorIs(t, err, forma.ErrUniqueConflict)
		require.ErrorIs(t, err, forma.ErrConflict, "pre-#645 callers match ErrConflict")
		assert.False(t, forma.IsRetryable(err))

		msg, ok := forma.ResolvePublicMessage(err)
		require.True(t, ok, "a unique conflict publishes: %v", err)
		assert.Equal(t, "the write conflicts with a row that already exists", msg, "the published message is unchanged")
		for _, physical := range []string{"entity_main_pkey", "ltbase_row_id", "entity_main", "Key ("} {
			assert.NotContains(t, msg, physical, "#313: no physical detail in the published message")
		}
		assert.Contains(t, err.Error(), detail, "the operator copy keeps the driver detail")
		assert.True(t, forma.HasOperatorDetail(err))
		var kept *pgconn.PgError
		require.ErrorAs(t, err, &kept, "the driver error stays in the chain")
	})
}

func TestClassifyStatementErrorMapsRetryableCodes(t *testing.T) {
	tests := []struct {
		name string
		err  *pgconn.PgError
	}{
		{"serialization failure", pgError("40001", "could not serialize access due to concurrent update")},
		// Aurora DSQL puts its OCC code in the message; the SQLSTATE alone
		// decides, so the message is never read.
		{"serialization failure with a DSQL OC000 message", pgError("40001", "change conflicts with another transaction, please retry: (OC000)")},
		{"serialization failure with a DSQL OC001 message", pgError("40001", "schema has been updated by another transaction, please retry: (OC001)")},
		{"deadlock detected", pgError("40P01", "deadlock detected")},
		{"lock not available", pgError("55P03", "canceling statement due to lock timeout")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bothShapes(t, tt.err, func(t *testing.T, input error) {
				err := classifyStatementError(input)
				require.ErrorIs(t, err, forma.ErrRetryableConflict)
				assert.True(t, forma.IsRetryable(err))
				assert.NotErrorIs(t, err, forma.ErrConflict, "a retryable conflict is not a request conflict")
				assert.NotErrorIs(t, err, forma.ErrCommitOutcomeUnknown)
				_, published := forma.ResolvePublicMessage(err)
				assert.False(t, published, "driver text never publishes")
				assert.Contains(t, err.Error(), "SQLSTATE "+tt.err.Code, "the operator copy keeps the driver error")
			})
		})
	}
}

func TestClassifyStatementErrorLeavesEverythingElseUnchanged(t *testing.T) {
	already := classifyStatementError(pgError("40P01", "deadlock detected"))
	tests := []struct {
		name string
		err  error
	}{
		{"foreign key violation", pgError("23503", "violates foreign key constraint")},
		{"undefined table", pgError("42P01", `relation "entity_main" does not exist`)},
		{"plain error", errors.New("boom")},
		{"lost connection", io.ErrUnexpectedEOF},
		{"already classified", already},
		{"already classified behind a wrap", fmt.Errorf("outer: %w", already)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Same(t, tt.err, classifyStatementError(tt.err))
		})
	}
	assert.NoError(t, classifyStatementError(nil))
}

// safeToRetryError stands in for pgconn's unexported errors that prove no
// byte was sent: a context already done before the send, a connection
// already known to be closed, a write that wrote nothing.
type safeToRetryError struct{ safe bool }

func (e safeToRetryError) Error() string     { return "write failed" }
func (e safeToRetryError) SafeToRetry() bool { return e.safe }

// timeoutError mirrors pgconn's errTimeout: the shape a deadline that expired
// after the send takes, still matching context.DeadlineExceeded.
type timeoutError struct{ err error }

func (e timeoutError) Error() string     { return "timeout: " + e.err.Error() }
func (e timeoutError) Unwrap() error     { return e.err }
func (e timeoutError) SafeToRetry() bool { return pgconn.SafeToRetry(e.err) }

func TestClassifyCommitErrorSeparatesNotCommittedFromUnknown(t *testing.T) {
	type outcome int
	const (
		notCommitted outcome = iota
		retryable
		unique
		unknown
	)
	fatal := &pgconn.PgError{Severity: "FATAL", SeverityUnlocalized: "FATAL", Code: "57P01",
		Message: "terminating connection due to administrator command"}
	tests := []struct {
		name string
		err  error
		want outcome
	}{
		{"server rejected COMMIT with 40001", pgError("40001", "could not serialize access"), retryable},
		{"server rejected COMMIT with a DSQL OC000 conflict", pgError("40001", "change conflicts with another transaction (OC000)"), retryable},
		{"deferred unique violation at COMMIT", pgError("23505", "duplicate key value"), unique},
		{"other ERROR-severity rejection", pgError("23503", "violates foreign key constraint"), notCommitted},
		{"session ended with FATAL", fatal, unknown},
		{"pooler lost the backend after COMMIT", &pgconn.PgError{Severity: "FATAL", Code: "08P01", Message: "server conn crashed?"}, unknown},
		{"PANIC", &pgconn.PgError{Severity: "PANIC", SeverityUnlocalized: "PANIC", Code: "XX000"}, unknown},
		// A session that ended proves nothing about the commit, whatever the
		// code: retryable must mean rolled back. PostgreSQL itself raises
		// FATAL 40001 to end a session over a recovery conflict.
		{"FATAL with a retryable code", &pgconn.PgError{Severity: "FATAL", SeverityUnlocalized: "FATAL", Code: "40001",
			Message: "terminating connection due to conflict with recovery"}, unknown},
		{"FATAL with a unique-violation code", &pgconn.PgError{Severity: "FATAL", SeverityUnlocalized: "FATAL", Code: "23505"}, unknown},
		{"server answered ROLLBACK", pgx.ErrTxCommitRollback, notCommitted},
		{"provably not sent", safeToRetryError{safe: true}, notCommitted},
		{"context done before send", timeoutError{err: safeToRetryError{safe: true}}, notCommitted},
		{"write failed after some bytes", safeToRetryError{safe: false}, unknown},
		{"connection lost after send", &net.OpError{Op: "read", Net: "tcp", Err: io.ErrUnexpectedEOF}, unknown},
		{"bare EOF after send", io.ErrUnexpectedEOF, unknown},
		{"context cancelled after send", context.Canceled, unknown},
		{"deadline expired after send", timeoutError{err: context.DeadlineExceeded}, unknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bothShapes(t, tt.err, func(t *testing.T, input error) {
				err := classifyCommitError(input)
				require.Error(t, err)
				require.ErrorIs(t, err, input, "the driver error stays in the chain")
				assert.Equal(t, tt.want == unknown, errors.Is(err, forma.ErrCommitOutcomeUnknown), "unknown")
				assert.Equal(t, tt.want == retryable, errors.Is(err, forma.ErrRetryableConflict), "retryable")
				assert.Equal(t, tt.want == retryable, forma.IsRetryable(err), "IsRetryable")
				assert.Equal(t, tt.want == unique, errors.Is(err, forma.ErrUniqueConflict), "unique")
				assert.Equal(t, tt.want == unique, errors.Is(err, forma.ErrConflict), "an unknown or not-committed commit is no conflict")
				if tt.want == notCommitted {
					assert.Contains(t, err.Error(), "transaction not committed")
				}
			})
		})
	}
	assert.NoError(t, classifyCommitError(nil))
}

// TestClassifyCommitErrorKeepsTheContextCause pins the half of #645's
// Given/When/Then 5 that callers lean on: an unknown commit still says why
// the wait ended, but the outcome is what classifies it.
func TestClassifyCommitErrorKeepsTheContextCause(t *testing.T) {
	err := classifyCommitError(timeoutError{err: context.DeadlineExceeded})
	require.ErrorIs(t, err, forma.ErrCommitOutcomeUnknown)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.False(t, forma.IsRetryable(err))
}
