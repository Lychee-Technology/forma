package coordinationtest

import (
	"errors"
	"fmt"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// InTx's contract: one call of Transactor.InTx, no retry, and the error as
// Forma answered it.
func TestInTxRunsFnOnceAndReturnsItsErrorUnchanged(t *testing.T) {
	e := newEnv(t)
	retryable := fmt.Errorf("the caller's attempt lost a race: %w", forma.ErrRetryableConflict)
	calls := 0
	err := e.inTx(func(UnitOfWork) error {
		calls++
		return retryable
	})
	require.Equal(t, 1, calls, "InTx never retries, even an ErrRetryableConflict")
	require.ErrorIs(t, err, forma.ErrRetryableConflict)
	require.ErrorIs(t, err, retryable, "the error reaches the caller as fn returned it")

	plain := errors.New("the caller's own failure")
	calls = 0
	err = e.inTx(func(UnitOfWork) error {
		calls++
		return plain
	})
	require.Equal(t, 1, calls)
	require.ErrorIs(t, err, plain)
}

// The counting engine is the manager's: a federated read reaches it. Every
// test's cleanup asserts the count is zero, so this control proves the
// assertion can fail.
func TestCountingEngineSeesAFederatedRead(t *testing.T) {
	e := newEnv(t)
	_, _ = e.em.Query(e.ctx, &forma.QueryRequest{SchemaName: InstanceSchema, Page: 1, ItemsPerPage: 1,
		Condition: equals("status", "running"),
		Federated: &forma.FederatedQueryRequest{Enabled: true, AllowPartialDegradedMode: true}})
	require.EqualValues(t, 1, e.engine.calls.Load(), "a federated read goes through the counting engine")
	e.engine.calls.Store(0)
}
