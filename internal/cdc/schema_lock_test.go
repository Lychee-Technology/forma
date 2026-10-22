package cdc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

// TestErrSchemaLockContendedIdentity pins the sentinel's errors.Is identity so
// callers (cdc-init) can distinguish lock contention from other failures.
func TestErrSchemaLockContendedIdentity(t *testing.T) {
	wrapped := fmt.Errorf("schema 7 skipped: %w", ErrSchemaLockContended)
	require.ErrorIs(t, wrapped, ErrSchemaLockContended)
	require.False(t, errors.Is(errors.New("other"), ErrSchemaLockContended))
}

// TestTrySchemaLockConnFailureWraps verifies the db.Conn(ctx) failure path
// returns a wrapped error and a nil unlock. Real advisory-lock behaviour is
// covered by reconcile/production e2e against Postgres.
func TestTrySchemaLockConnFailureWraps(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	require.NoError(t, db.Close()) // closed pool: Conn(ctx) must fail

	locked, unlock, err := TrySchemaLock(context.Background(), db, 7)
	require.Error(t, err)
	require.Contains(t, err.Error(), "pin connection for schema 7 advisory lock")
	require.False(t, locked)
	require.Nil(t, unlock)
}

func TestNewSchemaLockerFollowsTheEngine(t *testing.T) {
	db := &sql.DB{}
	for _, engine := range []forma.DatabaseEngine{"", forma.EnginePostgreSQL} {
		locker, err := NewSchemaLocker(db, engine, "entity_main")
		require.NoError(t, err)
		require.IsType(t, advisorySchemaLocker{}, locker, "engine %q", engine)
	}

	locker, err := NewSchemaLocker(db, forma.EngineAuroraDSQL, "tenant.entity_main")
	require.NoError(t, err)
	lease, ok := locker.(*leaseSchemaLocker)
	require.True(t, ok, "aurora-dsql must lock with a lease, got %T", locker)
	require.Equal(t, `"tenant"."entity_main_schema_lease"`, lease.table)
	require.Equal(t, defaultLeaseTTL, lease.ttl)
	require.Greater(t, lease.validFor(), lease.renewEvery, "a holder must survive a missed renewal")

	locker, err = NewSchemaLocker(db, forma.EngineAuroraDSQL, "")
	require.NoError(t, err)
	require.Equal(t, `"entity_main_schema_lease"`, locker.(*leaseSchemaLocker).table)

	_, err = NewSchemaLocker(db, "mysql", "entity_main")
	var cfgErr *forma.ConfigError
	require.ErrorAs(t, err, &cfgErr)
	require.Contains(t, err.Error(), `"mysql"`)
}

func TestWithLockLossNamesALostLock(t *testing.T) {
	workErr := fmt.Errorf("export batch: %w", context.Canceled)
	require.NoError(t, WithLockLoss(context.Background(), nil))

	released, cancel := context.WithCancel(context.Background())
	cancel()
	require.Same(t, workErr, WithLockLoss(released, workErr), "a release is not a loss")

	lost, cancelCause := context.WithCancelCause(context.Background())
	cancelCause(fmt.Errorf("%w: schema 7's lease is held by another holder or gone", ErrSchemaLockLost))
	err := WithLockLoss(lost, workErr)
	require.ErrorIs(t, err, ErrSchemaLockLost)
	require.ErrorIs(t, err, context.Canceled, "the work's own error must stay matchable")
	require.Contains(t, err.Error(), "schema 7's lease")

	already := fmt.Errorf("flush: %w", context.Cause(lost))
	require.Same(t, already, WithLockLoss(lost, already), "an error naming the loss is not wrapped twice")

	require.NoError(t, WithLockLoss(released, nil), "work that finished before a release is clean")
	err = WithLockLoss(lost, nil)
	require.ErrorIs(t, err, ErrSchemaLockLost, "work that swallowed the loss's cancellation must not read as clean")
	require.Contains(t, err.Error(), "schema 7's lease")
}
