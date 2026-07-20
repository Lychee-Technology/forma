package cdc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
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
