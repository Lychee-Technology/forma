package internal

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// server_time and preconditions inside a Tx (#649 over #648) on real
// PostgreSQL.

// sessionNow answers now() as the Tx's own transaction reads it, truncated
// to the millisecond.
func sessionNow(t *testing.T, ctx context.Context, tx forma.Tx) time.Time {
	t.Helper()
	var now time.Time
	require.NoError(t, tx.(*txSession).tx.QueryRow(ctx, "SELECT now()").Scan(&now))
	return now.Truncate(time.Millisecond).UTC()
}

// awaitClockPastNow polls until the transaction's wall clock is at least a
// millisecond past its now(), so that a value read from the wall clock
// would differ from the transaction instant.
func awaitClockPastNow(t *testing.T, ctx context.Context, tx forma.Tx) {
	t.Helper()
	require.Eventually(t, func() bool {
		var advanced bool
		err := tx.(*txSession).tx.QueryRow(ctx,
			"SELECT date_trunc('milliseconds', clock_timestamp()) > date_trunc('milliseconds', now())").Scan(&advanced)
		return err == nil && advanced
	}, 5*time.Second, time.Millisecond, "the wall clock never passed now()")
}

// TestServerTimeIsTheTransactionInstant pins server_time to SELECT now() of
// the same transaction, truncated to milliseconds, plus Offset, at each
// attribute's precision.
func TestServerTimeIsTheTransactionInstant(t *testing.T) {
	e := newCondPG(t)
	rowID := e.create(t, map[string]any{"version": int64(1)})

	var now time.Time
	err := e.txr.InTx(e.ctx, func(ctx context.Context, tx forma.Tx) error {
		now = sessionNow(t, ctx, tx)
		awaitClockPastNow(t, ctx, tx)
		_, err := tx.Update(ctx, condUpdate(rowID, nil, nil,
			forma.ServerTime("due", 0),
			forma.ServerTime("due_main", -90*time.Minute),
			forma.ServerTime("day", 0),
			forma.ServerTime("due_iso", 1500*time.Millisecond)))
		return err
	})
	require.NoError(t, err)

	got := e.get(t, rowID)
	assert.Equal(t, now, got["due"], "datetime (EAV)")
	assert.Equal(t, now.Add(-90*time.Minute), got["due_main"], "datetime (unix_ms column)")
	assert.Equal(t, now.Truncate(24*time.Hour), got["day"], "date: the UTC day")
	assert.Equal(t, now.Add(1500*time.Millisecond).Truncate(time.Second), got["due_iso"], "datetime (iso8601 column): whole seconds")
}

// TestServerTimeOutsideATxReadsItsOwnTransaction brackets a plain Update's
// server_time between now() read before and after it.
func TestServerTimeOutsideATxReadsItsOwnTransaction(t *testing.T) {
	e := newCondPG(t)
	rowID := e.create(t, map[string]any{"version": int64(1)})
	poolNow := func() time.Time {
		var now time.Time
		require.NoError(t, e.pool.QueryRow(e.ctx, "SELECT now()").Scan(&now))
		return now.Truncate(time.Millisecond).UTC()
	}

	before := poolNow()
	_, err := e.em.Update(e.ctx, condUpdate(rowID, nil, nil, forma.ServerTime("due", 0), forma.ServerTime("due_main", 0)))
	require.NoError(t, err)
	after := poolNow()

	got := e.get(t, rowID)
	due, ok := got["due"].(time.Time)
	require.True(t, ok, "due is a time: %#v", got["due"])
	assert.False(t, due.Before(before) || due.After(after), "server_time %s lies between %s and %s", due, before, after)
	assert.Equal(t, due, got["due_main"], "two operators of one update share one instant")
}

// TestNowPreconditionAndServerTimeShareTheTxInstant writes server_time, lets
// the wall clock move on, and tests the written value with $now in the same
// Tx: equal, and not earlier.
func TestNowPreconditionAndServerTimeShareTheTxInstant(t *testing.T) {
	e := newCondPG(t)
	rowID := e.create(t, map[string]any{"version": int64(1)})

	err := e.txr.InTx(e.ctx, func(ctx context.Context, tx forma.Tx) error {
		if _, err := tx.Update(ctx, condUpdate(rowID, nil, nil, forma.ServerTime("due", 0), forma.ServerTime("day", 0), forma.ServerTime("due_main", time.Hour))); err != nil {
			return err
		}
		awaitClockPastNow(t, ctx, tx)
		_, err := tx.Update(ctx, condUpdate(rowID, kv("due", "lt:$now"), map[string]any{"name": "earlier"}))
		require.ErrorIs(t, err, forma.ErrPreconditionFailed, "the written instant is not before $now")
		_, err = tx.Update(ctx, condUpdate(rowID,
			and(kv("due", "equals:$now"), kv("day", "equals:$now"), kv("due_main", "equals:$now+1h")),
			map[string]any{"name": "same instant"}))
		return err
	})
	require.NoError(t, err)
	assert.Equal(t, "same instant", e.get(t, rowID)["name"])
}

// TestConditionalUpdateFailureRollsBackTheTx is the #593 test: a Tx updates
// A, creates B, and a conditional update of C fails; fn returns the failure
// and nothing the Tx wrote survives, change_log included.
func TestConditionalUpdateFailureRollsBackTheTx(t *testing.T) {
	e := newCondPG(t)
	a := e.create(t, map[string]any{"version": int64(1), "name": "a"})
	c := e.create(t, map[string]any{"version": int64(7), "name": "c"})
	beforeA, beforeC := e.snapshot(t, a), e.snapshot(t, c)

	var b uuid.UUID
	err := e.txr.InTx(e.ctx, func(ctx context.Context, tx forma.Tx) error {
		if _, err := tx.Update(ctx, condUpdate(a, nil, map[string]any{"name": "a2"}, forma.Increment("version", 1))); err != nil {
			return err
		}
		created, err := tx.Create(ctx, &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: condSchemaName}, Data: map[string]any{"name": "b"}})
		if err != nil {
			return err
		}
		b = created.RowID
		_, err = tx.Update(ctx, condUpdate(c, kv("version", "equals:6"), nil, forma.Increment("version", 1)))
		return err
	})
	require.ErrorIs(t, err, forma.ErrPreconditionFailed)
	require.NotEqual(t, uuid.Nil, b)

	assert.Equal(t, beforeA, e.snapshot(t, a), "A is unchanged")
	assert.Equal(t, rowSnapshot{eav: "[]", changeLog: "[]"}, e.snapshot(t, b), "B and its change_log entry are gone")
	assert.Equal(t, beforeC, e.snapshot(t, c), "C is unchanged")
}

// TestSwallowedPreconditionFailureDoesNotPoisonTheTx is its variant: fn
// handles the refusals and returns nil, and the Tx's other writes commit.
func TestSwallowedPreconditionFailureDoesNotPoisonTheTx(t *testing.T) {
	e := newCondPG(t)
	a := e.create(t, map[string]any{"version": int64(1), "name": "a"})
	c := e.create(t, map[string]any{"version": int64(7), "name": "c"})
	beforeC := e.snapshot(t, c)

	var b uuid.UUID
	err := e.txr.InTx(e.ctx, func(ctx context.Context, tx forma.Tx) error {
		if _, err := tx.Update(ctx, condUpdate(a, kv("version", "equals:1"), map[string]any{"name": "a2"}, forma.Increment("version", 1))); err != nil {
			return err
		}
		_, err := tx.Update(ctx, condUpdate(c, kv("version", "equals:6"), nil, forma.Increment("version", 1)))
		require.ErrorIs(t, err, forma.ErrPreconditionFailed)
		require.ErrorIs(t, tx.Delete(ctx, condDelete(c, kv("name", "equals:not-c"))), forma.ErrPreconditionFailed)
		created, err := tx.Create(ctx, &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: condSchemaName}, Data: map[string]any{"name": "b"}})
		if err != nil {
			return err
		}
		b = created.RowID
		return nil
	})
	require.NoError(t, err)

	gotA := e.get(t, a)
	assert.Equal(t, "a2", gotA["name"])
	assert.EqualValues(t, 2, gotA["version"])
	assert.Equal(t, "b", e.get(t, b)["name"])
	assert.Equal(t, beforeC, e.snapshot(t, c), "C is unchanged")
}

// TestUpdateOperatorRefusalAfterSQLPoisonsTheTx pins the one operator
// refusal that comes after the write has sent SQL: a missing value or an
// overflow is judged against the locked row, so the Tx is spent.
func TestUpdateOperatorRefusalAfterSQLPoisonsTheTx(t *testing.T) {
	e := newCondPG(t)
	rowID := e.create(t, map[string]any{"name": "no counter"})
	err := e.txr.InTx(e.ctx, func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Update(ctx, condUpdate(rowID, nil, nil, forma.Increment("counter", 1)))
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		_, err = tx.Update(ctx, condUpdate(rowID, nil, map[string]any{"name": "after"}))
		require.ErrorIs(t, err, forma.ErrTxAborted)
		return nil
	})
	require.ErrorIs(t, err, forma.ErrTxAborted)
	assert.Equal(t, "no counter", e.get(t, rowID)["name"])
}
