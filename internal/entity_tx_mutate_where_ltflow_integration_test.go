package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// LTFlow's PostgreSQL task and retention stores (#652 motivation) written as
// predicate mutations over the task652 schema. Each mirrors one statement
// LTFlow runs inside its InTx today.

// CancelPendingForInstanceVersion: UPDATE workflow_tasks SET status =
// 'cancelled', updated_at = NOW(), resolved_at = NOW() WHERE instance_id = $1
// AND state = $2 AND version = $3 AND status = 'pending' RETURNING ...
func TestLTFlowCancelPendingForInstanceVersion(t *testing.T) {
	env := newTaskPG(t)
	shaped := func(instance, state string, version int64, status string) map[string]any {
		return map[string]any{"instance": instance, "state": state, "version": version, "status": status}
	}
	target := env.createN(t, 3, func(int) map[string]any { return shaped("I", "review", 4, "pending") })
	others := []uuid.UUID{
		env.create(t, shaped("I", "review", 3, "pending")),   // an older version
		env.create(t, shaped("I", "approve", 4, "pending")),  // another state
		env.create(t, shaped("I", "review", 4, "completed")), // already resolved
		env.create(t, shaped("J", "review", 4, "pending")),   // another instance
	}
	untouched := env.snapshots(t, others)

	result, err := env.updateWhere(&forma.MutateWhereRequest{SchemaName: taskSchemaName,
		Condition: and(kv("instance", "equals:I"), kv("state", "equals:review"), kv("version", "equals:4"), kv("status", "equals:pending")),
		Updates:   map[string]any{"status": "cancelled"},
		UpdateOps: []forma.UpdateOp{forma.ServerTime("updated_at", 0), forma.ServerTime("resolved_at", 0)},
		Returning: true})
	require.NoError(t, err)

	require.Equal(t, byRowID(target), result.RowIDs)
	for _, record := range result.Records {
		assert.Equal(t, "cancelled", record.Attributes["status"])
		assert.Equal(t, record.Attributes["updated_at"], record.Attributes["resolved_at"], "both stamps are the transaction's instant")
		assert.Equal(t, env.attrs(t, record.RowID), record.Attributes)
	}
	env.requireUnchanged(t, untouched, "no other task is written")
}

// UpdateStatus(from, to): UPDATE ... WHERE token_hash = $2 AND status = $3.
// LTFlow reports zero rows affected as an error; here that is an empty
// RowIDs.
func TestLTFlowUpdateStatusDetectsZeroRows(t *testing.T) {
	env := newTaskPG(t)
	row := env.create(t, map[string]any{"instance": "I", "token_hash": "h1", "status": "pending"})
	updateStatus := func(from, to string) (int, error) {
		result, err := env.updateWhere(&forma.MutateWhereRequest{SchemaName: taskSchemaName,
			Condition: and(kv("token_hash", "equals:h1"), kv("status", "equals:"+from)), Updates: map[string]any{"status": to}})
		if err != nil {
			return 0, err
		}
		return len(result.RowIDs), nil
	}

	n, err := updateStatus("pending", "completed")
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	n, err = updateStatus("pending", "completed")
	require.NoError(t, err)
	assert.Zero(t, n, "the task is no longer pending: zero rows, which the caller turns into its error")
	assert.Equal(t, "completed", env.attrs(t, row)["status"])
}

// Retention: DELETE ... WHERE id IN (SELECT id ... WHERE eligible_cleanup_at
// <= $1 LIMIT $2), in bounded batches until a batch deletes nothing.
func TestLTFlowBatchedRetention(t *testing.T) {
	env := newTaskPG(t)
	expired := env.createN(t, 250, func(i int) map[string]any {
		return map[string]any{"instance": "I", "status": "done", "eligible_cleanup_at": "2026-09-01T00:00:00Z", "priority": i}
	})
	kept := env.createN(t, 3, func(i int) map[string]any {
		return map[string]any{"instance": "I", "status": "done", "eligible_cleanup_at": "2026-12-01T00:00:00Z", "priority": i}
	})
	untouched := env.snapshots(t, kept)

	var batches []int
	deleted := make(map[uuid.UUID]bool)
	for len(batches) < 10 {
		result, err := env.deleteWhere(&forma.MutateWhereRequest{SchemaName: taskSchemaName,
			Condition: kv("eligible_cleanup_at", "lte:2026-10-01T00:00:00Z"), Limit: 100})
		require.NoError(t, err)
		batches = append(batches, len(result.RowIDs))
		for _, id := range result.RowIDs {
			deleted[id] = true
		}
		if len(result.RowIDs) == 0 {
			break
		}
	}
	assert.Equal(t, []int{100, 100, 50, 0}, batches)
	assert.Len(t, deleted, len(expired), "every expired task deleted once")
	for _, id := range expired {
		assert.True(t, deleted[id])
	}
	env.requireUnchanged(t, untouched, "a task not yet eligible is kept")
}

// The operation limit counts each affected row; a breach poisons.
func TestMutateWhereCountsEachAffectedRowAsAnOperation(t *testing.T) {
	env := newTaskPG(t)
	ids := env.createN(t, 4, func(i int) map[string]any { return task("I", "pending", i) })
	before := env.snapshots(t, ids)
	cancel := &forma.MutateWhereRequest{SchemaName: taskSchemaName, Condition: pendingOf("I"), Updates: map[string]any{"status": "cancelled"}}

	err := env.txr.InTx(env.ctx, func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.UpdateWhere(ctx, cancel)
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		require.ErrorContains(t, err, "limit of 3 operations")
		return nil
	}, forma.WithTxMaxOperations(3)) // four affected rows are four operations
	require.ErrorIs(t, err, forma.ErrTxAborted)
	env.requireUnchanged(t, before, "the breach wrote nothing that commits")

	err = env.txr.InTx(env.ctx, func(ctx context.Context, tx forma.Tx) error {
		result, err := tx.UpdateWhere(ctx, cancel)
		if err != nil {
			return err
		}
		if len(result.RowIDs) != 4 {
			return fmt.Errorf("affected %d rows, want 4", len(result.RowIDs))
		}
		_, err = tx.Get(ctx, &forma.QueryRequest{SchemaName: taskSchemaName, RowID: &ids[0]})
		if !errors.Is(err, forma.ErrInvalidInput) {
			return fmt.Errorf("a fifth operation should breach the limit, got %v", err)
		}
		return nil
	}, forma.WithTxMaxOperations(4))
	require.ErrorIs(t, err, forma.ErrTxAborted, "four rows are four operations, so a fifth call breaches")
}
