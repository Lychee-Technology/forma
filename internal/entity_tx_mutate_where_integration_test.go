package internal

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Predicate mutation (#652) on real PostgreSQL over the task652 schema.

// Special scenario 5: UpdateWhere cancels exactly instance I's pending
// tasks, answers them in row ID order, and each answered record is the row
// as committed.
func TestUpdateWhereCancelsExactlyTheMatchingRows(t *testing.T) {
	env := newTaskPG(t)
	pending := env.createN(t, 6, func(i int) map[string]any { return task("I", "pending", i) })
	done := env.createN(t, 4, func(i int) map[string]any { return task("I", "done", i) })
	otherInstance := env.createN(t, 5, func(i int) map[string]any { return task("J", "pending", i) })
	untouched := env.snapshots(t, append(done, otherInstance...))

	result, err := env.updateWhere(&forma.MutateWhereRequest{SchemaName: taskSchemaName, Condition: pendingOf("I"),
		Updates:   map[string]any{"status": "cancelled"},
		UpdateOps: []forma.UpdateOp{forma.ServerTime("resolved_at", 0)},
		Returning: true})
	require.NoError(t, err)

	want := byRowID(pending)
	assert.Equal(t, want, result.RowIDs, "the six pending tasks of I, in row ID order")
	require.Equal(t, want, rowIDs(result.Records))
	for _, record := range result.Records {
		assert.Equal(t, "cancelled", record.Attributes["status"])
		assert.NotNil(t, record.Attributes["resolved_at"])
		assert.Equal(t, env.attrs(t, record.RowID), record.Attributes, "the returned record is the committed row")
		env.requireStoredAs(t, record)
	}
	env.requireUnchanged(t, untouched, "a row the condition does not match is not written")
}

func TestDeleteWhereReturnsThePreDeleteRowsAndStampsTombstones(t *testing.T) {
	env := newTaskPG(t)
	expired := env.createN(t, 3, func(i int) map[string]any {
		data := task("I", "done", i)
		data["eligible_cleanup_at"] = "2026-09-01T00:00:00Z"
		return data
	})
	kept := env.create(t, map[string]any{"instance": "I", "status": "done", "eligible_cleanup_at": "2026-12-01T00:00:00Z"})
	untouched := env.snapshots(t, []uuid.UUID{kept})
	// Advance one row's version stamp, so its tombstone must land past it.
	_, err := env.em.Update(env.ctx, &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: taskSchemaName, RowID: expired[0]},
		UpdateOps: []forma.UpdateOp{forma.Increment("version", 1)}})
	require.NoError(t, err)
	before := make(map[uuid.UUID]map[string]any, len(expired))
	lastStamp := make(map[uuid.UUID]int64, len(expired))
	for _, id := range expired {
		before[id] = env.attrs(t, id)
		lastStamp[id], _ = env.hotStampOf(t, taskSchemaID, id)
	}

	result, err := env.deleteWhere(&forma.MutateWhereRequest{SchemaName: taskSchemaName,
		Condition: kv("eligible_cleanup_at", "lte:2026-10-01T00:00:00Z"), Returning: true})
	require.NoError(t, err)

	require.Equal(t, byRowID(expired), result.RowIDs)
	for _, record := range result.Records {
		assert.Equal(t, before[record.RowID], record.Attributes, "the returned record is the row before its delete")
		assert.Nil(t, env.attrs(t, record.RowID), "the row is gone")
		stamp, deleted := env.tombstone(t, record.RowID)
		assert.True(t, deleted, "the hot change_log entry is a tombstone")
		assert.Greater(t, stamp, lastStamp[record.RowID], "the tombstone is stamped after the row's last update (#274)")
	}
	env.requireUnchanged(t, untouched, "a row past the cutoff is kept")
}

// Limit takes the first rows in Sort order, row ID breaking ties.
func TestMutateWhereLimitTakesTheFirstRowsInSortOrder(t *testing.T) {
	env := newTaskPG(t)
	priorities := []int{5, 9, 9, 7, 1, 9}
	ids := env.createN(t, len(priorities), func(i int) map[string]any { return task("I", "pending", priorities[i]) })
	var nines []uuid.UUID
	for i, p := range priorities {
		if p == 9 {
			nines = append(nines, ids[i])
		}
	}
	want := append(byRowID(nines), ids[3]) // the three 9s by row ID, then the 7

	result, err := env.updateWhere(&forma.MutateWhereRequest{SchemaName: taskSchemaName, Condition: pendingOf("I"),
		Sort: []forma.OrderBy{{Attribute: "priority", SortOrder: forma.SortOrderDesc}}, Limit: 4,
		Updates: map[string]any{"status": "picked"}})
	require.NoError(t, err)
	assert.Equal(t, want, result.RowIDs)
	assert.Nil(t, result.Records, "no records without Returning")
	for i, id := range ids {
		wantStatus := "pending"
		if slices.Contains(want, id) {
			wantStatus = "picked"
		}
		assert.Equal(t, wantStatus, env.attrs(t, id)["status"], "row %d (priority %d)", i, priorities[i])
	}
}

// More matching rows than MaxAffected is refused after the candidate select:
// nothing is written and the Tx commits its other work.
func TestMutateWhereOverflowWritesNothingAndLeavesTheTxUsable(t *testing.T) {
	env := newTaskPG(t)
	ids := env.createN(t, 6, func(i int) map[string]any { return task("I", "pending", i) })
	before := env.snapshots(t, ids)

	var other uuid.UUID
	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.UpdateWhere(ctx, &forma.MutateWhereRequest{SchemaName: taskSchemaName, Condition: pendingOf("I"),
			MaxAffected: 5, Updates: map[string]any{"status": "cancelled"}})
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		require.ErrorContains(t, err, "more than MaxAffected 5")
		_, err = tx.DeleteWhere(ctx, &forma.MutateWhereRequest{SchemaName: taskSchemaName, Condition: pendingOf("I"), MaxAffected: 5})
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		created, err := tx.Create(ctx, &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: taskSchemaName},
			Data: task("K", "pending", 0)})
		if err != nil {
			return err
		}
		other = created.RowID
		return nil
	})
	require.NoError(t, err, "the overflow did not poison the Tx")
	env.requireUnchanged(t, before, "the overflow wrote nothing")
	assert.NotNil(t, env.attrs(t, other), "the Tx's other work committed")
}

// Exactly MaxAffected matching rows is not an overflow: all of them are
// affected.
func TestMutateWhereAffectsExactlyMaxAffectedRows(t *testing.T) {
	env := newTaskPG(t)
	ids := env.createN(t, 5, func(i int) map[string]any { return task("I", "pending", i) })

	result, err := env.updateWhere(&forma.MutateWhereRequest{SchemaName: taskSchemaName, Condition: pendingOf("I"),
		MaxAffected: 5, Updates: map[string]any{"status": "cancelled"}})
	require.NoError(t, err)
	assert.Equal(t, byRowID(ids), result.RowIDs)
}

func TestMutateWhereMatchingNothingIsEmptyNotAnError(t *testing.T) {
	env := newTaskPG(t)
	ids := env.createN(t, 2, func(i int) map[string]any { return task("I", "done", i) })
	before := env.snapshots(t, ids)

	updated, err := env.updateWhere(&forma.MutateWhereRequest{SchemaName: taskSchemaName, Condition: pendingOf("I"),
		Updates: map[string]any{"status": "cancelled"}, Returning: true})
	require.NoError(t, err)
	assert.Empty(t, updated.RowIDs)
	assert.Empty(t, updated.Records)
	deleted, err := env.deleteWhere(&forma.MutateWhereRequest{SchemaName: taskSchemaName, Condition: pendingOf("I")})
	require.NoError(t, err)
	assert.Empty(t, deleted.RowIDs)
	assert.Nil(t, deleted.Records)
	env.requireUnchanged(t, before, "nothing matched, nothing written")
}

// A row the Tx created is a candidate, and two predicate mutations in one Tx
// see each other: LTFlow's CompleteMatchingAndCancelPending.
func TestMutateWhereSeesTheTxsOwnWrites(t *testing.T) {
	env := newTaskPG(t)
	committed := env.createN(t, 4, func(i int) map[string]any {
		data := task("I", "pending", i)
		data["expected_event"] = []string{"approve", "reject"}[i%2]
		return data
	})
	var completed, cancelled *forma.MutateWhereResult
	var created uuid.UUID
	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		data := task("I", "pending", 9)
		data["expected_event"] = "approve"
		record, err := tx.Create(ctx, &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: taskSchemaName}, Data: data})
		require.NoError(t, err)
		created = record.RowID
		completed, err = tx.UpdateWhere(ctx, &forma.MutateWhereRequest{SchemaName: taskSchemaName,
			Condition: and(pendingOf("I"), kv("expected_event", "equals:approve")), Updates: map[string]any{"status": "completed"}})
		require.NoError(t, err)
		cancelled, err = tx.UpdateWhere(ctx, &forma.MutateWhereRequest{SchemaName: taskSchemaName,
			Condition: pendingOf("I"), Updates: map[string]any{"status": "cancelled"}})
		return err
	})
	require.NoError(t, err)

	assert.Contains(t, completed.RowIDs, created, "a row created earlier in the Tx is a candidate")
	assert.Len(t, completed.RowIDs, 3)
	assert.Len(t, cancelled.RowIDs, 2)
	for _, id := range completed.RowIDs {
		assert.NotContains(t, cancelled.RowIDs, id, "the second call does not see the rows the first completed as pending")
	}
	all := append(slices.Clone(committed), created)
	assert.ElementsMatch(t, all, append(slices.Clone(completed.RowIDs), cancelled.RowIDs...), "together they cover the pending set")
}

func TestMutateWhereRollsBackWithItsTx(t *testing.T) {
	env := newTaskPG(t)
	ids := env.createN(t, 3, func(i int) map[string]any { return task("I", "pending", i) })
	before := env.snapshots(t, ids)
	bail := errors.New("roll back")
	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		updated, err := tx.UpdateWhere(ctx, &forma.MutateWhereRequest{SchemaName: taskSchemaName, Condition: pendingOf("I"),
			Updates: map[string]any{"status": "cancelled"}})
		require.NoError(t, err)
		require.Len(t, updated.RowIDs, 3)
		_, err = tx.DeleteWhere(ctx, &forma.MutateWhereRequest{SchemaName: taskSchemaName, Condition: kv("instance", "equals:I")})
		require.NoError(t, err)
		return bail
	})
	require.ErrorIs(t, err, bail)
	env.requireUnchanged(t, before, "a rolled-back predicate mutation leaves every row and change_log entry")
}

// A failure after the first row was written poisons the Tx, whatever its
// class; one before any row was written follows the usual rules.
func TestMutateWhereFailureAfterTheFirstWritePoisons(t *testing.T) {
	env := newTaskPG(t)
	ids := env.createN(t, 5, func(i int) map[string]any { return task("I", "pending", i) })
	before := env.snapshots(t, ids)
	conflict := fmt.Errorf("injected: %w", forma.ErrUniqueConflict)
	failAt := 0
	env.txr.mutateHooks = &mutateWhereHooks{beforeWrite: func(i int, _ uuid.UUID) error {
		if i == failAt {
			return conflict
		}
		return nil
	}}
	cancel := &forma.MutateWhereRequest{SchemaName: taskSchemaName, Condition: pendingOf("I"), Updates: map[string]any{"status": "cancelled"}}

	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.UpdateWhere(ctx, cancel)
		require.ErrorIs(t, err, forma.ErrUniqueConflict)
		return nil
	})
	require.NoError(t, err, "a unique conflict before any row was written leaves the Tx usable")
	env.requireUnchanged(t, before, "the first row was not written")

	failAt = 2
	err = env.inTx(func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.UpdateWhere(ctx, cancel)
		require.ErrorIs(t, err, forma.ErrUniqueConflict)
		return nil
	})
	require.ErrorIs(t, err, forma.ErrTxAborted, "two rows were written: the Tx cannot commit")
	require.ErrorIs(t, err, forma.ErrUniqueConflict, "InTx carries the cause")
	env.requireUnchanged(t, before, "no row changed")
}
