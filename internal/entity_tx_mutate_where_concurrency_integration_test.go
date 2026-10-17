package internal

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Concurrency proofs of predicate mutation (#652) on real PostgreSQL. Each
// worker runs its own Tx on its own connection. A concurrent change lands
// between the candidate select and the lock through the afterCandidates
// hook, and every wait on server state polls pg_locks; nothing sleeps for a
// fixed time. The re-check's drop is observable only where a lock waits, so
// the tests that assert it are _PostgresOnly.

func (e *taskPG) setStatus(t *testing.T, rowID uuid.UUID, status string) {
	t.Helper()
	_, err := e.em.Update(e.ctx, &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: taskSchemaName, RowID: rowID},
		Updates: map[string]any{"status": status}})
	require.NoError(t, err)
}

// onceAfterCandidates installs a hook that runs change the first time a
// predicate mutation has selected its candidates.
func (e *taskPG) onceAfterCandidates(t *testing.T, change func(candidates []uuid.UUID)) {
	t.Helper()
	ran := false
	e.txr.testHooks = &txTestHooks{afterCandidates: func(candidates []uuid.UUID) {
		if !ran {
			ran = true
			change(candidates)
		}
	}}
	t.Cleanup(func() { e.txr.testHooks = nil })
}

// A candidate another transaction changes and commits between the candidate
// select and its lock no longer matches: it is neither written nor returned.
func TestMutateWhereDropsACandidateThatStoppedMatching_PostgresOnly(t *testing.T) {
	env := newTaskPG(t)
	pending := env.createN(t, 6, func(i int) map[string]any { return task("I", "pending", i) })
	victim := pending[2]
	env.onceAfterCandidates(t, func(candidates []uuid.UUID) {
		require.Contains(t, candidates, victim, "the victim was a candidate")
		env.setStatus(t, victim, "done")
	})

	result, err := env.updateWhere(&forma.MutateWhereRequest{SchemaName: taskSchemaName, Condition: pendingOf("I"),
		Updates: map[string]any{"status": "cancelled"}, Returning: true})
	require.NoError(t, err)

	survivors := slices.DeleteFunc(slices.Clone(pending), func(id uuid.UUID) bool { return id == victim })
	assert.Equal(t, byRowID(survivors), result.RowIDs, "five rows, without the one that stopped matching")
	assert.Equal(t, byRowID(survivors), rowIDs(result.Records))
	assert.Equal(t, "done", env.attrs(t, victim)["status"], "the concurrent writer's value stands")
}

// A row that starts matching after the candidate select is not affected.
func TestMutateWhereLeavesAPhantomAlone_PostgresOnly(t *testing.T) {
	env := newTaskPG(t)
	pending := env.createN(t, 3, func(i int) map[string]any { return task("I", "pending", i) })
	var phantom uuid.UUID
	env.onceAfterCandidates(t, func([]uuid.UUID) { phantom = env.create(t, task("I", "pending", 99)) })

	result, err := env.updateWhere(&forma.MutateWhereRequest{SchemaName: taskSchemaName, Condition: pendingOf("I"),
		Updates: map[string]any{"status": "cancelled"}})
	require.NoError(t, err)

	assert.Equal(t, byRowID(pending), result.RowIDs)
	assert.NotContains(t, result.RowIDs, phantom)
	assert.Equal(t, "pending", env.attrs(t, phantom)["status"], "the phantom still matches after commit")
}

// A predicate mutation waits on a candidate a single-row writer holds, and
// once that writer commits a change that makes it non-matching, completes
// without it.
func TestMutateWhereWaitsForASingleRowWriter_PostgresOnly(t *testing.T) {
	env := newTaskPG(t)
	pending := env.createN(t, 4, func(i int) map[string]any { return task("I", "pending", i) })
	held := pending[1]

	written, release, writer := env.holdingTx(func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Update(ctx, &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: taskSchemaName, RowID: held},
			Updates: map[string]any{"status": "done"}})
		return err
	})
	defer release()
	await(t, written, writer, "the single-row write")

	type outcome struct {
		result *forma.MutateWhereResult
		err    error
	}
	mutation := make(chan outcome, 1)
	go func() {
		result, err := env.updateWhere(&forma.MutateWhereRequest{SchemaName: taskSchemaName, Condition: pendingOf("I"),
			Updates: map[string]any{"status": "cancelled"}})
		mutation <- outcome{result, err}
	}()
	require.Eventually(t, func() bool { return env.rowLockWaitersOf(taskSchemaID, held) == 1 }, lockWaitDeadline, time.Millisecond,
		"the predicate mutation waits on the held candidate")

	release()
	require.NoError(t, <-writer)
	got := <-mutation
	require.NoError(t, got.err)
	survivors := slices.DeleteFunc(slices.Clone(pending), func(id uuid.UUID) bool { return id == held })
	assert.Equal(t, byRowID(survivors), got.result.RowIDs)
	assert.Equal(t, "done", env.attrs(t, held)["status"])
}

// Two predicate mutations over overlapping sets lose no update: every row in
// the intersection is incremented by both, for 50 iterations.
func TestOverlappingMutateWhereLosesNoUpdate(t *testing.T) {
	const iterations = 50
	env := newTaskPG(t)
	groups := []string{"x", "y", "z"}
	ids := env.createN(t, 12, func(i int) map[string]any {
		data := task("I", "pending", i)
		data["group"] = groups[i%3]
		return data
	})
	increment := func(groupList string) func(int) error {
		return func(int) error {
			_, err := env.updateWhere(&forma.MutateWhereRequest{SchemaName: taskSchemaName,
				Condition: kv("group", "in:"+groupList), UpdateOps: []forma.UpdateOp{forma.Increment("counter", 1)}})
			return err
		}
	}
	xy, yz := increment(`["x","y"]`), increment(`["y","z"]`)

	for iter := range iterations {
		errs := runTogether(2, func(i int) error {
			if i == 0 {
				return xy(i)
			}
			return yz(i)
		})
		require.NoError(t, errors.Join(errs...), "iteration %d", iter)
	}
	for i, id := range ids {
		want := iterations
		if groups[i%3] == "y" {
			want = 2 * iterations
		}
		assert.EqualValues(t, want, env.attrs(t, id)["counter"], "row %d in group %s", i, groups[i%3])
	}
}

// Opposite sorts lock in one order: two mutations over the same 100 rows,
// one sorted ascending and one descending, never deadlock.
func TestOppositelySortedMutateWhereNeverDeadlocks(t *testing.T) {
	const iterations = 50
	env := newTaskPG(t)
	ids := env.createN(t, 100, func(i int) map[string]any { return task("I", "pending", i) })
	sorted := func(order forma.SortOrder) func(int) error {
		return func(int) error {
			_, err := env.updateWhere(&forma.MutateWhereRequest{SchemaName: taskSchemaName, Condition: kv("instance", "equals:I"),
				Sort: []forma.OrderBy{{Attribute: "priority", SortOrder: order}}, UpdateOps: []forma.UpdateOp{forma.Increment("counter", 1)}})
			return err
		}
	}
	asc, desc := sorted(forma.SortOrderAsc), sorted(forma.SortOrderDesc)

	for iter := range iterations {
		errs := runTogether(2, func(i int) error {
			if i == 0 {
				return asc(i)
			}
			return desc(i)
		})
		for _, err := range errs {
			require.NotErrorIs(t, err, forma.ErrRetryableConflict, "iteration %d: no deadlock", iter)
			require.NoError(t, err, "iteration %d", iter)
		}
	}
	for _, id := range ids {
		require.EqualValues(t, 2*iterations, env.attrs(t, id)["counter"], "every row incremented by both mutations in every iteration")
	}
}
