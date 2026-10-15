package coordinationtest

import (
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// Special scenario 5 and the adapter's other predicate mutations (#652).

func TestPredicateMutationScenario(t *testing.T) {
	e := newEnv(t)
	inst := e.seedInstance(t, 4)
	inst.State = "review"
	older := *inst
	older.Version = 3
	other := e.seedInstance(t, 4)
	matching := []*Task{
		task(inst, "match-1", TaskPending, "approve", PurposeWorker),
		task(inst, "match-2", TaskPending, "approve", PurposeWorker),
		task(inst, "match-3", TaskPending, "approve", PurposeWorker),
	}
	pendingOthers := []*Task{
		task(inst, "timer", TaskPending, "approve", PurposeTimer),
		task(inst, "reject", TaskPending, "reject", PurposeWorker),
		task(&older, "older-visit", TaskPending, "approve", PurposeWorker),
		task(inst, "escalate", TaskPending, "escalate", PurposeTimer),
	}
	done := []*Task{
		task(inst, "done-completed", TaskCompleted, "approve", PurposeWorker),
		task(inst, "done-cancelled", TaskCancelled, "approve", PurposeWorker),
		task(inst, "done-expired", TaskExpired, "approve", PurposeTimer),
	}
	control := task(other, "other-instance", TaskPending, "approve", PurposeWorker)
	e.seedTasks(t, slices.Concat(matching, pendingOthers, done, []*Task{control})...)
	untouched := slices.Concat(done, []*Task{control})
	entries := map[uuid.UUID][][2]int64{}
	for _, tk := range untouched {
		entries[tk.ID] = e.changeLogEntries(t, TaskSchema, tk.ID)
	}
	before := e.stamps(t, TaskSchema, taskIDs(slices.Concat(matching, pendingOthers))...)

	var completed, cancelled []Task
	var readBack *Task
	require.NoError(t, e.inTx(func(uow UnitOfWork) error {
		var err error
		completed, cancelled, err = uow.Tasks().CompleteMatchingAndCancelPending(e.ctx, inst.ID, "review", 4, "approve")
		if err != nil {
			return err
		}
		readBack, err = uow.Tasks().GetByTokenHash(e.ctx, "match-1")
		return err
	}))
	require.Equal(t, TaskCompleted, readBack.Status, "a Tx.Query after the Tx's UpdateWhere reads its write")

	require.ElementsMatch(t, taskIDs(matching), returnedIDs(completed), "the three matching tasks are completed")
	require.ElementsMatch(t, taskIDs(pendingOthers), returnedIDs(cancelled), "the four other pending tasks are cancelled")
	for _, got := range slices.Concat(completed, cancelled) {
		require.Equal(t, map[string]any{"status": got.Status, "token_hash": got.TokenHash, "state": got.State, "version": got.Version},
			e.stored(t, TaskSchema, got.ID, "status", "token_hash", "state", "version"), "returned equals committed")
		require.NotNil(t, got.ResolvedAt, "task %s answers its resolved_at", got.TokenHash)
		require.Equal(t, strconv.FormatInt(got.ResolvedAt.UnixMilli(), 10), e.eavValues(t, TaskSchema, got.ID)["resolved_at"],
			"task %s: the resolved_at returned is the one committed", got.TokenHash)
	}
	require.ElementsMatch(t, slices.Concat(taskIDs(matching), []uuid.UUID{done[0].ID}), e.liveIDs(t, TaskSchema, "status", TaskCompleted))
	require.ElementsMatch(t, slices.Concat(taskIDs(pendingOthers), []uuid.UUID{done[1].ID}), e.liveIDs(t, TaskSchema, "status", TaskCancelled))
	require.Equal(t, []uuid.UUID{control.ID}, e.liveIDs(t, TaskSchema, "status", TaskPending), "the other instance's task stays pending")
	for _, tk := range untouched {
		require.Equal(t, entries[tk.ID], e.changeLogEntries(t, TaskSchema, tk.ID), "task %s was not written", tk.TokenHash)
	}
	e.requireWritten(t, TaskSchema, before, taskIDs(slices.Concat(matching, pendingOthers))...)
}

func TestTaskMutations(t *testing.T) {
	t.Run("UpdateStatus moves a task from the expected status only", func(t *testing.T) {
		e := newEnv(t)
		inst := e.seedInstance(t, 1)
		e.seedTasks(t, task(inst, "h1", TaskPending, "approve", PurposeWorker))
		move := func(from, to string) error {
			return e.inTx(func(uow UnitOfWork) error { return uow.Tasks().UpdateStatus(e.ctx, "h1", from, to) })
		}
		require.NoError(t, move(TaskPending, TaskCompleted))
		require.ErrorIs(t, move(TaskPending, TaskCancelled), forma.ErrPreconditionFailed, "the task is no longer pending")
		require.ErrorIs(t, e.inTx(func(uow UnitOfWork) error {
			return uow.Tasks().UpdateStatus(e.ctx, "missing", TaskPending, TaskCompleted)
		}),
			forma.ErrPreconditionFailed)
		var got *Task
		require.NoError(t, e.inTx(func(uow UnitOfWork) error {
			var err error
			got, err = uow.Tasks().GetByTokenHash(e.ctx, "h1")
			return err
		}))
		require.Equal(t, TaskCompleted, got.Status)
		require.ErrorIs(t, e.inTx(func(uow UnitOfWork) error {
			_, err := uow.Tasks().GetByTokenHash(e.ctx, "missing")
			return err
		}), forma.ErrNotFound)
	})

	t.Run("retention marks resolved tasks eligible, then deletes up to the limit", func(t *testing.T) {
		e := newEnv(t)
		inst := e.seedInstance(t, 1)
		resolved := []*Task{task(inst, "r1", TaskCompleted, "a", PurposeWorker), task(inst, "r2", TaskCancelled, "a", PurposeWorker),
			task(inst, "r3", TaskExpired, "a", PurposeTimer)}
		live := task(inst, "p1", TaskPending, "a", PurposeWorker)
		e.seedTasks(t, append(slices.Clone(resolved), live)...)
		eligible := time.Now().Add(-time.Hour)
		require.NoError(t, e.inTx(func(uow UnitOfWork) error {
			return uow.Tasks().MarkResolvedForInstanceEligible(e.ctx, inst.ID, eligible)
		}))
		require.Nil(t, e.stored(t, TaskSchema, live.ID, "eligible_cleanup_at")["eligible_cleanup_at"], "a pending task is not marked")
		for _, tk := range resolved {
			require.Equal(t, eligible.Truncate(time.Millisecond).UnixMilli(), e.stored(t, TaskSchema, tk.ID, "eligible_cleanup_at")["eligible_cleanup_at"])
		}
		var deleted int
		require.NoError(t, e.inTx(func(uow UnitOfWork) error {
			var err error
			deleted, err = uow.Tasks().DeleteEligible(e.ctx, time.Now(), 2)
			return err
		}))
		require.Equal(t, 2, deleted)
		require.Equal(t, 2, e.liveRows(t, TaskSchema), "the limit leaves one eligible task and the pending one")
		require.NotNil(t, e.stored(t, TaskSchema, live.ID, "status"))
	})
}

func TestLinksAndHistoryMutations(t *testing.T) {
	t.Run("links: insert, update status, get by parent task hash", func(t *testing.T) {
		e := newEnv(t)
		parent, child := e.seedInstance(t, 1), e.seedInstance(t, 1)
		link := &Link{ParentTaskHash: "parent-1", ParentInstanceID: parent.ID, ChildInstanceID: child.ID, Status: "running"}
		require.NoError(t, e.inTx(func(uow UnitOfWork) error { return uow.Links().Insert(e.ctx, link) }))
		require.NoError(t, e.inTx(func(uow UnitOfWork) error {
			if err := uow.Links().UpdateStatus(e.ctx, "parent-1", "completed"); err != nil {
				return err
			}
			return uow.Links().UpdateStatus(e.ctx, "no-such-parent", "completed")
		}), "an update that matches no link is no error, as in LTFlow")
		var got *Link
		require.NoError(t, e.inTx(func(uow UnitOfWork) error {
			var err error
			got, err = uow.Links().GetByParentTaskHash(e.ctx, "parent-1")
			return err
		}))
		require.Equal(t, Link{ID: link.ID, ParentTaskHash: "parent-1", ParentInstanceID: parent.ID, ChildInstanceID: child.ID, Status: "completed"}, *got)
		require.Equal(t, 1, e.liveRows(t, LinkSchema))
	})

	t.Run("history: MarkArchived stamps one instance's events and counts them", func(t *testing.T) {
		e := newEnv(t)
		a, b := e.seedInstance(t, 1), e.seedInstance(t, 1)
		events := []HistoryEvent{{InstanceID: a.ID, Type: "t1"}, {InstanceID: a.ID, Type: "t2"}, {InstanceID: b.ID, Type: "t1"}}
		require.NoError(t, e.inTx(func(uow UnitOfWork) error { return uow.History().Insert(e.ctx, events) }))
		at := time.Now().Add(-time.Minute)
		var n int
		require.NoError(t, e.inTx(func(uow UnitOfWork) error {
			var err error
			n, err = uow.History().MarkArchived(e.ctx, a.ID, at)
			return err
		}))
		require.Equal(t, 2, n)
		for _, evt := range events[:2] {
			require.Equal(t, at.Truncate(time.Millisecond).UnixMilli(), e.stored(t, HistorySchema, evt.ID, "archived_at")["archived_at"])
		}
		require.Nil(t, e.stored(t, HistorySchema, events[2].ID, "archived_at")["archived_at"], "the other instance's event is not archived")
	})

	t.Run("instances: Get and ListByFilter read inside the transaction", func(t *testing.T) {
		e := newEnv(t)
		running := e.seedInstance(t, 1)
		var got *Instance
		var listed []Instance
		require.NoError(t, e.inTx(func(uow UnitOfWork) error {
			done := &Instance{ID: newID(), WorkflowName: workflowName, State: "end", Status: "completed", Version: 1}
			if err := uow.Instances().Create(e.ctx, done); err != nil {
				return err
			}
			var err error
			if got, err = uow.Instances().Get(e.ctx, running.ID); err != nil {
				return err
			}
			listed, err = uow.Instances().ListByFilter(e.ctx, workflowName, "completed")
			return err
		}))
		require.Equal(t, running.BusinessReference, got.BusinessReference)
		require.Len(t, listed, 1, "the Tx sees its own uncommitted create")
		require.ErrorIs(t, e.inTx(func(uow UnitOfWork) error {
			_, err := uow.Instances().Get(e.ctx, newID())
			return err
		}), forma.ErrNotFound)
	})
}

func taskIDs(tasks []*Task) []uuid.UUID {
	out := make([]uuid.UUID, len(tasks))
	for i, tk := range tasks {
		out[i] = tk.ID
	}
	return out
}

func returnedIDs(tasks []Task) []uuid.UUID {
	out := make([]uuid.UUID, len(tasks))
	for i, tk := range tasks {
		out[i] = tk.ID
	}
	return out
}
