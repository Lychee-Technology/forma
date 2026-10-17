package coordinationtest

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// #593's transaction tests and special scenario 1, through the adapter's
// InTx. Every check of committed state reads the tables with direct SQL.

func TestTransactionsCommitAtomically(t *testing.T) {
	t.Run("create + create across different schemas commits atomically", func(t *testing.T) {
		e := newEnv(t)
		inst := &Instance{ID: newID(), WorkflowName: workflowName, State: "start", Status: "running", Version: 1}
		tk := task(inst, "hash-create", TaskPending, "approve", PurposeWorker)
		require.NoError(t, e.inTx(func(uow UnitOfWork) error {
			if err := uow.Instances().Create(e.ctx, inst); err != nil {
				return err
			}
			return uow.Tasks().Insert(e.ctx, tk)
		}))
		require.Equal(t, map[string]any{"state": "start", "version": int64(1)}, e.stored(t, InstanceSchema, inst.ID, "state", "version"))
		require.Equal(t, map[string]any{"token_hash": "hash-create", "instance_id": inst.ID.String()},
			e.stored(t, TaskSchema, tk.ID, "token_hash", "instance_id"))
		e.requireWritten(t, InstanceSchema, nil, inst.ID)
		e.requireWritten(t, TaskSchema, nil, tk.ID)
	})

	t.Run("update + create commits atomically", func(t *testing.T) {
		e := newEnv(t)
		a := e.seedInstance(t, 7)
		before := e.stamps(t, InstanceSchema, a.ID)
		events := []HistoryEvent{{InstanceID: a.ID, Type: "transition", StateFrom: "start", StateTo: "shipping"}}
		require.NoError(t, e.inTx(func(uow UnitOfWork) error {
			if err := uow.Instances().Update(e.ctx, advanced(a, "shipping"), 7); err != nil {
				return err
			}
			return uow.History().Insert(e.ctx, events)
		}))
		require.Equal(t, map[string]any{"state": "shipping", "version": int64(8)}, e.stored(t, InstanceSchema, a.ID, "state", "version"))
		require.Equal(t, map[string]any{"type": "transition"}, e.stored(t, HistorySchema, events[0].ID, "type"))
		e.requireWritten(t, InstanceSchema, before, a.ID)
		e.requireWritten(t, HistorySchema, nil, events[0].ID)
	})

	t.Run("update + delete + create commits atomically", func(t *testing.T) {
		e := newEnv(t)
		a := e.seedInstance(t, 7)
		done := task(a, "hash-done", TaskCompleted, "approve", PurposeWorker)
		eligible := time.Now().Add(-time.Hour)
		done.EligibleCleanupAt = &eligible
		e.seedTasks(t, done)
		before := e.stamps(t, InstanceSchema, a.ID)
		maps := e.stamps(t, TaskSchema, done.ID)
		msgs := []OutboxMessage{message("partition-a", time.Now())}
		require.NoError(t, e.inTx(func(uow UnitOfWork) error {
			if err := uow.Instances().Update(e.ctx, advanced(a, "shipping"), 7); err != nil {
				return err
			}
			n, err := uow.Tasks().DeleteEligible(e.ctx, time.Now(), 10)
			if err != nil {
				return err
			}
			if n != 1 {
				return fmt.Errorf("deleted %d tasks, want 1", n)
			}
			return uow.Outbox().Insert(e.ctx, msgs)
		}))
		require.Equal(t, map[string]any{"version": int64(8)}, e.stored(t, InstanceSchema, a.ID, "version"))
		require.Nil(t, e.stored(t, TaskSchema, done.ID, "status"), "the eligible task is deleted")
		require.Equal(t, map[string]any{"status": OutboxPending}, e.stored(t, OutboxSchema, msgs[0].ID, "status"))
		e.requireWritten(t, InstanceSchema, before, a.ID)
		e.requireWritten(t, TaskSchema, maps, done.ID)
		e.requireWritten(t, OutboxSchema, nil, msgs[0].ID)
	})
}

func TestTransactionsRollBack(t *testing.T) {
	t.Run("validation failure rolls back every previous operation (scenario 1)", func(t *testing.T) {
		e := newEnv(t)
		err := e.scenarioOne(t, func(uow UnitOfWork, a *Instance) error {
			return uow.History().Insert(e.ctx, []HistoryEvent{{InstanceID: a.ID, Type: ""}})
		})
		require.ErrorIs(t, err, forma.ErrInvalidInput, "history type has minLength 1")
	})

	t.Run("storage failure rolls back every previous operation (scenario 1, storage variant)", func(t *testing.T) {
		e := newEnv(t)
		e.failInserts(t, HistorySchema)
		err := e.scenarioOne(t, func(uow UnitOfWork, a *Instance) error {
			return uow.History().Insert(e.ctx, []HistoryEvent{{InstanceID: a.ID, Type: "transition"}})
		})
		require.ErrorContains(t, err, injectedFailure)
	})

	t.Run("CDC/change-log records roll back with the transaction", func(t *testing.T) {
		e := newEnv(t)
		abort := errors.New("the caller aborts after every write succeeded")
		err := e.scenarioOne(t, func(uow UnitOfWork, a *Instance) error {
			if err := uow.History().Insert(e.ctx, []HistoryEvent{{InstanceID: a.ID, Type: "transition"}}); err != nil {
				return err
			}
			return abort
		})
		require.ErrorIs(t, err, abort)
	})

	t.Run("conditional-update failure rolls back the whole transaction", func(t *testing.T) {
		e := newEnv(t)
		a := e.seedInstance(t, 7)
		entries := e.changeLogEntries(t, InstanceSchema, a.ID)
		err := e.inTx(func(uow UnitOfWork) error {
			if err := uow.Tasks().Insert(e.ctx, task(a, "hash-b", TaskPending, "approve", PurposeWorker)); err != nil {
				return err
			}
			if err := uow.Outbox().Insert(e.ctx, []OutboxMessage{message("partition-a", time.Now())}); err != nil {
				return err
			}
			return uow.Instances().Update(e.ctx, advanced(a, "shipping"), 6)
		})
		require.ErrorIs(t, err, forma.ErrPreconditionFailed)
		e.requireNoTrace(t, a, entries)
	})

	t.Run("a fenced CAS failure returned by fn rolls back the whole transaction (scenario 1, fence variant)", func(t *testing.T) {
		e := newEnv(t)
		other := e.seedInstance(t, 3)
		err := e.scenarioOne(t, func(uow UnitOfWork, _ *Instance) error {
			return uow.Instances().FenceVersion(e.ctx, other.ID, 2)
		})
		require.ErrorIs(t, err, forma.ErrPreconditionFailed)
	})
}

// scenarioOne runs special scenario 1: InTx{update instance A; create task
// B; create outbox C; last}, where last fails. It asserts that nothing was
// committed and A's change_log is as it was, and answers InTx's error.
func (e *env) scenarioOne(t *testing.T, last func(UnitOfWork, *Instance) error) error {
	t.Helper()
	a := e.seedInstance(t, 7)
	entries := e.changeLogEntries(t, InstanceSchema, a.ID)
	err := e.inTx(func(uow UnitOfWork) error {
		next := advanced(a, "shipping")
		if err := uow.Instances().Update(e.ctx, next, 7); err != nil {
			return err
		}
		if err := uow.Tasks().Insert(e.ctx, task(next, "hash-b", TaskPending, "shipped", PurposeWorker)); err != nil {
			return err
		}
		if err := uow.Outbox().Insert(e.ctx, []OutboxMessage{message("partition-a", time.Now())}); err != nil {
			return err
		}
		return last(uow, a)
	})
	require.Error(t, err)
	e.requireNoTrace(t, a, entries)
	return err
}

// requireNoTrace asserts a failed transaction left nothing: instance a is
// at version 7 in state start with the change_log entries it had, and no
// task, outbox message or history event exists, nor any change_log entry
// of one.
func (e *env) requireNoTrace(t *testing.T, a *Instance, entries [][2]int64) {
	t.Helper()
	require.Equal(t, map[string]any{"state": "start", "version": int64(7)}, e.stored(t, InstanceSchema, a.ID, "state", "version"))
	require.Equal(t, entries, e.changeLogEntries(t, InstanceSchema, a.ID), "instance A's change_log is as it was")
	for _, schema := range []string{TaskSchema, OutboxSchema, HistorySchema} {
		require.Zero(t, e.liveRows(t, schema), "no %s row was committed", schema)
		require.Zero(t, e.changeLogRows(t, schema), "no %s change_log entry was committed", schema)
	}
}

// advanced answers inst moved to state at the next version.
func advanced(inst *Instance, state string) *Instance {
	next := *inst
	next.State, next.Version = state, inst.Version+1
	return &next
}

const injectedFailure = "injected storage failure"

// failInserts makes every insert of a schema's entity_main row fail, a
// storage failure injected below Forma with a trigger on the test's table.
func (e *env) failInserts(t *testing.T, schema string) {
	t.Helper()
	fn := e.app + "_fail_inserts"
	_, err := e.pool.Exec(e.ctx, fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION '%s'; END $$`, fn, injectedFailure))
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = e.pool.Exec(e.ctx, "DROP FUNCTION IF EXISTS "+fn+" CASCADE") })
	_, err = e.pool.Exec(e.ctx, fmt.Sprintf(`CREATE TRIGGER %s BEFORE INSERT ON %s FOR EACH ROW
		WHEN (NEW.ltbase_schema_id = %d) EXECUTE FUNCTION %s()`, fn, e.tables.entityMain, schemaIDs[schema], fn))
	require.NoError(t, err)
}
