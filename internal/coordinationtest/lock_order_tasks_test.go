package coordinationtest

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// taskRound is a lock-order round's task group: Upsert on the unique key
// token_hash (#651) against UpdateWhere and AssertRow over the same tasks.
// One key upsert finds a seeded task through the key table, then locks it
// as AssertRow does. The other creates under a token hash the round's
// creator gate holds an uncommitted entry for. On PostgreSQL it waits on
// that entry, and once the gate commits it locks and updates the gate's
// task instead.
type taskRound struct {
	e     *env
	state string
	tasks []*Task
	// created is the token hash of the task the creator gate creates.
	created  string
	before   map[uuid.UUID]int64
	upserted [2]forma.UpsertOutcome
	touched  [2][]uuid.UUID
}

// newTaskRound seeds two tasks of inst at a visit of the round's own.
func (e *env) newTaskRound(t *testing.T, inst *Instance, round int) *taskRound {
	t.Helper()
	visit := *inst
	visit.State = fmt.Sprintf("round-%d", round)
	r := &taskRound{e: e, state: visit.State, created: visit.State + "-t2", tasks: []*Task{
		task(&visit, visit.State+"-t0", TaskPending, "advance", PurposeWorker),
		task(&visit, visit.State+"-t1", TaskPending, "advance", PurposeTimer),
	}}
	e.seedTasks(t, r.tasks...)
	r.before = e.stamps(t, TaskSchema, taskIDs(r.tasks)...)
	return r
}

// holdCreated parks the creator gate: a transaction that has created the
// task under r.created with the key upsert, so it holds that token hash's
// key entry, uncommitted, until the round releases it.
func (r *taskRound) holdCreated(t *testing.T) *held {
	t.Helper()
	return r.e.holdTx(t, func(ctx context.Context, tx forma.Tx) error {
		outcome, err := r.upsertIn(ctx, tx, r.created)
		if err == nil && outcome != forma.UpsertCreated {
			return fmt.Errorf("the creator gate's upsert of %s answered %v, want %v", r.created, outcome, forma.UpsertCreated)
		}
		return err
	})
}

// workers are the group's five transactions: a key upsert that updates t0
// and one of the creator gate's token hash, an UpdateWhere over the visit's
// tasks in each token-hash order, and an AssertRow on t1.
func (r *taskRound) workers() []func() error {
	return []func() error{
		func() error { return r.upsert(r.tasks[0].TokenHash, &r.upserted[0]) },
		func() error { return r.upsert(r.created, &r.upserted[1]) },
		func() error { return r.touch(forma.SortOrderAsc, &r.touched[0]) },
		func() error { return r.touch(forma.SortOrderDesc, &r.touched[1]) },
		func() error {
			return r.e.store.tr.InTx(r.e.ctx, func(ctx context.Context, tx forma.Tx) error {
				return tx.AssertRow(ctx, TaskSchema, r.tasks[1].ID, equals("status", TaskPending))
			})
		},
	}
}

// requireCommitted asserts what the group's committed transactions did,
// given their errors in workers order and the creator gate's, and that
// each token hash names one row.
func (r *taskRound) requireCommitted(t *testing.T, errs []error, creatorErr error) {
	t.Helper()
	e := r.e
	if errs[0] == nil {
		require.Equal(t, forma.UpsertUpdated, r.upserted[0])
		require.Equal(t, map[string]any{"expected_event": "upserted"}, e.stored(t, TaskSchema, r.tasks[0].ID, "expected_event"))
	}
	if errs[1] == nil {
		want := forma.UpsertUpdated
		if creatorErr != nil {
			want = forma.UpsertCreated
		}
		require.Equal(t, want, r.upserted[1], "the second writer of token hash %s", r.created)
	}
	for i, rows := range r.touched {
		if errs[2+i] == nil {
			require.Subset(t, rows, taskIDs(r.tasks), "UpdateWhere %d touched every seeded task", i)
		}
	}
	if errs[0] == nil || errs[2] == nil || errs[3] == nil {
		e.requireWritten(t, TaskSchema, r.before, r.tasks[0].ID)
	}
	for _, h := range []string{r.tasks[0].TokenHash, r.tasks[1].TokenHash, r.created} {
		want := 1
		if h == r.created && errs[1] != nil && creatorErr != nil {
			want = 0
		}
		require.Len(t, e.liveIDs(t, TaskSchema, "token_hash", h), want, "token hash %s names %d rows", h, want)
	}
	e.requireOneHotEntryPerRow(t, TaskSchema)
}

// upsert upserts the visit's task with token hash h on the unique key in a
// transaction of its own; see upsertIn.
func (r *taskRound) upsert(h string, out *forma.UpsertOutcome) error {
	return r.e.store.tr.InTx(r.e.ctx, func(ctx context.Context, tx forma.Tx) error {
		var err error
		*out, err = r.upsertIn(ctx, tx, h)
		return err
	})
}

// upsertIn upserts the visit's task with token hash h on the unique key: it
// creates the task, or sets expected_event on the task that holds h.
func (r *taskRound) upsertIn(ctx context.Context, tx forma.Tx, h string) (forma.UpsertOutcome, error) {
	result, err := tx.Upsert(ctx, &forma.UpsertRequest{
		Create: forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: TaskSchema},
			Data: map[string]any{"token_hash": h, "instance_id": r.tasks[0].InstanceID.String(), "state": r.state,
				"status": TaskPending, "expected_event": "upserted", "purpose": PurposeWorker, "version": r.tasks[0].Version}},
		Target:  forma.ConflictTarget{Key: "token_hash"},
		Action:  forma.UpsertUpdate,
		Updates: map[string]any{"expected_event": "upserted"},
	})
	if err != nil {
		return "", err
	}
	return result.Outcome, nil
}

// touch stamps purpose on every task of the visit with one Tx.UpdateWhere,
// its candidates in token-hash order or the reverse.
func (r *taskRound) touch(order forma.SortOrder, out *[]uuid.UUID) error {
	return r.e.store.tr.InTx(r.e.ctx, func(ctx context.Context, tx forma.Tx) error {
		result, err := tx.UpdateWhere(ctx, &forma.MutateWhereRequest{SchemaName: TaskSchema, Condition: equals("state", r.state),
			Sort: []forma.OrderBy{{Attribute: "token_hash", SortOrder: order}}, Updates: map[string]any{"purpose": "touched " + string(order)}})
		if err != nil {
			return err
		}
		*out = result.RowIDs
		return nil
	})
}
