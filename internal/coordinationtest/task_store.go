package coordinationtest

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// TaskStore manages workflow tasks. LTFlow: repository.TaskStore, plus the
// task half of retention.
type TaskStore interface {
	Insert(ctx context.Context, t *Task) error
	GetByTokenHash(ctx context.Context, h string) (*Task, error)
	UpdateStatus(ctx context.Context, h, from, to string) error
	CancelPendingForInstance(ctx context.Context, instanceID uuid.UUID) ([]Task, error)
	CompleteMatchingAndCancelPending(ctx context.Context, instanceID uuid.UUID, state string, version int64, event string) (completed, cancelled []Task, err error)
	MarkResolvedForInstanceEligible(ctx context.Context, instanceID uuid.UUID, at time.Time) error
	DeleteEligible(ctx context.Context, before time.Time, limit int) (int, error)
}

type taskStore struct {
	tx forma.Tx
}

// Insert persists a new task with Tx.Create; its token hash is the unique
// key token_hash (#651), so a second task with the hash is
// ErrUniqueConflict. t.ID is set to the new row's ID. LTFlow:
// TaskStore.Insert.
func (s *taskStore) Insert(ctx context.Context, t *Task) error {
	data := map[string]any{
		"token_hash": t.TokenHash, "instance_id": t.InstanceID.String(), "state": t.State, "status": t.Status,
		"expected_event": t.ExpectedEvent, "purpose": t.Purpose, "version": t.Version,
	}
	setInstant(data, "eligible_cleanup_at", t.EligibleCleanupAt)
	record, err := create(ctx, s.tx, TaskSchema, t.ID, data)
	if err != nil {
		return fmt.Errorf("insert task %s: %w", t.TokenHash, err)
	}
	t.ID = record.RowID
	return nil
}

// GetByTokenHash reads the task with token hash h with Tx.Query (#650); no
// such task is ErrNotFound. LTFlow: TaskStore.GetByTokenHash.
func (s *taskStore) GetByTokenHash(ctx context.Context, h string) (*Task, error) {
	records, err := queryAll(ctx, s.tx, TaskSchema, equals("token_hash", h))
	if err != nil {
		return nil, fmt.Errorf("get task %s: %w", h, err)
	}
	if len(records) == 0 {
		return nil, forma.NotFoundf("no task has token hash %s", h)
	}
	return taskOf(records[0]), nil
}

// UpdateStatus moves the task with token hash h from status from to to with
// one Tx.UpdateWhere on token_hash = h AND status = from (#652). When no
// task matches, it is ErrPreconditionFailed: LTFlow's "token not found or
// status mismatch". LTFlow: TaskStore.UpdateStatus.
func (s *taskStore) UpdateStatus(ctx context.Context, h, from, to string) error {
	req := &forma.MutateWhereRequest{SchemaName: TaskSchema,
		Condition: all(equals("token_hash", h), equals("status", from)), Updates: map[string]any{"status": to}}
	if resolves(to) {
		req.UpdateOps = []forma.UpdateOp{forma.ServerTime("resolved_at", 0)}
	}
	result, err := s.tx.UpdateWhere(ctx, req)
	if err != nil {
		return fmt.Errorf("move task %s from %s to %s: %w", h, from, to, err)
	}
	if len(result.RowIDs) == 0 {
		return forma.PreconditionFailedf("task %s is not %s, or does not exist", h, from)
	}
	return nil
}

// CancelPendingForInstance cancels every pending task of the instance with
// one Tx.UpdateWhere with Returning (#652), and answers the cancelled tasks
// as stored. LTFlow: TaskStore.CancelPendingForInstance (named
// CancelPendingForInstanceVersion when #654 was written).
func (s *taskStore) CancelPendingForInstance(ctx context.Context, instanceID uuid.UUID) ([]Task, error) {
	tasks, err := s.resolveWhere(ctx, all(equals("instance_id", instanceID), equals("status", TaskPending)), TaskCancelled)
	if err != nil {
		return nil, fmt.Errorf("cancel the pending tasks of instance %s: %w", instanceID, err)
	}
	return tasks, nil
}

// CompleteMatchingAndCancelPending completes the pending worker tasks of the
// visit (state, version) that expect event, then cancels every other
// pending task of the instance: two Tx.UpdateWhere calls in one Tx (#652).
// LTFlow: TaskStore.CompleteMatchingAndCancelPending.
func (s *taskStore) CompleteMatchingAndCancelPending(ctx context.Context, instanceID uuid.UUID, state string, version int64, event string) (completed, cancelled []Task, err error) {
	matching := all(equals("instance_id", instanceID), equals("state", state), equals("version", version),
		equals("status", TaskPending), equals("expected_event", event), equals("purpose", PurposeWorker))
	completed, err = s.resolveWhere(ctx, matching, TaskCompleted)
	if err != nil {
		return nil, nil, fmt.Errorf("complete the tasks of instance %s at %s/%d: %w", instanceID, state, version, err)
	}
	cancelled, err = s.CancelPendingForInstance(ctx, instanceID)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve the tasks of instance %s: %w", instanceID, err)
	}
	return completed, cancelled, nil
}

// MarkResolvedForInstanceEligible stamps eligible_cleanup_at on the
// instance's resolved tasks with Tx.UpdateWhere (#652). LTFlow:
// TaskStore.MarkResolvedForInstanceEligible.
func (s *taskStore) MarkResolvedForInstanceEligible(ctx context.Context, instanceID uuid.UUID, at time.Time) error {
	_, err := s.tx.UpdateWhere(ctx, &forma.MutateWhereRequest{SchemaName: TaskSchema,
		Condition: all(equals("instance_id", instanceID), forma.In("status", TaskCompleted, TaskCancelled, TaskExpired)),
		Updates:   map[string]any{"eligible_cleanup_at": rfc3339(at)}})
	if err != nil {
		return fmt.Errorf("mark the resolved tasks of instance %s eligible: %w", instanceID, err)
	}
	return nil
}

// DeleteEligible deletes up to limit tasks whose eligible_cleanup_at is at
// or before before, oldest first, with Tx.DeleteWhere (#652), and answers
// how many it deleted. LTFlow: RetentionStore.DeleteEligibleTasks.
func (s *taskStore) DeleteEligible(ctx context.Context, before time.Time, limit int) (int, error) {
	result, err := s.tx.DeleteWhere(ctx, &forma.MutateWhereRequest{SchemaName: TaskSchema,
		Condition: &forma.KvCondition{Attr: "eligible_cleanup_at", Value: "lte:" + rfc3339(before)},
		Sort:      []forma.OrderBy{{Attribute: "eligible_cleanup_at"}}, Limit: limit})
	if err != nil {
		return 0, fmt.Errorf("delete tasks eligible before %s: %w", rfc3339(before), err)
	}
	return len(result.RowIDs), nil
}

// resolveWhere moves every task matching cond to the resolved status to,
// stamping resolved_at, and answers them as stored.
func (s *taskStore) resolveWhere(ctx context.Context, cond forma.Condition, to string) ([]Task, error) {
	result, err := s.tx.UpdateWhere(ctx, &forma.MutateWhereRequest{SchemaName: TaskSchema, Condition: cond,
		Updates: map[string]any{"status": to}, UpdateOps: []forma.UpdateOp{forma.ServerTime("resolved_at", 0)}, Returning: true})
	if err != nil {
		return nil, fmt.Errorf("move tasks to %s: %w", to, err)
	}
	tasks := make([]Task, len(result.Records))
	for i, r := range result.Records {
		tasks[i] = *taskOf(r)
	}
	return tasks, nil
}

// resolves reports whether status is a resolved task status.
func resolves(status string) bool {
	return status == TaskCompleted || status == TaskCancelled || status == TaskExpired
}

func taskOf(r *forma.DataRecord) *Task {
	return &Task{
		ID: r.RowID, TokenHash: text(r, "token_hash"), InstanceID: id(r, "instance_id"), State: text(r, "state"),
		Version: whole(r, "version"), Status: text(r, "status"), ExpectedEvent: text(r, "expected_event"),
		Purpose: text(r, "purpose"), ResolvedAt: instantPtr(r, "resolved_at"), EligibleCleanupAt: instantPtr(r, "eligible_cleanup_at"),
	}
}
