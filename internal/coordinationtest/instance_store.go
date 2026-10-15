package coordinationtest

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// InstanceStore manages workflow instances. LTFlow: repository.InstanceStore.
type InstanceStore interface {
	Create(ctx context.Context, inst *Instance) error
	Get(ctx context.Context, id uuid.UUID) (*Instance, error)
	ListByFilter(ctx context.Context, workflowName, status string) ([]Instance, error)
	Update(ctx context.Context, inst *Instance, expectedVersion int64) error
	FenceVersion(ctx context.Context, id uuid.UUID, version int64) error
	RecordEvent(ctx context.Context, id uuid.UUID) (int64, error)
}

type instanceStore struct {
	tx forma.Tx
}

// Create inserts a new instance under its ID with Tx.Create and a caller
// row ID (#651). An instance with the ID is ErrUniqueConflict, LTFlow's
// domain.ErrInstanceAlreadyExists. LTFlow: InstanceStore.Create.
func (s *instanceStore) Create(ctx context.Context, inst *Instance) error {
	_, err := create(ctx, s.tx, InstanceSchema, inst.ID, instanceData(inst))
	if err != nil {
		return fmt.Errorf("create instance %s: %w", inst.ID, err)
	}
	return nil
}

// Get reads an instance with Tx.Get (#650); a missing one is ErrNotFound.
// LTFlow: InstanceStore.Get.
func (s *instanceStore) Get(ctx context.Context, id uuid.UUID) (*Instance, error) {
	record, err := s.tx.Get(ctx, &forma.QueryRequest{SchemaName: InstanceSchema, RowID: &id})
	if err != nil {
		return nil, fmt.Errorf("get instance %s: %w", id, err)
	}
	return instanceOf(record), nil
}

// ListByFilter answers the instances of workflowName in status with
// Tx.Query (#650) and the condition DSL. LTFlow: InstanceStore.ListByFilter.
func (s *instanceStore) ListByFilter(ctx context.Context, workflowName, status string) ([]Instance, error) {
	records, err := queryAll(ctx, s.tx, InstanceSchema, all(equals("workflow_name", workflowName), equals("status", status)))
	if err != nil {
		return nil, fmt.Errorf("list instances of %s in %s: %w", workflowName, status, err)
	}
	instances := make([]Instance, len(records))
	for i, r := range records {
		instances[i] = *instanceOf(r)
	}
	return instances, nil
}

// Update persists inst with Tx.Update and the precondition version equals
// expectedVersion (#649). An instance past it is ErrPreconditionFailed,
// LTFlow's domain.ErrConcurrencyConflict. LTFlow: InstanceStore.Update.
func (s *instanceStore) Update(ctx context.Context, inst *Instance, expectedVersion int64) error {
	updates := instanceData(inst)
	delete(updates, "workflow_name")
	delete(updates, "event_count")
	_, err := s.tx.Update(ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: InstanceSchema, RowID: inst.ID},
		Preconditions:    equals("version", expectedVersion),
		Updates:          updates,
	})
	if err != nil {
		return fmt.Errorf("update instance %s from version %d: %w", inst.ID, expectedVersion, err)
	}
	return nil
}

// FenceVersion makes the transaction's commit conditional on the instance
// still being at version, with Tx.AssertRow (#650). An instance past it is
// ErrPreconditionFailed, a missing one ErrNotFound. LTFlow:
// InstanceStore.FenceVersion.
func (s *instanceStore) FenceVersion(ctx context.Context, id uuid.UUID, version int64) error {
	if err := s.tx.AssertRow(ctx, InstanceSchema, id, equals("version", version)); err != nil {
		return fmt.Errorf("fence instance %s at version %d: %w", id, version, err)
	}
	return nil
}

// RecordEvent counts one more event on a running instance with Tx.Update,
// the precondition status equals running (#649) and Increment(event_count),
// and answers the new count. It is the suite's lost-update probe for a
// conditional mutation, not an LTFlow method.
func (s *instanceStore) RecordEvent(ctx context.Context, id uuid.UUID) (int64, error) {
	record, err := s.tx.Update(ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: InstanceSchema, RowID: id},
		Preconditions:    equals("status", "running"),
		UpdateOps:        []forma.UpdateOp{forma.Increment("event_count", 1)},
	})
	if err != nil {
		return 0, fmt.Errorf("record an event on instance %s: %w", id, err)
	}
	return whole(record, "event_count"), nil
}

func instanceData(inst *Instance) map[string]any {
	data := map[string]any{
		"workflow_name": inst.WorkflowName, "state": inst.State, "status": inst.Status,
		"business_reference": inst.BusinessReference, "payload": inst.Payload,
		"version": inst.Version, "event_count": inst.EventCount,
	}
	setInstant(data, "eligible_cleanup_at", inst.EligibleCleanupAt)
	return data
}

func instanceOf(r *forma.DataRecord) *Instance {
	return &Instance{
		ID: r.RowID, WorkflowName: text(r, "workflow_name"), State: text(r, "state"), Status: text(r, "status"),
		BusinessReference: text(r, "business_reference"), Payload: text(r, "payload"),
		Version: whole(r, "version"), EventCount: whole(r, "event_count"),
		EligibleCleanupAt: instantPtr(r, "eligible_cleanup_at"),
	}
}
