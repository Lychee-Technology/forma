package coordinationtest

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// LinkStore manages parent-child workflow links. LTFlow:
// repository.WorkflowLinkStore.
type LinkStore interface {
	Insert(ctx context.Context, l *Link) error
	UpdateStatus(ctx context.Context, parentTaskHash, status string) error
	GetByParentTaskHash(ctx context.Context, h string) (*Link, error)
}

type linkStore struct {
	tx forma.Tx
}

// Insert persists a new link with Tx.Create; its parent task hash is the
// unique key parent_task_hash (#651), so a second link for the parent task
// is ErrUniqueConflict. l.ID is set to the new row's ID. LTFlow:
// WorkflowLinkStore.Insert.
func (s *linkStore) Insert(ctx context.Context, l *Link) error {
	data := map[string]any{"parent_task_hash": l.ParentTaskHash, "parent_instance_id": l.ParentInstanceID.String(), "status": l.Status}
	if l.ChildInstanceID != uuid.Nil {
		data["child_instance_id"] = l.ChildInstanceID.String()
	}
	record, err := create(ctx, s.tx, LinkSchema, l.ID, data)
	if err != nil {
		return fmt.Errorf("insert the link of parent task %s: %w", l.ParentTaskHash, err)
	}
	l.ID = record.RowID
	return nil
}

// UpdateStatus sets the status of the link of parentTaskHash with one
// Tx.UpdateWhere (#652). Like LTFlow's, it is no error when no link
// matches. LTFlow: WorkflowLinkStore.UpdateStatus.
func (s *linkStore) UpdateStatus(ctx context.Context, parentTaskHash, status string) error {
	_, err := s.tx.UpdateWhere(ctx, &forma.MutateWhereRequest{SchemaName: LinkSchema,
		Condition: equals("parent_task_hash", parentTaskHash), Updates: map[string]any{"status": status}})
	if err != nil {
		return fmt.Errorf("set the link of parent task %s to %s: %w", parentTaskHash, status, err)
	}
	return nil
}

// GetByParentTaskHash reads the link of parent task hash h with Tx.Query
// (#650); none is ErrNotFound. LTFlow: WorkflowLinkStore.GetByParentTaskHash.
func (s *linkStore) GetByParentTaskHash(ctx context.Context, h string) (*Link, error) {
	records, err := queryAll(ctx, s.tx, LinkSchema, equals("parent_task_hash", h))
	if err != nil {
		return nil, fmt.Errorf("get the link of parent task %s: %w", h, err)
	}
	if len(records) == 0 {
		return nil, forma.NotFoundf("no link has parent task hash %s", h)
	}
	r := records[0]
	return &Link{ID: r.RowID, ParentTaskHash: text(r, "parent_task_hash"), ParentInstanceID: id(r, "parent_instance_id"),
		ChildInstanceID: id(r, "child_instance_id"), Status: text(r, "status")}, nil
}
