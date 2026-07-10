package production

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	forma "github.com/lychee-technology/forma"
)

// EventKind enumerates the entity lifecycle operations the harness drives
// through the real EntityManager.
type EventKind string

const (
	EventCreate EventKind = "create"
	EventUpdate EventKind = "update"
	EventDelete EventKind = "delete"
)

// Event is one entity mutation. ApplyEvents fills Seq, RowID (for creates),
// and the wall-clock ChangedAt/DeletedAt read back from change_log — the
// EntityManager stamps its own timestamps, so a seed alone cannot fix them
// and the oracle needs the actual values.
type Event struct {
	Kind   EventKind      `json:"kind"`
	Schema SchemaRef      `json:"schema"`
	RowID  uuid.UUID      `json:"row_id"`
	Attrs  map[string]any `json:"attrs,omitempty"`

	Seq       int   `json:"seq"`
	ChangedAt int64 `json:"changed_at"`
	DeletedAt int64 `json:"deleted_at"`
}

// CreateEvent builds a create event; the row ID is assigned by the
// EntityManager during ApplyEvents.
func CreateEvent(schema SchemaRef, attrs map[string]any) *Event {
	return &Event{Kind: EventCreate, Schema: schema, Attrs: attrs}
}

// UpdateEvent builds a partial-update event for an existing row.
func UpdateEvent(schema SchemaRef, rowID uuid.UUID, attrs map[string]any) *Event {
	return &Event{Kind: EventUpdate, Schema: schema, RowID: rowID, Attrs: attrs}
}

// DeleteEvent builds a soft-delete event for an existing row.
func DeleteEvent(schema SchemaRef, rowID uuid.UUID) *Event {
	return &Event{Kind: EventDelete, Schema: schema, RowID: rowID}
}

// ApplyEvents drives each event through the real EntityManager
// (Create/Update/Delete), which maintains entity_main, eav_data, and
// change_log exactly like production, then reads the resulting change_log
// timestamps back into the event and appends it to the Env's event log.
func (e *Env) ApplyEvents(ctx context.Context, events ...*Event) error {
	manager := e.EntityManager()
	for _, ev := range events {
		e.eventSeq++
		ev.Seq = e.eventSeq

		if err := applyEvent(ctx, manager, ev); err != nil {
			return fmt.Errorf("apply event seq=%d kind=%s schema=%s: %w", ev.Seq, ev.Kind, ev.Schema.Name, err)
		}
		if err := e.readBackTimestamps(ctx, ev); err != nil {
			return fmt.Errorf("read back timestamps for seq=%d row=%s: %w", ev.Seq, ev.RowID, err)
		}
		e.events = append(e.events, ev)
	}
	return nil
}

func applyEvent(ctx context.Context, manager forma.EntityManager, ev *Event) error {
	switch ev.Kind {
	case EventCreate:
		record, err := manager.Create(ctx, &forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: ev.Schema.Name},
			Type:             forma.OperationCreate,
			Data:             ev.Attrs,
		})
		if err != nil {
			return fmt.Errorf("entity manager create: %w", err)
		}
		ev.RowID = record.RowID
		return nil
	case EventUpdate:
		if _, err := manager.Update(ctx, &forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: ev.Schema.Name, RowID: ev.RowID},
			Type:             forma.OperationUpdate,
			Updates:          ev.Attrs,
		}); err != nil {
			return fmt.Errorf("entity manager update: %w", err)
		}
		return nil
	case EventDelete:
		if err := manager.Delete(ctx, &forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: ev.Schema.Name, RowID: ev.RowID},
			Type:             forma.OperationDelete,
		}); err != nil {
			return fmt.Errorf("entity manager delete: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unknown event kind %q", ev.Kind)
	}
}

// readBackTimestamps loads the unflushed change_log entry the write produced.
func (e *Env) readBackTimestamps(ctx context.Context, ev *Event) error {
	row := e.Pool.QueryRow(ctx,
		`SELECT changed_at, COALESCE(deleted_at, 0)
		 FROM change_log
		 WHERE schema_id = $1 AND row_id = $2 AND flushed_at = 0`,
		ev.Schema.ID, ev.RowID)
	if err := row.Scan(&ev.ChangedAt, &ev.DeletedAt); err != nil {
		return fmt.Errorf("scan change_log entry: %w", err)
	}
	return nil
}

// Events returns the Env's applied event log in order.
func (e *Env) Events() []*Event {
	return e.events
}

// ExecSQL is the failure-matrix escape hatch (for #179/#181): it executes
// raw SQL against the per-test database and fails the test on error. Use it
// to inject states production code cannot produce through its API (e.g.
// truncating change_log to model post-init onboarding).
func (e *Env) ExecSQL(ctx context.Context, sql string, args ...any) {
	e.T.Helper()
	if _, err := e.Pool.Exec(ctx, sql, args...); err != nil {
		e.T.Fatalf("ExecSQL %q: %v", sql, err)
	}
}
