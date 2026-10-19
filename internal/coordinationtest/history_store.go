package coordinationtest

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// HistoryStore manages workflow history events. LTFlow:
// repository.HistoryStore, plus RetentionStore.MarkHistoryArchived.
type HistoryStore interface {
	Insert(ctx context.Context, events []HistoryEvent) error
	UpsertDeduped(ctx context.Context, evt HistoryEvent, dedupKey string, window time.Duration) error
	MarkArchived(ctx context.Context, instanceID uuid.UUID, archivedAt time.Time) (int, error)
}

type historyStore struct {
	tx forma.Tx
}

// dedupNamespace derives a deduplicated event's row ID from its dedup key,
// as LTFlow uses the key as the row's primary key.
var dedupNamespace = uuid.MustParse("6f1d4c52-3b7a-4c55-9d3e-654000000654")

// DedupRowID answers the row ID of the deduplicated event of dedupKey.
func DedupRowID(dedupKey string) uuid.UUID {
	return uuid.NewSHA1(dedupNamespace, []byte(dedupKey))
}

// Insert appends events with one Tx.Create each, under each event's ID
// unless it is uuid.Nil, and sets every ID to its row's. LTFlow:
// HistoryStore.Insert.
func (s *historyStore) Insert(ctx context.Context, events []HistoryEvent) error {
	for i := range events {
		record, err := create(ctx, s.tx, HistorySchema, events[i].ID, historyData(&events[i]))
		if err != nil {
			return fmt.Errorf("insert history event %d of %d: %w", i+1, len(events), err)
		}
		events[i].ID = record.RowID
	}
	return nil
}

// UpsertDeduped inserts evt under the row ID of dedupKey or, when that row
// was recorded within window of the transaction's clock, increments its
// dedup_count: Tx.Upsert on the row ID with the precondition recorded_at
// gte $now-window and Increment(dedup_count) (#651, #649, #646). An
// existing row outside the window is left as it is, silently, as LTFlow's
// ON CONFLICT ... WHERE leaves it. LTFlow: HistoryStore.UpsertDeduped.
func (s *historyStore) UpsertDeduped(ctx context.Context, evt HistoryEvent, dedupKey string, window time.Duration) error {
	evt.DedupCount = 0
	_, err := s.tx.Upsert(ctx, &forma.UpsertRequest{
		Create: forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: HistorySchema, RowID: DedupRowID(dedupKey)},
			Data: historyData(&evt)},
		Target:        forma.ConflictTarget{RowID: true},
		Action:        forma.UpsertUpdate,
		Preconditions: forma.Now("recorded_at", forma.FilterGreaterEq, -window),
		UpdateOps:     []forma.UpdateOp{forma.Increment("dedup_count", 1)},
	})
	if errors.Is(err, forma.ErrPreconditionFailed) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("upsert the history event of dedup key %s: %w", dedupKey, err)
	}
	return nil
}

// MarkArchived stamps archived_at on the instance's history with one
// Tx.UpdateWhere (#652), and answers how many events it stamped. LTFlow:
// RetentionStore.MarkHistoryArchived.
func (s *historyStore) MarkArchived(ctx context.Context, instanceID uuid.UUID, archivedAt time.Time) (int, error) {
	result, err := s.tx.UpdateWhere(ctx, &forma.MutateWhereRequest{SchemaName: HistorySchema,
		Condition: equals("instance_id", instanceID), Updates: map[string]any{"archived_at": rfc3339(archivedAt)}})
	if err != nil {
		return 0, fmt.Errorf("mark the history of instance %s archived: %w", instanceID, err)
	}
	return len(result.RowIDs), nil
}

func historyData(evt *HistoryEvent) map[string]any {
	recorded := evt.RecordedAt
	if recorded.IsZero() {
		recorded = time.Now()
	}
	return map[string]any{
		"instance_id": evt.InstanceID.String(), "type": evt.Type, "state_from": evt.StateFrom, "state_to": evt.StateTo,
		"event": evt.Event, "details": evt.Details, "dedup_count": evt.DedupCount, "recorded_at": rfc3339(recorded),
	}
}
