package coordinationtest

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// IdempotencyStore manages event idempotency records. LTFlow:
// repository.IdempotencyStore.
type IdempotencyStore interface {
	Upsert(ctx context.Context, r *EventRecord) (forma.UpsertOutcome, error)
	Get(ctx context.Context, instanceID uuid.UUID, key string) (*EventRecord, error)
	DeleteEligible(ctx context.Context, before time.Time, limit int) (int, error)
}

type idempotencyStore struct {
	tx forma.Tx
}

// Upsert writes a request's outcome with Tx.Upsert on the unique key
// idempotency, (instance_id, idempotency_key) (#651). It creates the
// record, or replaces an existing one only when that record is failed and
// has the same request hash, the precondition status = failed AND
// request_hash = h (#649). Any other existing record is left as it is and
// answered ErrPreconditionFailed, LTFlow's domain.ErrConcurrencyConflict.
// It answers created or updated, which LTFlow's Upsert does not return,
// and sets *r to the record as committed. LTFlow: IdempotencyStore.Upsert.
func (s *idempotencyStore) Upsert(ctx context.Context, r *EventRecord) (forma.UpsertOutcome, error) {
	data := map[string]any{
		"instance_id": r.InstanceID.String(), "idempotency_key": r.IdempotencyKey, "request_hash": r.RequestHash,
		"event": r.Event, "status": r.Status, "result_state": r.ResultState, "result_version": r.ResultVersion,
		"error_code": r.ErrorCode,
	}
	setInstant(data, "eligible_cleanup_at", r.EligibleCleanupAt)
	// The replacement keeps the key attributes, which name the record.
	updates := make(map[string]any, len(data))
	for k, v := range data {
		if k != "instance_id" && k != "idempotency_key" {
			updates[k] = v
		}
	}
	result, err := s.tx.Upsert(ctx, &forma.UpsertRequest{
		Create:        forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: IdempotencySchema}, Data: data},
		Target:        forma.ConflictTarget{Key: "idempotency"},
		Action:        forma.UpsertUpdate,
		Preconditions: all(equals("status", RecordFailed), equals("request_hash", r.RequestHash)),
		Updates:       updates,
	})
	if err != nil {
		return "", fmt.Errorf("upsert the record of instance %s key %s: %w", r.InstanceID, r.IdempotencyKey, err)
	}
	*r = *eventRecordOf(result.Record)
	return result.Outcome, nil
}

// Get reads the record of (instanceID, key) with Tx.Query (#650), or nil
// when there is none. LTFlow: IdempotencyStore.Get.
func (s *idempotencyStore) Get(ctx context.Context, instanceID uuid.UUID, key string) (*EventRecord, error) {
	records, err := queryAll(ctx, s.tx, IdempotencySchema, all(equals("instance_id", instanceID), equals("idempotency_key", key)))
	if err != nil {
		return nil, fmt.Errorf("get the record of instance %s key %s: %w", instanceID, key, err)
	}
	if len(records) == 0 {
		return nil, nil
	}
	return eventRecordOf(records[0]), nil
}

func eventRecordOf(r *forma.DataRecord) *EventRecord {
	return &EventRecord{
		ID: r.RowID, InstanceID: id(r, "instance_id"), IdempotencyKey: text(r, "idempotency_key"),
		RequestHash: text(r, "request_hash"), Event: text(r, "event"), Status: text(r, "status"),
		ResultState: text(r, "result_state"), ResultVersion: whole(r, "result_version"), ErrorCode: text(r, "error_code"),
		EligibleCleanupAt: instantPtr(r, "eligible_cleanup_at"),
	}
}

// DeleteEligible deletes up to limit records whose eligible_cleanup_at is
// at or before before, oldest first, with Tx.DeleteWhere (#652), and
// answers how many it deleted. LTFlow: IdempotencyStore.DeleteEligible.
func (s *idempotencyStore) DeleteEligible(ctx context.Context, before time.Time, limit int) (int, error) {
	result, err := s.tx.DeleteWhere(ctx, &forma.MutateWhereRequest{SchemaName: IdempotencySchema,
		Condition: &forma.KvCondition{Attr: "eligible_cleanup_at", Value: "lte:" + rfc3339(before)},
		Sort:      []forma.OrderBy{{Attribute: "eligible_cleanup_at"}}, Limit: limit})
	if err != nil {
		return 0, fmt.Errorf("delete records eligible before %s: %w", rfc3339(before), err)
	}
	return len(result.RowIDs), nil
}
