package coordinationtest

import (
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// LTFlow's idempotency contract: a failed request may be retried with the
// same request, nothing else replaces a record, and a completed record is
// never overwritten.

func TestIdempotency(t *testing.T) {
	setup := func(t *testing.T, status string) (*env, *EventRecord) {
		e := newEnv(t)
		inst := e.seedInstance(t, 1)
		r := &EventRecord{InstanceID: inst.ID, IdempotencyKey: "approve-1", RequestHash: "h1", Event: "approve",
			Status: status, ErrorCode: "timeout"}
		e.upsert(t, r, forma.UpsertCreated)
		e.requireCommitted(t, r)
		return e, r
	}
	retry := func(r *EventRecord, hash, status string) *EventRecord {
		next := *r
		next.RequestHash, next.Status, next.ErrorCode, next.ResultState, next.ResultVersion = hash, status, "", "approved", 2
		return &next
	}

	t.Run("a failed record with the same request hash is updated", func(t *testing.T) {
		e, r := setup(t, RecordFailed)
		before := e.stamps(t, IdempotencySchema, r.ID)
		next := retry(r, "h1", RecordSucceeded)
		e.upsert(t, next, forma.UpsertUpdated)
		require.Equal(t, r.ID, next.ID, "the update answers the existing record")
		require.Equal(t, RecordSucceeded, next.Status)
		require.Equal(t, "approved", next.ResultState)
		e.requireCommitted(t, next)
		require.Equal(t, next, e.getRecord(t, r), "Get reads the committed record")
		e.requireWritten(t, IdempotencySchema, before, r.ID)
	})

	t.Run("a failed record with a different request hash is ErrPreconditionFailed", func(t *testing.T) {
		e, r := setup(t, RecordFailed)
		entries := e.changeLogEntries(t, IdempotencySchema, r.ID)
		_, err := e.tryUpsert(retry(r, "h2", RecordSucceeded))
		require.ErrorIs(t, err, forma.ErrPreconditionFailed)
		require.Equal(t, map[string]any{"status": RecordFailed, "request_hash": "h1"},
			e.stored(t, IdempotencySchema, r.ID, "status", "request_hash"))
		require.Equal(t, entries, e.changeLogEntries(t, IdempotencySchema, r.ID))
	})

	t.Run("a completed record is never overwritten", func(t *testing.T) {
		e, r := setup(t, RecordSucceeded)
		entries := e.changeLogEntries(t, IdempotencySchema, r.ID)
		for _, next := range []*EventRecord{retry(r, "h1", RecordFailed), retry(r, "h1", RecordSucceeded), retry(r, "h2", RecordSucceeded)} {
			_, err := e.tryUpsert(next)
			require.ErrorIs(t, err, forma.ErrPreconditionFailed, "hash %s status %s", next.RequestHash, next.Status)
		}
		require.Equal(t, map[string]any{"status": RecordSucceeded, "request_hash": "h1"},
			e.stored(t, IdempotencySchema, r.ID, "status", "request_hash"))
		require.Equal(t, entries, e.changeLogEntries(t, IdempotencySchema, r.ID))
	})

	t.Run("retention deletes eligible records oldest first, up to the limit", func(t *testing.T) {
		e := newEnv(t)
		inst := e.seedInstance(t, 1)
		now := time.Now()
		var records []*EventRecord
		for i := 0; i < 4; i++ {
			at := now.Add(time.Duration(i-5) * time.Hour)
			if i == 3 {
				at = now.Add(time.Hour)
			}
			r := &EventRecord{InstanceID: inst.ID, IdempotencyKey: string(rune('a' + i)), RequestHash: "h", Status: RecordSucceeded,
				EligibleCleanupAt: &at}
			e.upsert(t, r, forma.UpsertCreated)
			e.requireCommitted(t, r)
			records = append(records, r)
		}
		var deleted int
		require.NoError(t, e.inTx(func(uow UnitOfWork) error {
			var err error
			deleted, err = uow.Idempotency().DeleteEligible(e.ctx, now, 2)
			return err
		}))
		require.Equal(t, 2, deleted)
		require.Nil(t, e.stored(t, IdempotencySchema, records[0].ID, "status"), "the oldest eligible record is deleted")
		require.Nil(t, e.stored(t, IdempotencySchema, records[1].ID, "status"))
		require.NotNil(t, e.stored(t, IdempotencySchema, records[2].ID, "status"), "the limit leaves the third eligible record")
		require.NotNil(t, e.stored(t, IdempotencySchema, records[3].ID, "status"), "a record not yet eligible stays")
	})
}

// upsert upserts r in a transaction of its own and asserts its outcome.
func (e *env) upsert(t *testing.T, r *EventRecord, want forma.UpsertOutcome) {
	t.Helper()
	got, err := e.tryUpsert(r)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// requireCommitted asserts that r, as Upsert set it, equals the committed
// record, read by direct SQL.
func (e *env) requireCommitted(t *testing.T, r *EventRecord) {
	t.Helper()
	main := e.stored(t, IdempotencySchema, r.ID, "instance_id", "idempotency_key", "request_hash", "status", "eligible_cleanup_at")
	require.NotNil(t, main, "record %s is live", r.ID)
	eav := e.eavValues(t, IdempotencySchema, r.ID)
	committed := &EventRecord{ID: r.ID, InstanceID: uuid.MustParse(main["instance_id"].(string)),
		IdempotencyKey: main["idempotency_key"].(string), RequestHash: main["request_hash"].(string), Status: main["status"].(string),
		Event: eav["event"], ResultState: eav["result_state"], ErrorCode: eav["error_code"]}
	if v, ok := eav["result_version"]; ok {
		var err error
		committed.ResultVersion, err = strconv.ParseInt(v, 10, 64)
		require.NoError(t, err)
	}
	returned := *r
	returned.EligibleCleanupAt = nil
	require.Equal(t, committed, &returned, "returned equals committed")
	if r.EligibleCleanupAt == nil {
		require.Nil(t, main["eligible_cleanup_at"])
		return
	}
	require.Equal(t, r.EligibleCleanupAt.UnixMilli(), main["eligible_cleanup_at"])
}

func (e *env) tryUpsert(r *EventRecord) (forma.UpsertOutcome, error) {
	var outcome forma.UpsertOutcome
	err := e.inTx(func(uow UnitOfWork) error {
		var err error
		outcome, err = uow.Idempotency().Upsert(e.ctx, r)
		return err
	})
	return outcome, err
}

// getRecord reads r's record through the adapter.
func (e *env) getRecord(t *testing.T, r *EventRecord) *EventRecord {
	t.Helper()
	var got *EventRecord
	require.NoError(t, e.inTx(func(uow UnitOfWork) error {
		var err error
		got, err = uow.Idempotency().Get(e.ctx, r.InstanceID, r.IdempotencyKey)
		return err
	}))
	require.NotNil(t, got)
	return got
}
