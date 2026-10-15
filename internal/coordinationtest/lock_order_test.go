package coordinationtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// Lock-order composition: Upsert (#651), UpdateWhere (#652), Claim (#653)
// and AssertRow (#650) run at once over the same outbox rows, and Upsert on
// a unique key, UpdateWhere and AssertRow over the same tasks, each in an
// InTx of its own. On PostgreSQL their lock rules give them one order, so
// none may wait in a cycle: a deadlock aborts one transaction with
// ErrRetryableConflict, and that is a lock-order bug.

const lockOrderRounds = 50

func TestLockOrderComposition(t *testing.T) {
	e := newEnv(t)
	inst := e.seedInstance(t, 1)
	retryable := 0
	for round := 0; round < lockOrderRounds; round++ {
		retryable += e.lockOrderRound(t, round, inst)
	}
	t.Logf("%d rounds, %d ErrRetryableConflict", lockOrderRounds, retryable)
	t.Run("mechanism: no ErrRetryableConflict", func(t *testing.T) {
		requirePostgresMechanisms(t)
		require.Zero(t, retryable)
	})
}

// lockOrderRound seeds a topic of its own, two partitions of two messages,
// and a task group, runs eight transactions over the messages and five over
// the tasks at once, checks what the committed ones did, and answers how
// many failed with ErrRetryableConflict.
func (e *env) lockOrderRound(t *testing.T, round int, inst *Instance) int {
	t.Helper()
	topic := fmt.Sprintf("round-%d", round)
	base := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	msgs := make([]OutboxMessage, 4)
	for i := range msgs {
		msgs[i] = message(fmt.Sprintf("%s-p%d", topic, i/2), base.Add(time.Duration(i)*time.Millisecond))
		msgs[i].Topic = topic
	}
	require.NoError(t, e.inTx(func(uow UnitOfWork) error { return uow.Outbox().Insert(e.ctx, msgs) }))
	before := e.stamps(t, OutboxSchema, ids(msgs)...)
	created := newID()
	tasks := e.newTaskRound(t, inst, round)

	claimed := make([][]OutboxMessage, 2)
	var touched [2][]uuid.UUID
	var upserted [2]forma.UpsertOutcome
	workers := []func() error{
		func() error { return e.claimTopic(topic, 0, &claimed[0]) },
		func() error { return e.claimTopic(topic, 1, &claimed[1]) },
		func() error { return e.touchTopic(topic, forma.SortOrderAsc, &touched[0]) },
		func() error { return e.touchTopic(topic, forma.SortOrderDesc, &touched[1]) },
		func() error { return e.upsertMessage(msgs[1].ID, message(msgs[1].PartitionKey, base), &upserted[0]) },
		func() error {
			m := message(msgs[3].PartitionKey, base.Add(time.Minute))
			m.Topic = topic
			return e.upsertMessage(created, m, &upserted[1])
		},
		func() error { return e.assertGeneration(msgs[0].ID) },
		func() error { return e.assertGeneration(msgs[3].ID) },
	}
	errs := e.raceWithin(t, time.Minute, append(workers, tasks.workers()...))

	retryable := 0
	for i, err := range errs {
		if forma.IsRetryable(err) {
			retryable++
			continue
		}
		require.NoError(t, err, "round %d transaction %d", round, i)
	}
	e.requireLockOrderRound(t, msgs, claimed, errs)
	tasks.requireCommitted(t, errs[len(workers):])
	if errs[4] == nil {
		require.Equal(t, forma.UpsertUpdated, upserted[0])
		require.Equal(t, map[string]any{"attempts": int64(1)}, e.stored(t, OutboxSchema, msgs[1].ID, "attempts"))
	}
	if errs[5] == nil {
		require.Equal(t, forma.UpsertCreated, upserted[1])
		require.NotNil(t, e.stored(t, OutboxSchema, created, "status"), "the upsert created its message")
	}
	for i, rows := range touched {
		if errs[2+i] == nil {
			require.Subset(t, rows, ids(msgs), "UpdateWhere %d touched every seeded message", i)
		}
	}
	if errs[2] == nil || errs[3] == nil {
		e.requireWritten(t, OutboxSchema, before, ids(msgs)...)
	}
	e.requireOneHotEntryPerRow(t, OutboxSchema)
	return retryable
}

// requireLockOrderRound asserts the round's claims: disjoint, partition
// heads only, and each message's generation counting its committed claims.
func (e *env) requireLockOrderRound(t *testing.T, msgs []OutboxMessage, claimed [][]OutboxMessage, errs []error) {
	t.Helper()
	requireDisjointClaims(t, e, []uuid.UUID{msgs[0].ID, msgs[2].ID}, claimed, errs)
	claims := map[uuid.UUID]int64{}
	for i, got := range claimed {
		if errs[i] != nil {
			continue
		}
		for _, m := range got {
			claims[m.ID]++
		}
	}
	for _, m := range msgs {
		require.Equal(t, map[string]any{"lock_generation": claims[m.ID]}, e.stored(t, OutboxSchema, m.ID, "lock_generation"),
			"message %s: its generation counts its committed claims", m.ID)
	}
}

// raceWithin runs workers at once and answers each one's error. A worker
// that has not returned within limit fails the test: a hang.
func (e *env) raceWithin(t *testing.T, limit time.Duration, workers []func() error) []error {
	t.Helper()
	done := make(chan []error, 1)
	go func() { done <- race(len(workers), func(i int) error { return workers[i]() }) }()
	select {
	case errs := <-done:
		return errs
	case <-time.After(limit):
		t.Fatalf("a transaction did not return within %s", limit)
		return nil
	}
}

func (e *env) claimTopic(topic string, relay int, out *[]OutboxMessage) error {
	return e.inTx(func(uow UnitOfWork) error {
		var err error
		*out, err = uow.Outbox().ClaimBatchByTopics(e.ctx, fmt.Sprintf("relay-%d", relay), time.Hour, 2, []string{topic})
		return err
	})
}

// touchTopic stamps last_error on every message of topic with one
// Tx.UpdateWhere, its candidates in enqueued order or the reverse.
func (e *env) touchTopic(topic string, order forma.SortOrder, out *[]uuid.UUID) error {
	return e.store.tr.InTx(e.ctx, func(ctx context.Context, tx forma.Tx) error {
		result, err := tx.UpdateWhere(ctx, &forma.MutateWhereRequest{SchemaName: OutboxSchema, Condition: equals("topic", topic),
			Sort: []forma.OrderBy{{Attribute: "enqueued_at", SortOrder: order}}, Updates: map[string]any{"last_error": "touched " + string(order)}})
		if err != nil {
			return err
		}
		*out = result.RowIDs
		return nil
	})
}

// upsertMessage upserts on row ID: it creates m under rowID, or counts one
// more attempt on the row that holds it.
func (e *env) upsertMessage(rowID uuid.UUID, m OutboxMessage, out *forma.UpsertOutcome) error {
	return e.store.tr.InTx(e.ctx, func(ctx context.Context, tx forma.Tx) error {
		result, err := tx.Upsert(ctx, &forma.UpsertRequest{
			Create: forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: OutboxSchema, RowID: rowID},
				Data: map[string]any{"partition_key": m.PartitionKey, "topic": m.Topic, "status": m.Status, "attempts": 0,
					"lock_generation": 0, "next_attempt_at": rfc3339(m.NextAttemptAt), "enqueued_at": rfc3339(m.EnqueuedAt)}},
			Target:    forma.ConflictTarget{RowID: true},
			Action:    forma.UpsertUpdate,
			UpdateOps: []forma.UpdateOp{forma.Increment("attempts", 1)},
		})
		if err != nil {
			return err
		}
		*out = result.Outcome
		return nil
	})
}

// assertGeneration asserts in a transaction of its own that the message is
// at a generation, any generation: AssertRow's lock, held until the commit.
func (e *env) assertGeneration(id uuid.UUID) error {
	return e.store.tr.InTx(e.ctx, func(ctx context.Context, tx forma.Tx) error {
		return tx.AssertRow(ctx, OutboxSchema, id, &forma.KvCondition{Attr: "lock_generation", Value: "gte:0"})
	})
}
