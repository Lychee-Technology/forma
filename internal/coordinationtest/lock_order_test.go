package coordinationtest

import (
	"context"
	"fmt"
	"sync/atomic"
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
//
// Each round opens behind two gates. One transaction holds AssertRow's
// locks on the seeded rows that single-row primitives lock, apart from the
// partition head an AssertRow contends for with the claims, and another has
// created a task under the token hash a key upsert of the round creates.
// Once every transaction of the round has answered or waits on a lock, both
// gates commit, and each lock's waiters are granted at one instant instead
// of whenever a narrow window happens to open. A primitive that took the
// two locks of a row in another order than UpdateWhere then holds one while
// an UpdateWhere holds the other.

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
// the tasks at once behind the round's gates, checks what the committed ones
// did, and answers how many transactions, the gates included, failed with
// ErrRetryableConflict.
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
	// msgs[0] and msgs[2] are the partition heads, which the gate leaves to
	// the claims.
	gate := e.holdRows(t, rowRef{OutboxSchema, msgs[1].ID}, rowRef{OutboxSchema, msgs[3].ID},
		rowRef{TaskSchema, tasks.tasks[0].ID}, rowRef{TaskSchema, tasks.tasks[1].ID})
	creator := tasks.holdCreated(t)

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
	errs, gateErrs := e.raceGated(t, time.Minute, []*held{gate, creator}, append(workers, tasks.workers()...))

	retryable := 0
	for i, err := range errs {
		if forma.IsRetryable(err) {
			retryable++
			continue
		}
		require.NoError(t, err, "round %d transaction %d", round, i)
	}
	// On PostgreSQL neither gate waits once the round starts, so neither
	// can lose. Elsewhere either may lose to a worker that committed first,
	// the creator to the round's other writer of its token hash.
	gateErr, creatorErr := gateErrs[0], gateErrs[1]
	for _, err := range gateErrs {
		if forma.IsRetryable(err) {
			retryable++
		}
	}
	if !forma.IsRetryable(gateErr) {
		require.NoError(t, gateErr, "round %d: the row gate", round)
	}
	if creatorErr != nil {
		requireLoser(t, creatorErr, forma.ErrUniqueConflict)
	}
	e.requireLockOrderRound(t, msgs, before, claimed, errs)
	tasks.requireCommitted(t, errs[len(workers):], creatorErr)
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
func (e *env) requireLockOrderRound(t *testing.T, msgs []OutboxMessage, before map[uuid.UUID]int64, claimed [][]OutboxMessage, errs []error) {
	t.Helper()
	requireDisjointClaims(t, e, []uuid.UUID{msgs[0].ID, msgs[2].ID}, before, claimed, errs)
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

// raceGated starts workers at once while gates hold their locks, waits
// until each worker has answered or waits on a lock, then releases every
// gate at once. It answers each worker's error and each gate's. A worker
// that has not returned within limit of the start fails the test: a hang.
// On a backend whose writers do not block, every worker answers before the
// gates are released.
func (e *env) raceGated(t *testing.T, limit time.Duration, gates []*held, workers []func() error) (errs, gateErrs []error) {
	t.Helper()
	deadline := time.After(limit)
	var answered atomic.Int64
	done := make(chan []error, 1)
	go func() {
		done <- race(len(workers), func(i int) error {
			defer answered.Add(1)
			return workers[i]()
		})
	}()
	require.Eventually(t, func() bool {
		n := int(answered.Load())
		if waits, err := e.lockWaits(); err == nil {
			n += waits
		}
		return n >= len(workers)
	}, limit, 10*time.Millisecond, "each transaction of the round answers or waits on a lock")
	for _, g := range gates {
		g.finish()
	}
	gateErrs = make([]error, len(gates))
	for i, g := range gates {
		gateErrs[i] = e.result(t, g.done)
	}
	select {
	case errs = <-done:
		return errs, gateErrs
	case <-deadline:
		t.Fatalf("a transaction did not return within %s", limit)
		return nil, nil
	}
}

// rowRef names one row.
type rowRef struct {
	schema string
	id     uuid.UUID
}

// holdRows parks a transaction that holds AssertRow's locks on rows: a
// lock-order round's gate. It takes them before any worker of the round
// starts, so it waits for none of them.
func (e *env) holdRows(t *testing.T, rows ...rowRef) *held {
	t.Helper()
	return e.holdTx(t, func(ctx context.Context, tx forma.Tx) error {
		for _, r := range rows {
			if err := tx.AssertRow(ctx, r.schema, r.id, nil); err != nil {
				return fmt.Errorf("gate %s row %s: %w", r.schema, r.id, err)
			}
		}
		return nil
	})
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
