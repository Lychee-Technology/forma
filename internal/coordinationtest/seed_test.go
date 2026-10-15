package coordinationtest

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// Seed data, written through the adapter in transactions of their own.

const workflowName = "order_fulfilment"

// newID answers a fresh row ID.
func newID() uuid.UUID {
	return uuid.Must(uuid.NewV7())
}

// seedInstance creates a running instance at version.
func (e *env) seedInstance(t *testing.T, version int64) *Instance {
	t.Helper()
	inst := &Instance{ID: newID(), WorkflowName: workflowName, State: "start", Status: "running", Version: version,
		BusinessReference: "ref-" + uuid.NewString()[:8]}
	require.NoError(t, e.inTx(func(uow UnitOfWork) error { return uow.Instances().Create(e.ctx, inst) }))
	return inst
}

// seedTasks creates tasks in one transaction.
func (e *env) seedTasks(t *testing.T, tasks ...*Task) {
	t.Helper()
	require.NoError(t, e.inTx(func(uow UnitOfWork) error {
		for _, task := range tasks {
			if err := uow.Tasks().Insert(e.ctx, task); err != nil {
				return err
			}
		}
		return nil
	}))
}

// task answers a task of inst at its visit (state, version).
func task(inst *Instance, hash, status, event, purpose string) *Task {
	return &Task{TokenHash: hash, InstanceID: inst.ID, State: inst.State, Version: inst.Version, Status: status,
		ExpectedEvent: event, Purpose: purpose}
}

// message answers a pending message of partition, due since a minute ago,
// enqueued at enqueued.
func message(partition string, enqueued time.Time) OutboxMessage {
	return OutboxMessage{PartitionKey: partition, Topic: "orders", CommandType: "ship", Payload: "{}",
		Status: OutboxPending, NextAttemptAt: time.Now().Add(-time.Minute), EnqueuedAt: enqueued}
}

// seeding numbers each seedOutbox call, so that each one's partitions are
// new and none queues behind the heads an earlier call left held.
var seeding atomic.Int64

// seedOutbox enqueues perPartition messages in each of partitions new
// partitions, partition p's message i enqueued i*partitions+p milliseconds
// after base, and answers them in enqueue order per partition.
func (e *env) seedOutbox(t *testing.T, partitions, perPartition int) [][]OutboxMessage {
	t.Helper()
	base := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	batch := seeding.Add(1)
	out := make([][]OutboxMessage, partitions)
	require.NoError(t, e.inTx(func(uow UnitOfWork) error {
		for p := range out {
			out[p] = make([]OutboxMessage, perPartition)
			for i := range out[p] {
				out[p][i] = message(fmt.Sprintf("seed-%d-partition-%03d", batch, p), base.Add(time.Duration(i*partitions+p)*time.Millisecond))
			}
			if err := uow.Outbox().Insert(e.ctx, out[p]); err != nil {
				return err
			}
		}
		return nil
	}))
	return out
}

// seedTiedOutbox enqueues perPartition messages in each of partitions new
// partitions, every message at one instant, as one transaction enqueues
// LTFlow's messages at one now(). Each partition's messages are inserted in
// descending row-ID order and answered in ascending row-ID order, the order
// the enqueued_at tie must be broken in.
func (e *env) seedTiedOutbox(t *testing.T, partitions, perPartition int) [][]OutboxMessage {
	t.Helper()
	at := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	batch := seeding.Add(1)
	out := make([][]OutboxMessage, partitions)
	require.NoError(t, e.inTx(func(uow UnitOfWork) error {
		for p := range out {
			out[p] = make([]OutboxMessage, perPartition)
			for i, id := range ascendingIDs(perPartition) {
				out[p][i] = message(fmt.Sprintf("seed-%d-tied-%03d", batch, p), at)
				out[p][i].ID = id
			}
			reversed := slices.Clone(out[p])
			slices.Reverse(reversed)
			if err := uow.Outbox().Insert(e.ctx, reversed); err != nil {
				return err
			}
		}
		return nil
	}))
	return out
}

// ascendingIDs answers n fresh row IDs in ascending order.
func ascendingIDs(n int) []uuid.UUID {
	out := make([]uuid.UUID, n)
	for i := range out {
		out[i] = newID()
	}
	slices.SortFunc(out, func(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) })
	return out
}

// flatten answers every message of partitions.
func flatten(partitions [][]OutboxMessage) []OutboxMessage {
	var all []OutboxMessage
	for _, p := range partitions {
		all = append(all, p...)
	}
	return all
}

// claimBatch claims in a transaction of its own and commits.
func (e *env) claimBatch(t *testing.T, claimer string, lease time.Duration, limit int) []OutboxMessage {
	t.Helper()
	var claimed []OutboxMessage
	require.NoError(t, e.inTx(func(uow UnitOfWork) error {
		var err error
		claimed, err = uow.Outbox().ClaimBatch(e.ctx, claimer, lease, limit)
		return err
	}))
	return claimed
}

// expireLease ends the lease of a held message: a CAS update of
// locked_until to the past, conditional on its generation, through the
// public Transactor (scenario 7's setup).
func (e *env) expireLease(t *testing.T, msg OutboxMessage) {
	t.Helper()
	require.NoError(t, e.tryExpireLease(msg))
}

func (e *env) tryExpireLease(msg OutboxMessage) error {
	return e.store.tr.InTx(e.ctx, func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Update(ctx, &forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: OutboxSchema, RowID: msg.ID},
			Preconditions:    equals("lock_generation", msg.LockGeneration),
			Updates:          map[string]any{"locked_until": rfc3339(time.Now().Add(-time.Minute))},
		})
		return err
	})
}

// ids answers the IDs of msgs.
func ids(msgs []OutboxMessage) []uuid.UUID {
	out := make([]uuid.UUID, len(msgs))
	for i, m := range msgs {
		out[i] = m.ID
	}
	return out
}
