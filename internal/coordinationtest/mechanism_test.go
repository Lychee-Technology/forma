package coordinationtest

import (
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// PostgreSQL mechanisms the contract relies on, observed in pg_locks.

func TestMechanisms(t *testing.T) {
	t.Run("a relay claiming while an instance step holds a message row does not wait", func(t *testing.T) {
		requirePostgresMechanisms(t)
		e := newEnv(t)
		inst := e.seedInstance(t, 1)
		partitions := e.seedOutbox(t, 2, 2)
		heldMsg, other := partitions[0][0], partitions[1][0]
		// The step advances the instance and amends the command of its
		// pending message, which keeps that row locked until it commits.
		step := e.hold(t, func(uow UnitOfWork) error {
			if err := uow.Instances().Update(e.ctx, advanced(inst, "shipping"), 1); err != nil {
				return err
			}
			return e.amendPayload(uow, heldMsg)
		})

		claimed := e.claimBatch(t, "relay-a", time.Hour, 5)
		require.Equal(t, ids([]OutboxMessage{other}), ids(claimed),
			"the claim skips the held message and its partition, and takes the other partition's head")
		require.Zero(t, e.waitingBackends(t), "no backend waits on a lock")
		requireUnanswered(t, step.done)

		require.NoError(t, step.commit())
		again := e.claimBatch(t, "relay-b", time.Hour, 5)
		require.Equal(t, ids([]OutboxMessage{heldMsg}), ids(again), "the committed step's message is claimable")
		require.Equal(t, map[string]any{"locked_by": "relay-b", "lock_generation": int64(1)},
			e.stored(t, OutboxSchema, heldMsg.ID, "locked_by", "lock_generation"))
	})

	t.Run("a fence blocks a concurrent update of the instance until it commits", func(t *testing.T) {
		requirePostgresMechanisms(t)
		e := newEnv(t)
		inst := e.seedInstance(t, 3)
		fence := e.hold(t, func(uow UnitOfWork) error { return uow.Instances().FenceVersion(e.ctx, inst.ID, 3) })
		update := async(func() error {
			return e.inTx(func(uow UnitOfWork) error { return uow.Instances().Update(e.ctx, advanced(inst, "shipping"), 3) })
		})
		e.requireBlocked(t, 1)
		requireUnanswered(t, update)
		require.Equal(t, map[string]any{"version": int64(3)}, e.stored(t, InstanceSchema, inst.ID, "version"))

		require.NoError(t, fence.commit(), "the fenced transaction commits at version 3")
		require.NoError(t, e.result(t, update), "the update proceeds once the fence's lock is released")
		require.Equal(t, map[string]any{"version": int64(4)}, e.stored(t, InstanceSchema, inst.ID, "version"))
	})
}

// amendPayload rewrites msg's payload in uow's transaction, through the
// Transactor's Tx under the adapter: the adapter's outbox writes only claim
// state.
func (e *env) amendPayload(uow UnitOfWork, msg OutboxMessage) error {
	_, err := uow.(*unitOfWork).tx.Update(e.ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: OutboxSchema, RowID: msg.ID},
		Updates:          map[string]any{"payload": `{"amended":true}`},
	})
	return err
}
