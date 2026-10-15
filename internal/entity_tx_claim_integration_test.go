package internal

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Claim (#653) on real PostgreSQL, one claimer at a time: the #593 claim-list
// items that need no contention, the rule-8 guard, LTFlow's MarkFailed, and
// how a claim treats a candidate that changed, the Tx's own writes and the
// operation limit. Every test runs through newOutboxPG, which connects with
// testdb.Connect: it fails rather than skips in CI.

// #593 claim list 3: every claimed row is written before Claim returns, and
// its record is the committed row.
func TestClaimReturnsTheRowsAsStored(t *testing.T) {
	env := newOutboxPG(t)
	env.createMessages(t, "t", 4)
	records, err := env.claim(claimOf("t", "relay-1", 3))
	require.NoError(t, err)
	require.Len(t, records, 3)
	for _, record := range records {
		env.requireStoredAs(t, record)
		assert.Equal(t, "publishing", record.Attributes["status"])
		assert.Equal(t, "relay-1", record.Attributes["locked_by"])
		assert.EqualValues(t, 1, record.Attributes["lock_generation"])
		lease := record.Attributes["locked_until"].(time.Time)
		assert.WithinDuration(t, time.Now().Add(outboxLease), lease, 10*time.Second, "the lease runs from the Tx clock")
		assert.Equal(t, record.Attributes, env.get(t, record.RowID), "a direct Get after commit")
	}
}

// #593 claim list 4: on quiescent data a claim answers the first Limit rows
// by (Sort, row_id), every time; with fewer matching rows, all of them.
func TestClaimTakesTheFirstRowsInSortOrder(t *testing.T) {
	env := newOutboxPG(t)
	for n, topic := range []string{"first", "fresh"} {
		// Six messages, created in three instants of two. Their row IDs
		// descend in creation order, so within an instant the row ID, not
		// the insertion order, decides.
		var ids []uuid.UUID
		for i := range 6 {
			id := uuid.MustParse(fmt.Sprintf("0190b6a0-0000-7000-8000-%02d%010d", n, 6-i))
			ids = append(ids, env.createWithID(t, id, pendingMessage(topic, i/2)))
		}
		want := slices.Clone(ids)
		slices.SortStableFunc(want, func(a, b uuid.UUID) int {
			ia, ib := slices.Index(ids, a)/2, slices.Index(ids, b)/2
			if ia != ib {
				return ia - ib
			}
			return bytes.Compare(a[:], b[:])
		})
		require.NotEqual(t, ids, want, "the row IDs run against the insertion order")
		records, err := env.claim(claimOf(topic, "relay", 3))
		require.NoError(t, err)
		assert.Equal(t, want[:3], rowIDsOf(records), "%s: the first three by (enqueued_at, row_id)", topic)

		records, err = env.claim(claimOf(topic, "relay", 10))
		require.NoError(t, err)
		assert.Equal(t, want[3:], rowIDsOf(records), "%s: every remaining row, in order", topic)

		records, err = env.claim(claimOf(topic, "relay", 10))
		require.NoError(t, err)
		assert.Empty(t, records, "%s: nothing left is not an error", topic)
	}
}

// claimOne claims topic's single message and answers it.
func (e *outboxPG) claimOne(t *testing.T, topic, worker string) *forma.DataRecord {
	t.Helper()
	records, err := e.claim(claimOf(topic, worker, 1))
	require.NoError(t, err)
	require.Len(t, records, 1)
	return records[0]
}

// #593 claim list 5, special scenario 7: a lease moved into the past is
// reclaimed, at the next generation.
func TestClaimReclaimsAnExpiredLease(t *testing.T) {
	env := newOutboxPG(t)
	msg := pendingMessage("t", 0)
	msg["lock_generation"] = int64(3)
	row := env.create(t, msg)
	a := env.claimOne(t, "t", "A")
	require.EqualValues(t, 4, generationOf(t, a))

	env.expireLease(t, row, 4)
	b := env.claimOne(t, "t", "B")
	assert.Equal(t, row, b.RowID)
	assert.EqualValues(t, 5, generationOf(t, b))
	assert.Equal(t, "B", b.Attributes["locked_by"])
	env.requireStoredAs(t, b)
}

// #593 claim list 6, special scenario 7: an active lease is not claimable.
func TestClaimLeavesAnActiveLease(t *testing.T) {
	env := newOutboxPG(t)
	row := env.create(t, pendingMessage("t", 0))
	a := env.claimOne(t, "t", "A")
	before := env.snapshot(t, row)

	records, err := env.claim(claimOf("t", "B", 10))
	require.NoError(t, err)
	assert.Empty(t, records)
	assert.Equal(t, before, env.snapshot(t, row), "B wrote nothing")
	require.NotNil(t, env.stored(t, a.RowID).lockedBy)
	assert.Equal(t, "A", *env.stored(t, a.RowID).lockedBy, "A keeps its lease")
}

// #593 claim list 7, special scenario 8: a holder whose lease was taken over
// cannot complete; the new holder's claim stands.
func TestClaimStaleGenerationCannotComplete(t *testing.T) {
	env := newOutboxPG(t)
	msg := pendingMessage("t", 0)
	msg["lock_generation"] = int64(3)
	row := env.create(t, msg)
	require.EqualValues(t, 4, generationOf(t, env.claimOne(t, "t", "A")))
	env.expireLease(t, row, 4)
	b := env.claimOne(t, "t", "B")
	before := env.snapshot(t, row)

	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Update(ctx, complete(row, 4))
		return err
	})
	require.ErrorIs(t, err, forma.ErrPreconditionFailed)
	assert.Equal(t, before, env.snapshot(t, row), "A's completion wrote nothing")
	s := env.stored(t, row)
	assert.Equal(t, "publishing", s.status)
	assert.EqualValues(t, 5, s.generation)
	require.NotNil(t, s.lockedBy)
	assert.Equal(t, "B", *s.lockedBy)
	assert.Equal(t, b.Attributes["locked_until"].(time.Time).UnixMilli(), *s.lockedUntil, "B's lease")
}

// Rule 8: a claim mutation that leaves a claimed row matching the condition
// is ErrInvalidInput and poisons the Tx, so nothing it wrote commits.
func TestClaimThatLeavesItsConditionTrueIsRefused(t *testing.T) {
	env := newOutboxPG(t)
	ids := env.createMessages(t, "t", 3)
	before := env.snapshots(t, ids)
	req := claimOf("t", "relay", 3)
	req.Updates = map[string]any{"locked_by": "relay"} // status stays pending
	req.UpdateOps = []forma.UpdateOp{forma.Increment("lock_generation", 1)}

	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Claim(ctx, req)
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		require.ErrorContains(t, err, "the claim mutation must make a claimed row stop matching the claim condition")
		published, ok := forma.ResolvePublicMessage(err)
		require.True(t, ok)
		assert.Contains(t, published, "3 of the 3 rows claimed on outbox653 still match it")
		_, err = tx.Get(ctx, &forma.QueryRequest{SchemaName: outboxSchemaName, RowID: &ids[0]})
		require.ErrorIs(t, err, forma.ErrTxAborted, "the Tx is poisoned")
		return nil
	})
	require.ErrorIs(t, err, forma.ErrTxAborted)
	env.requireUnchanged(t, before, "a refused claim commits nothing")
}

// requireUnchanged asserts every row in before is as it was, by direct SQL.
func (e *outboxPG) requireUnchanged(t *testing.T, before map[uuid.UUID]rowSnapshot, msg string) {
	t.Helper()
	for id, snap := range before {
		require.Equal(t, snap, e.snapshot(t, id), "%s: %s", msg, id)
	}
}

func (e *outboxPG) snapshot(t *testing.T, rowID uuid.UUID) rowSnapshot {
	t.Helper()
	return e.snapshotOf(t, outboxSchemaID, rowID)
}

func (e *outboxPG) snapshots(t *testing.T, ids []uuid.UUID) map[uuid.UUID]rowSnapshot {
	t.Helper()
	snaps := make(map[uuid.UUID]rowSnapshot, len(ids))
	for _, id := range ids {
		snaps[id] = e.snapshot(t, id)
	}
	return snaps
}

func (e *outboxPG) get(t *testing.T, rowID uuid.UUID) map[string]any {
	t.Helper()
	record, err := e.em.Get(e.ctx, &forma.QueryRequest{SchemaName: outboxSchemaName, RowID: &rowID})
	require.NoError(t, err)
	return record.Attributes
}

// LTFlow's MarkFailed completes a claim with a CAS on its generation, counts
// the attempt and ends the lease; the next claim waits for next_attempt_at.
// LTFlow sets locked_until to NULL; an Update cannot clear an attribute, so
// the lease ends at the Tx clock instead, which no claim condition reads as
// live.
func TestClaimMarkFailedShape(t *testing.T) {
	env := newOutboxPG(t)
	row := env.create(t, pendingMessage("t", 0))
	claimed := env.claimOne(t, "t", "A")
	retryAt := time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)

	_, err := env.em.Update(env.ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: outboxSchemaName, RowID: row},
		Preconditions:    &forma.KvCondition{Attr: "lock_generation", Value: fmt.Sprintf("equals:%d", generationOf(t, claimed))},
		Updates:          map[string]any{"status": "failed", "next_attempt_at": retryAt},
		UpdateOps:        []forma.UpdateOp{forma.Increment("attempts", 1), forma.ServerTime("locked_until", 0)},
	})
	require.NoError(t, err)
	s := env.stored(t, row)
	assert.Equal(t, "failed", s.status)
	require.NotNil(t, s.lockedUntil)
	assert.LessOrEqual(t, *s.lockedUntil, time.Now().UnixMilli(), "the lease has ended")

	records, err := env.claim(claimOf("t", "B", 10))
	require.NoError(t, err)
	assert.Empty(t, records, "the retry is not due")

	_, err = env.em.Update(env.ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: outboxSchemaName, RowID: row},
		UpdateOps:        []forma.UpdateOp{forma.ServerTime("next_attempt_at", -time.Second)},
	})
	require.NoError(t, err)
	b := env.claimOne(t, "t", "B")
	assert.EqualValues(t, 2, generationOf(t, b))
	assert.EqualValues(t, 1, b.Attributes["attempts"])
}

// A candidate that stopped matching between the select and its lock is
// dropped, and the next candidate is claimed in its place.
func TestClaimReplacesACandidateThatStoppedMatching(t *testing.T) {
	env := newOutboxPG(t)
	ids := env.createMessages(t, "t", 4)
	env.txr.testHooks = &txTestHooks{afterCandidates: func(candidates []uuid.UUID) {
		require.Equal(t, ids, candidates, "every message, in enqueued_at order")
		_, err := env.em.Update(env.ctx, complete(ids[0], 0))
		require.NoError(t, err)
	}}
	t.Cleanup(func() { env.txr.testHooks = nil })

	records, err := env.claim(claimOf("t", "relay", 2))
	require.NoError(t, err)
	assert.Equal(t, ids[1:3], rowIDsOf(records))
	assert.Equal(t, "published", env.stored(t, ids[0]).status, "the dropped candidate keeps the concurrent write")
}

// A claim sees the Tx's own writes: a row the Tx made due is claimable, and
// the Tx's own lock on it does not make the claim skip it.
func TestClaimSeesTheTxsOwnWrites(t *testing.T) {
	env := newOutboxPG(t)
	ids := env.createMessages(t, "t", 2)
	_, err := env.em.Update(env.ctx, complete(ids[0], 0))
	require.NoError(t, err)

	err = env.inTx(func(ctx context.Context, tx forma.Tx) error {
		if _, err := tx.Update(ctx, &forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: outboxSchemaName, RowID: ids[0]},
			Updates:          map[string]any{"status": "pending"},
		}); err != nil {
			return err
		}
		result, err := tx.Claim(ctx, claimOf("t", "relay", 10))
		if err != nil {
			return err
		}
		assert.Equal(t, ids, rowIDsOf(result.Records))
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, "publishing", env.stored(t, ids[0]).status)
}

// Each claimed row past the first is one more operation; a claim that would
// exceed the Tx's limit poisons it (#648 rule 19).
func TestClaimCountsEachClaimedRowAsAnOperation(t *testing.T) {
	env := newOutboxPG(t)
	ids := env.createMessages(t, "t", 5)
	before := env.snapshots(t, ids)

	err := env.txr.InTx(env.ctx, func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Claim(ctx, claimOf("t", "relay", 5))
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		require.ErrorContains(t, err, "limit of 4 operations")
		return nil
	}, forma.WithTxMaxOperations(4))
	require.ErrorIs(t, err, forma.ErrTxAborted)
	env.requireUnchanged(t, before, "an over-limit claim commits nothing")

	err = env.txr.InTx(env.ctx, func(ctx context.Context, tx forma.Tx) error {
		result, err := tx.Claim(ctx, claimOf("t", "relay", 4))
		if err == nil {
			assert.Len(t, result.Records, 4)
		}
		return err
	}, forma.WithTxMaxOperations(4))
	require.NoError(t, err, "four rows are four operations")
}

// A claim mutation may not change an attribute of a unique key: moving a
// key entry can wait for another transaction, and a claim never waits.
func TestClaimRefusesAMutationOfAUniqueKeyAttribute(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	row := env.mustCreate(t, ukIdem, map[string]any{"instance_id": "i1", "idempotency_key": "k1", "status": "pending"})
	claim := func(updates map[string]any) *forma.ClaimRequest {
		return &forma.ClaimRequest{SchemaName: ukIdem, Limit: 1,
			Condition: &forma.KvCondition{Attr: "status", Value: "equals:pending"}, Updates: updates}
	}
	require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
		for attr, key := range map[string]string{"idempotency_key": "idempotency", "external_ref": "external_ref"} {
			_, err := tx.Claim(ctx, claim(map[string]any{"status": "claimed", attr: "x"}))
			require.ErrorIs(t, err, forma.ErrInvalidInput, attr)
			require.ErrorContains(t, err, fmt.Sprintf("cannot change attribute '%s' of unique key %s", attr, key))
		}
		result, err := tx.Claim(ctx, claim(map[string]any{"status": "claimed"}))
		if err != nil {
			return err
		}
		require.Len(t, result.Records, 1)
		assert.Equal(t, row, result.Records[0].RowID)
		return nil
	}), "the refusals leave the Tx usable")
	env.assertKeyTableMatchesRows(t)
}

// A row failure that would not poison the Tx by itself, such as
// ErrPreconditionFailed, poisons a claim: its rows are held, and after the
// first one possibly written, so the Tx cannot commit part of a claim.
func TestClaimRowFailurePoisonsTheTx(t *testing.T) {
	for _, at := range []int{0, 1} {
		t.Run(fmt.Sprintf("at row %d", at), func(t *testing.T) {
			env := newOutboxPG(t)
			ids := env.createMessages(t, "t", 3)
			before := env.snapshots(t, ids)
			env.txr.testHooks = &txTestHooks{beforeWrite: func(i int, _ uuid.UUID) error {
				if i == at {
					return forma.PreconditionFailedf("injected at claimed row %d", i)
				}
				return nil
			}}
			t.Cleanup(func() { env.txr.testHooks = nil })

			err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
				_, err := tx.Claim(ctx, claimOf("t", "relay", 3))
				require.ErrorIs(t, err, forma.ErrPreconditionFailed)
				_, err = tx.Get(ctx, &forma.QueryRequest{SchemaName: outboxSchemaName, RowID: &ids[2]})
				require.ErrorIs(t, err, forma.ErrTxAborted, "the claim poisoned the Tx")
				return nil
			})
			require.ErrorIs(t, err, forma.ErrTxAborted)
			for _, id := range ids {
				assert.Equal(t, before[id], env.snapshot(t, id), "row %s is as it was", id)
			}
		})
	}
}
