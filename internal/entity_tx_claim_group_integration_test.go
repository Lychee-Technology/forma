package internal

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Group-head claims (#653) on real PostgreSQL: LTFlow's per-partition FIFO,
// where only the earliest unresolved message of a partition is claimable.

// outboxLive is LTFlow's liveness: a message blocks the later messages of
// its partition until it is published or dead-lettered.
var outboxLive = &forma.KvCondition{Attr: "status", Value: `not_in:["published","dead_letter"]`}

// fifo restricts req to partition heads by enqueued_at.
func fifo(req *forma.ClaimRequest) *forma.ClaimRequest {
	req.Group = &forma.ClaimGroup{Key: []string{"partition_key"},
		Order: []forma.OrderBy{{Attribute: "enqueued_at"}}, Live: outboxLive}
	return req
}

// partitioned answers a due pending message of topic in partition p, none
// when p is empty, enqueued at the i-th millisecond after outboxBase.
func partitioned(topic, p string, i int) map[string]any {
	message := pendingMessage(topic, i)
	message["partition_key"] = p
	if p == "" {
		delete(message, "partition_key")
	}
	return message
}

// setStatus moves rowID to status, committed, as a relay's completion
// would.
func (e *outboxPG) setStatus(t *testing.T, rowID uuid.UUID, status string) {
	t.Helper()
	_, err := e.em.Update(e.ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: outboxSchemaName, RowID: rowID},
		Updates:          map[string]any{"status": status}})
	require.NoError(t, err)
}

func (e *outboxPG) claimFIFO(t *testing.T, limit int) []uuid.UUID {
	t.Helper()
	records, err := e.claim(fifo(claimOf("g", "relay", limit)))
	require.NoError(t, err)
	return rowIDsOf(records)
}

// Only the earliest live row of each group is claimed, heads in Sort order.
// Row IDs descend as enqueued_at ascends, so the head is the group order's,
// not the row-ID tiebreak's.
func TestClaimGroupTakesOnlyTheHead(t *testing.T) {
	env := newOutboxPG(t)
	ids := env.createDescending(t, 1, []map[string]any{
		partitioned("g", "p1", 0), partitioned("g", "p1", 1), partitioned("g", "p1", 2),
		partitioned("g", "p2", 3), partitioned("g", "p2", 4),
	})
	assert.Equal(t, []uuid.UUID{ids[0], ids[3]}, env.claimFIFO(t, 10))
}

// Rows tied on the group order are ordered by row ID, ascending.
func TestClaimGroupBreaksAnOrderTieByRowID(t *testing.T) {
	env := newOutboxPG(t)
	ids := env.createDescending(t, 2, []map[string]any{partitioned("g", "p1", 0), partitioned("g", "p1", 0)})
	assert.Equal(t, ids[1:], env.claimFIFO(t, 10), "the tied row with the smaller row ID heads p1")
}

// A descending group order makes the latest row the head. Row IDs ascend
// with enqueued_at here, so neither the tiebreak nor an ascending order
// would pick it.
func TestClaimGroupDescendingOrder(t *testing.T) {
	env := newOutboxPG(t)
	ids := env.createAll(t, []map[string]any{partitioned("g", "p1", 0), partitioned("g", "p1", 1), partitioned("g", "p1", 2)})
	req := fifo(claimOf("g", "relay", 10))
	req.Group.Order = []forma.OrderBy{{Attribute: "enqueued_at", SortOrder: forma.SortOrderDesc}}
	records, err := env.claim(req)
	require.NoError(t, err)
	assert.Equal(t, ids[2:], rowIDsOf(records))
}

// The candidate select is restricted to heads, not only the re-check: a
// group whose live rows fill the over-fetched candidates does not crowd out
// another group's head.
func TestClaimGroupCandidatesAreHeads(t *testing.T) {
	env := newOutboxPG(t)
	ids := env.createDescending(t, 3, []map[string]any{
		partitioned("g", "p1", 0), partitioned("g", "p1", 1), partitioned("g", "p1", 2),
		partitioned("g", "p1", 3), partitioned("g", "p1", 4), partitioned("g", "p2", 5),
	})
	assert.Equal(t, []uuid.UUID{ids[0], ids[5]}, env.claimFIFO(t, 2),
		"p1's first 2×Limit rows by Sort are not all candidates; p2's head is")
}

// A head under an active claim is live and not due: it blocks its group,
// so the next claim takes nothing.
func TestClaimGroupHeadUnderAnActiveClaimBlocksItsGroup(t *testing.T) {
	env := newOutboxPG(t)
	ids := env.createAll(t, []map[string]any{partitioned("g", "p1", 0), partitioned("g", "p1", 1)})
	require.Equal(t, ids[:1], env.claimFIFO(t, 10))
	assert.Empty(t, env.claimFIFO(t, 10), "r2 waits behind the claimed head")
}

// A published head stops being live, and the next row becomes the head.
func TestClaimGroupPublishedHeadPassesToTheNextRow(t *testing.T) {
	env := newOutboxPG(t)
	ids := env.createAll(t, []map[string]any{partitioned("g", "p1", 0), partitioned("g", "p1", 1), partitioned("g", "p1", 2)})
	require.Equal(t, ids[:1], env.claimFIFO(t, 10))
	_, err := env.em.Update(env.ctx, complete(ids[0], 1))
	require.NoError(t, err)
	assert.Equal(t, ids[1:2], env.claimFIFO(t, 10))
}

// A head whose lease expired is reclaimed itself; the next row stays
// behind it.
func TestClaimGroupExpiredHeadIsReclaimedNotPassed(t *testing.T) {
	env := newOutboxPG(t)
	ids := env.createAll(t, []map[string]any{partitioned("g", "p1", 0), partitioned("g", "p1", 1)})
	require.Equal(t, ids[:1], env.claimFIFO(t, 10))
	env.expireLease(t, ids[0], 1)
	records, err := env.claim(fifo(claimOf("g", "B", 10)))
	require.NoError(t, err)
	require.Equal(t, ids[:1], rowIDsOf(records))
	assert.EqualValues(t, 2, generationOf(t, records[0]))
}

// Earlier rows that are no longer live do not block; a live earlier row
// that the claim condition does not match does, as LTFlow's failed message
// waiting for its retry holds back its partition.
func TestClaimGroupOnlyLiveEarlierRowsBlock(t *testing.T) {
	env := newOutboxPG(t)
	ids := env.createAll(t, []map[string]any{
		partitioned("g", "p1", 0), partitioned("g", "p1", 1), partitioned("g", "p1", 2),
		partitioned("g", "p2", 3), partitioned("g", "p2", 4),
	})
	env.setStatus(t, ids[0], "dead_letter")
	env.setStatus(t, ids[1], "published")
	_, err := env.em.Update(env.ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: outboxSchemaName, RowID: ids[3]},
		Updates:          map[string]any{"status": "failed"},
		UpdateOps:        []forma.UpdateOp{forma.ServerTime("next_attempt_at", time.Hour)}})
	require.NoError(t, err)
	assert.Equal(t, ids[2:3], env.claimFIFO(t, 10), "p1's resolved rows do not block r3; p2's failed head blocks s2")
}

// A null key component equals a null: rows without a partition form one
// group. A null order value sorts last, so it never heads a group with a
// valued row in it.
func TestClaimGroupNullsGroupAndSortLast(t *testing.T) {
	env := newOutboxPG(t)
	unordered := partitioned("g", "p1", 0)
	delete(unordered, "enqueued_at")
	ids := env.createAll(t, []map[string]any{
		partitioned("g", "", 1), partitioned("g", "", 2), unordered, partitioned("g", "p1", 3),
	})
	assert.Equal(t, []uuid.UUID{ids[0], ids[3]}, env.claimFIFO(t, 10),
		"one head for the rows without a partition; p1's valued row heads the row with no enqueued_at")
}

// attempted sets message's attempts to n, or clears it when n is negative.
func attempted(message map[string]any, n int) map[string]any {
	message["attempts"] = n
	if n < 0 {
		delete(message, "attempts")
	}
	return message
}

// Rows share a group only when every key component is equal, a null equal
// to a null.
func TestClaimGroupTwoKeyAttributes(t *testing.T) {
	env := newOutboxPG(t)
	ids := env.createDescending(t, 4, []map[string]any{
		partitioned("g", "p1", 0), partitioned("g", "p1", 1), attempted(partitioned("g", "p1", 2), 1),
		attempted(partitioned("g", "p1", 3), -1), attempted(partitioned("g", "p1", 4), -1), partitioned("g", "", 5),
	})
	req := fifo(claimOf("g", "relay", 10))
	req.Group.Key = []string{"partition_key", "attempts"}
	records, err := env.claim(req)
	require.NoError(t, err)
	assert.Equal(t, []uuid.UUID{ids[0], ids[2], ids[3], ids[5]}, rowIDsOf(records),
		"one head each for (p1, 0), (p1, 1), (p1, null) and (null, 0)")
}

// A later order attribute breaks a tie of an earlier one before the row ID
// does.
func TestClaimGroupTwoOrderAttributes(t *testing.T) {
	env := newOutboxPG(t)
	ids := env.createDescending(t, 5, []map[string]any{
		attempted(partitioned("g", "p1", 0), 1), partitioned("g", "p1", 1), partitioned("g", "p1", 2),
	})
	req := fifo(claimOf("g", "relay", 10))
	req.Group.Order = []forma.OrderBy{{Attribute: "attempts"}, {Attribute: "enqueued_at"}}
	records, err := env.claim(req)
	require.NoError(t, err)
	assert.Equal(t, ids[1:2], rowIDsOf(records),
		"attempts, then enqueued_at, pick r2; attempts then row ID would pick r3, enqueued_at alone r1")
}

// The re-check applies the head predicate too: a candidate that stopped
// being its group's head between the select and its lock is dropped.
func TestClaimGroupReCheckDropsARowThatStoppedBeingTheHead(t *testing.T) {
	env := newOutboxPG(t)
	ids := env.createAll(t, []map[string]any{partitioned("g", "p1", 0), partitioned("g", "p1", 1)})
	env.setStatus(t, ids[0], "published")
	env.txr.testHooks = &txTestHooks{afterCandidates: func(candidates []uuid.UUID) {
		require.Equal(t, ids[1:], candidates, "r2 heads p1 while r1 is published")
		_, err := env.em.Update(env.ctx, &forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: outboxSchemaName, RowID: ids[0]},
			Updates:          map[string]any{"status": "failed"},
			UpdateOps:        []forma.UpdateOp{forma.ServerTime("next_attempt_at", time.Hour)}})
		require.NoError(t, err)
	}}
	t.Cleanup(func() { env.txr.testHooks = nil })
	assert.Empty(t, env.claimFIFO(t, 10), "r1 is live again, so r2 is no head")
	assert.Equal(t, "pending", env.stored(t, ids[1]).status)
}

// A head another transaction holds is skipped without waiting, and no later
// row of its group is claimed in its place.
func TestClaimGroupHeldHeadHasNoFallThrough_PostgresOnly(t *testing.T) {
	env := newOutboxPG(t)
	ids := env.createAll(t, []map[string]any{
		partitioned("g", "p1", 0), partitioned("g", "p1", 1), partitioned("g", "p2", 2),
	})
	held, release, holder := env.holdingTx(func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Update(ctx, &forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: outboxSchemaName, RowID: ids[0]},
			Updates:          map[string]any{"locked_by": "holder"}})
		return err
	})
	defer release()
	await(t, held, holder, "the holder")

	records, waited := env.claimWatched(t, fifo(claimOf("g", "B", 10)))
	assert.Equal(t, ids[2:], rowIDsOf(records), "p1's head is held, and r2 does not fall through")
	assert.False(t, waited, "the claim's backend never had an ungranted lock")
	release()
	require.NoError(t, <-holder)
	assert.Equal(t, ids[:1], env.claimFIFO(t, 10), "once released, p1's head is claimable")
}
