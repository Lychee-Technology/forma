package internal

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL tests of the unset operator in Tx.Claim (#939) over the
// outbox653 fixture: the clear is part of the claim mutation, applied to the
// claimed rows only, under group exclusivity and generation fencing; and
// LTFlow's MarkFailed ends a lease by clearing it.

// outboxNextAttemptID is next_attempt_at's attribute ID, an EAV datetime.
const outboxNextAttemptID int16 = 7

// claimClearingRetry is claimOf whose mutation also clears next_attempt_at,
// the retry time a held message no longer waits for.
func claimClearingRetry(topic, worker string, limit int) *forma.ClaimRequest {
	req := claimOf(topic, worker, limit)
	req.UpdateOps = append(req.UpdateOps, forma.Unset("next_attempt_at"))
	return req
}

// eavCount counts rowID's eav_data rows of attrID.
func (e *outboxPG) eavCount(t *testing.T, rowID uuid.UUID, attrID int16) int {
	t.Helper()
	var n int
	require.NoError(t, e.pool.QueryRow(e.ctx, fmt.Sprintf(
		"SELECT count(*) FROM %s WHERE schema_id = $1 AND row_id = $2 AND attr_id = $3", sanitizeIdentifier(e.tables.EAVData)),
		outboxSchemaID, rowID, attrID).Scan(&n))
	return n
}

// requireRetryCleared asserts that each claimed record and its committed row
// hold no next_attempt_at, and that every other row of rows still holds it.
func (e *outboxPG) requireRetryCleared(t *testing.T, rows []uuid.UUID, records []*forma.DataRecord) {
	t.Helper()
	claimed := map[uuid.UUID]bool{}
	for _, record := range records {
		claimed[record.RowID] = true
		assert.NotContains(t, record.Attributes, "next_attempt_at", "the claim answers the cleared row")
		e.requireStoredAs(t, record)
	}
	for _, rowID := range rows {
		if claimed[rowID] {
			assert.Zero(t, e.eavCount(t, rowID, outboxNextAttemptID), "claimed row %s holds no retry time", rowID)
			assert.NotContains(t, e.get(t, rowID), "next_attempt_at")
			continue
		}
		assert.Equal(t, 1, e.eavCount(t, rowID, outboxNextAttemptID), "unclaimed row %s keeps its retry time", rowID)
		s := e.stored(t, rowID)
		assert.Equal(t, "pending", s.status)
		assert.Zero(t, s.generation, "unclaimed row %s keeps its generation", rowID)
	}
}

// createAgainstSort creates messages, given in reverse enqueued_at order, in
// one batch under caller row IDs (#651) that ascend as the batch goes on.
// Insertion order and row-ID order are then both the reverse of enqueued_at
// order, so a claim that answers enqueued_at order follows the requested
// Sort and not the heap or a row-ID tiebreak. It answers the row IDs in
// enqueued_at order; tag keeps one test's IDs apart.
func (e *outboxPG) createAgainstSort(t *testing.T, tag int, messages []map[string]any) []uuid.UUID {
	t.Helper()
	n := len(messages)
	rowIDs := make([]uuid.UUID, n)
	for i := range rowIDs {
		rowIDs[i] = uuid.MustParse(fmt.Sprintf("0190b6a1-0000-7000-8000-%02d%010d", tag, i+1))
	}
	inserted := e.createBatch(t, messages, rowIDs)
	sorted := make([]uuid.UUID, n)
	for i, rowID := range inserted {
		sorted[n-1-i] = rowID
	}
	return sorted
}

// TestClaimUnsetClearsOnlyTheClaimedRows claims the first two of four due
// messages with a mutation that clears their retry time: the two are taken
// in enqueued_at order, which neither their insertion nor their row IDs
// follow, and cleared, with their lease and generation; the other two are
// untouched, and a second claim takes them.
func TestClaimUnsetClearsOnlyTheClaimedRows(t *testing.T) {
	env := newOutboxPG(t)
	ids := env.createAgainstSort(t, 1, []map[string]any{pendingMessage("t", 3), pendingMessage("t", 2),
		pendingMessage("t", 1), pendingMessage("t", 0)})

	records, err := env.claim(claimClearingRetry("t", "A", 2))
	require.NoError(t, err)
	require.Equal(t, ids[:2], rowIDsOf(records), "the first rows in enqueued_at order")
	for _, record := range records {
		assert.EqualValues(t, 1, generationOf(t, record))
	}
	env.requireRetryCleared(t, ids, records)

	again, err := env.claim(claimClearingRetry("t", "B", 10))
	require.NoError(t, err)
	assert.Equal(t, ids[2:], rowIDsOf(again), "a held row is not claimed again; the rest are, in enqueued_at order")
	env.requireRetryCleared(t, ids[2:], again)
}

// TestClaimGroupUnsetClearsOnlyTheHeads runs the clearing claim as a
// per-partition FIFO claim: only each partition's head by enqueued_at is
// claimed and cleared, heads in Sort order, though insertion and row-ID
// order run the other way; the row behind p1's head, which has the lower
// row ID, keeps its retry time, and the claimed head still blocks its
// partition.
func TestClaimGroupUnsetClearsOnlyTheHeads(t *testing.T) {
	env := newOutboxPG(t)
	ids := env.createAgainstSort(t, 2, []map[string]any{partitioned("g", "p2", 2), partitioned("g", "p1", 1),
		partitioned("g", "p1", 0)})
	p1Head, p1Behind, p2Head := ids[0], ids[1], ids[2]

	records, err := env.claim(fifo(claimClearingRetry("g", "A", 10)))
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{p1Head, p2Head}, rowIDsOf(records), "the heads of p1 and p2, in Sort order")
	env.requireRetryCleared(t, ids, records)

	behind, err := env.claim(fifo(claimClearingRetry("g", "B", 10)))
	require.NoError(t, err)
	assert.Empty(t, behind, "the row behind p1's held head waits")
	assert.Equal(t, 1, env.eavCount(t, p1Behind, outboxNextAttemptID))
}

// markFailed is LTFlow's MarkFailed as #653 specifies it: a CAS on the
// claimed generation that counts the attempt, marks the message failed and
// ends the lease by clearing it.
func markFailed(rowID uuid.UUID, generation int64) *forma.EntityOperation {
	return &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: outboxSchemaName, RowID: rowID},
		Preconditions:    &forma.KvCondition{Attr: "lock_generation", Value: fmt.Sprintf("equals:%d", generation)},
		Updates:          map[string]any{"status": "failed"},
		UpdateOps: []forma.UpdateOp{forma.Increment("attempts", 1), forma.Unset("locked_by"),
			forma.Unset("locked_until")},
	}
}

// markFailedIn runs op in a Tx of its own and commits it.
func (e *outboxPG) markFailedIn(op *forma.EntityOperation) error {
	return e.inTx(func(ctx context.Context, tx forma.Tx) error {
		if _, err := tx.Update(ctx, op); err != nil {
			return fmt.Errorf("failed to mark %s failed: %w", op.RowID, err)
		}
		return nil
	})
}

// leaseEnded answers the rows `locked_until is_null` selects.
func (e *outboxPG) leaseEnded(t *testing.T) []uuid.UUID {
	t.Helper()
	result, err := e.em.Query(e.ctx, &forma.QueryRequest{SchemaName: outboxSchemaName,
		Condition: &forma.KvCondition{Attr: "locked_until", Value: "is_null:true"}, Page: 1, ItemsPerPage: 100})
	require.NoError(t, err)
	return rowIDsOf(result.Data)
}

// requireLeaseHeldBy asserts rowID's committed lease: status publishing,
// held by worker under generation, with an expiry.
func (e *outboxPG) requireLeaseHeldBy(t *testing.T, rowID uuid.UUID, worker string, generation int64) {
	t.Helper()
	s := e.stored(t, rowID)
	assert.Equal(t, "publishing", s.status)
	assert.Equal(t, generation, s.generation)
	require.NotNil(t, s.lockedUntil, "the lease keeps its expiry")
	require.NotNil(t, s.lockedBy, "the lease keeps its holder")
	assert.Equal(t, worker, *s.lockedBy)
}

// TestClaimMarkFailedUnsetsTheLease runs MarkFailed as #653 specifies it.
// A stale generation is refused and leaves the lease in place; the current
// one counts the attempt and clears locked_by and locked_until, so
// `locked_until is_null` tells the ended lease from a held one, and the
// failed message is claimable again under the next generation.
func TestClaimMarkFailedUnsetsTheLease(t *testing.T) {
	env := newOutboxPG(t)
	row := env.create(t, pendingMessage("t", 0))
	held := env.create(t, pendingMessage("other", 1))
	env.claimOne(t, "other", "C")
	a := env.claimOne(t, "t", "A")
	require.EqualValues(t, 1, generationOf(t, a))
	require.Empty(t, env.leaseEnded(t), "both messages are leased")
	err := env.markFailedIn(markFailed(row, 0))
	require.ErrorIs(t, err, forma.ErrPreconditionFailed, "generation 0 is stale")
	env.requireLeaseHeldBy(t, row, "A", 1)

	require.NoError(t, env.markFailedIn(markFailed(row, 1)))
	s := env.stored(t, row)
	assert.Equal(t, "failed", s.status)
	assert.EqualValues(t, 1, s.generation, "MarkFailed does not move the generation")
	assert.Nil(t, s.lockedUntil, "locked_until (entity_main bigint_02) is NULL")
	assert.Nil(t, s.lockedBy, "locked_by has no eav_data row")
	got := env.get(t, row)
	assert.EqualValues(t, 1, got["attempts"])
	assert.NotContains(t, got, "locked_until")
	assert.NotContains(t, got, "locked_by")
	assert.Equal(t, []uuid.UUID{row}, env.leaseEnded(t), "locked_until is_null selects the failed message, not the held one")
	env.requireLeaseHeldBy(t, held, "C", 1)

	// The retry is due at once: next_attempt_at is in the past. B takes it
	// under generation 2, and A's MarkFailed, replayed late, is stale.
	b := env.claimOne(t, "t", "B")
	assert.EqualValues(t, 2, generationOf(t, b))
	assert.EqualValues(t, 1, b.Attributes["attempts"])
	err = env.markFailedIn(markFailed(row, 1))
	require.ErrorIs(t, err, forma.ErrPreconditionFailed)
	env.requireLeaseHeldBy(t, row, "B", 2)
	assert.Empty(t, env.leaseEnded(t), "B's lease is in place")
}

// TestClaimRefusesAnUnsetOfAUniqueKeyComponent pins that the claim's
// unique-key rule covers an unset as it covers a Set: clearing a key
// component moves a key entry, so the claim is refused before any SQL, the
// Tx stays usable, and the entry stays held.
func TestClaimRefusesAnUnsetOfAUniqueKeyComponent(t *testing.T) {
	env := setupUnsetKeysPG(t)
	rowID := env.mustCreate(t, unsetKeysSchemaName, unsetKeysDoc())
	entries := env.keyEntries(t)
	sent := env.countStatements(t)
	for _, tc := range unsetKeyCases {
		t.Run(tc.name, func(t *testing.T) {
			err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
				before := sent.Load()
				_, err := tx.Claim(ctx, &forma.ClaimRequest{SchemaName: unsetKeysSchemaName,
					Condition: unsetTagged("note", "n", tc.attr), Limit: 1, UpdateOps: []forma.UpdateOp{forma.Unset(tc.attr)}})
				require.ErrorIs(t, err, forma.ErrInvalidInput)
				message, published := forma.ResolvePublicMessage(err)
				assert.True(t, published, "the refusal publishes its message: %v", err)
				assert.Contains(t, message, fmt.Sprintf("cannot change attribute '%s' of unique key", tc.attr))
				assert.Equal(t, before, sent.Load(), "the refusal sent no statement")
				if _, err := tx.Update(ctx, unsetKeysOp(rowID, map[string]any{"note": "n"})); err != nil {
					return fmt.Errorf("failed to update %s after the refusal: %w", rowID, err)
				}
				return nil
			})
			require.NoError(t, err, "the Tx commits after the refusal")
			assert.Equal(t, entries, env.keyEntries(t), "the entry stays held")
		})
	}
}
