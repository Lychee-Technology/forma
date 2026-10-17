package coordinationtest

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// #593's claim tests, re-run through the adapter's outbox, with special
// scenarios 6 to 9 among them.

func TestClaimExclusivity(t *testing.T) {
	t.Run("two concurrent claimers never receive the same record (scenario 6)", func(t *testing.T) {
		e := newEnv(t)
		// A head an earlier round left unclaimed is still its partition's
		// head. Claims that skip held rows take every head of a round; on an
		// optimistic backend the losers of a round claim nothing (#633).
		var heads []uuid.UUID
		for _, relays := range []int{2, 8} {
			for iteration := 0; iteration < 10; iteration++ {
				heads = append(heads, ids(firsts(e.seedOutbox(t, 10, 2)))...)
				// Each round's held messages stay live, so the statistics
				// must follow the outbox.
				e.analyze(t)
				claimed := make([][]OutboxMessage, relays)
				errs := race(relays, func(i int) error {
					return e.inTx(func(uow UnitOfWork) error {
						var err error
						claimed[i], err = uow.Outbox().ClaimBatch(e.ctx, fmt.Sprintf("relay-%d", i), time.Hour, 5)
						return err
					})
				})
				requireDisjointClaims(t, e, heads, claimed, errs)
				total := committedClaims(claimed, errs)
				require.NotZero(t, total, "%d relays, iteration %d: a claim committed", relays, iteration)
				t.Run(fmt.Sprintf("mechanism: %d relays, iteration %d, no ErrRetryableConflict", relays, iteration), func(t *testing.T) {
					requirePostgresMechanisms(t)
					require.Zero(t, retryable(errs), "errors %v", errs)
					// Every head is try-locked by some claimer, and a claimer
					// that took k of them claims min(k, 5): at least 5 in all.
					require.GreaterOrEqual(t, total, 5, "the relays claimed at least one limit's worth of the 10 heads")
				})
			}
		}
	})

	t.Run("each eligible record is returned to at most one active claimant, and partitions publish in order", func(t *testing.T) {
		e := newEnv(t)
		// Half the partitions enqueue all their messages at one instant, so
		// their order is the row-ID tiebreak's.
		partitions := append(e.seedOutbox(t, 10, 3), e.seedTiedOutbox(t, 10, 3)...)
		run := newRelayRun(e, 60)
		for i, err := range run.run(4) {
			require.NoError(t, err, "relay %d", i)
		}
		requireDrained(t, e, run, partitions)
		t.Run("mechanism: no ErrRetryableConflict", func(t *testing.T) {
			requirePostgresMechanisms(t)
			require.Zero(t, run.retryable)
		})
	})

	t.Run("concurrent claim tests run against real PostgreSQL", concurrentClaimsOnPostgres)
}

func TestClaimResults(t *testing.T) {
	t.Run("claimed rows are updated before they are returned", func(t *testing.T) {
		e := newEnv(t)
		msg := e.seedOutbox(t, 1, 1)[0][0]
		before := e.stamps(t, OutboxSchema, msg.ID)
		start := time.Now()
		got := e.claimBatch(t, "relay-a", time.Hour, 5)
		require.Len(t, got, 1)
		c := got[0]
		require.Equal(t, msg.ID, c.ID)
		require.Equal(t, OutboxPublishing, c.Status)
		require.Equal(t, "relay-a", c.LockedBy)
		require.EqualValues(t, 1, c.LockGeneration)
		require.NotNil(t, c.LockedUntil)
		require.WithinDuration(t, start.Add(time.Hour), *c.LockedUntil, time.Minute, "the lease is the Tx clock plus an hour")
		require.Equal(t, map[string]any{"status": OutboxPublishing, "locked_by": "relay-a", "lock_generation": int64(1),
			"locked_until": c.LockedUntil.UnixMilli()},
			e.stored(t, OutboxSchema, c.ID, "status", "locked_by", "lock_generation", "locked_until"), "returned equals committed")
		e.requireWritten(t, OutboxSchema, before, c.ID)
	})

	t.Run("ordering and limit are deterministic", func(t *testing.T) {
		e := newEnv(t)
		base := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
		// Six partition heads whose row IDs descend as enqueued_at ascends.
		msgs := make([]OutboxMessage, 6)
		descending := ascendingIDs(len(msgs))
		slices.Reverse(descending)
		for i := range msgs {
			msgs[i] = message(fmt.Sprintf("partition-%d", i), base.Add(time.Duration(i)*time.Millisecond))
			msgs[i].ID = descending[i]
		}
		require.NoError(t, e.inTx(func(uow UnitOfWork) error { return uow.Outbox().Insert(e.ctx, msgs) }))
		require.Equal(t, ids(msgs[:4]), ids(e.claimBatch(t, "relay-a", time.Hour, 4)), "the 4 earliest, in enqueued order")
		require.Equal(t, ids(msgs[4:]), ids(e.claimBatch(t, "relay-b", time.Hour, 4)), "the rest, in enqueued order")
		require.Empty(t, e.claimBatch(t, "relay-c", time.Hour, 4))
	})

	t.Run("an enqueued_at tie is broken by row ID, across heads and within a partition", func(t *testing.T) {
		e := newEnv(t)
		partitions := e.seedTiedOutbox(t, 3, 2)
		for place := 0; place < 2; place++ {
			want := make([]OutboxMessage, len(partitions))
			for p, msgs := range partitions {
				want[p] = msgs[place]
			}
			slices.SortFunc(want, byRowID)
			got := e.claimBatch(t, "relay-a", time.Hour, len(partitions))
			require.Equal(t, ids(want), ids(got), "each partition's message %d, in row-ID order", place)
			require.NoError(t, e.inTx(func(uow UnitOfWork) error {
				for _, m := range got {
					if err := uow.Outbox().MarkPublished(e.ctx, m.ID, m.LockGeneration); err != nil {
						return err
					}
				}
				return nil
			}))
		}
	})
}

func TestClaimLeases(t *testing.T) {
	t.Run("expired leases can be reclaimed (scenario 7)", func(t *testing.T) {
		e := newEnv(t)
		e.seedOutbox(t, 1, 1)
		a := e.claimBatch(t, "relay-a", time.Hour, 1)
		require.Len(t, a, 1)
		e.expireLease(t, a[0])
		before := e.stamps(t, OutboxSchema, a[0].ID)
		b := e.claimBatch(t, "relay-b", time.Hour, 1)
		require.Len(t, b, 1, "the expired lease is claimable")
		e.requireWritten(t, OutboxSchema, before, b[0].ID)
		require.Equal(t, a[0].ID, b[0].ID)
		require.EqualValues(t, 2, b[0].LockGeneration)
		require.Equal(t, map[string]any{"locked_by": "relay-b", "lock_generation": int64(2)},
			e.stored(t, OutboxSchema, b[0].ID, "locked_by", "lock_generation"))
	})

	t.Run("active leases cannot be reclaimed", func(t *testing.T) {
		e := newEnv(t)
		e.seedOutbox(t, 1, 1)
		a := e.claimBatch(t, "relay-a", time.Hour, 1)
		require.Len(t, a, 1)
		require.Empty(t, e.claimBatch(t, "relay-b", time.Hour, 1), "the lease is live")
		require.Equal(t, map[string]any{"locked_by": "relay-a", "lock_generation": int64(1)},
			e.stored(t, OutboxSchema, a[0].ID, "locked_by", "lock_generation"))
	})

	t.Run("stale generation/fencing updates fail (scenario 8)", func(t *testing.T) {
		e := newEnv(t)
		seeded := []OutboxMessage{message("partition-a", time.Now().Add(-time.Hour))}
		seeded[0].LockGeneration = 3
		require.NoError(t, e.inTx(func(uow UnitOfWork) error { return uow.Outbox().Insert(e.ctx, seeded) }))
		seed := seeded[0]
		a := e.claimBatch(t, "relay-a", time.Hour, 1)
		require.Len(t, a, 1)
		require.EqualValues(t, 4, a[0].LockGeneration)
		e.expireLease(t, a[0])
		b := e.claimBatch(t, "relay-b", time.Hour, 1)
		require.Len(t, b, 1)
		require.EqualValues(t, 5, b[0].LockGeneration)
		entries := e.changeLogEntries(t, OutboxSchema, seed.ID)
		for name, stale := range map[string]func(OutboxStore) error{
			"MarkPublished":  func(o OutboxStore) error { return o.MarkPublished(e.ctx, seed.ID, 4) },
			"MarkFailed":     func(o OutboxStore) error { return o.MarkFailed(e.ctx, seed.ID, 4, "late", time.Now()) },
			"MarkDeadLetter": func(o OutboxStore) error { return o.MarkDeadLetter(e.ctx, seed.ID, 4, "late") },
		} {
			err := e.inTx(func(uow UnitOfWork) error { return stale(uow.Outbox()) })
			require.ErrorIs(t, err, forma.ErrPreconditionFailed, "relay A's %s at generation 4", name)
		}
		require.Equal(t, entries, e.changeLogEntries(t, OutboxSchema, seed.ID), "no stale completion wrote")
		before := e.stamps(t, OutboxSchema, seed.ID)
		require.NoError(t, e.inTx(func(uow UnitOfWork) error { return uow.Outbox().MarkPublished(e.ctx, seed.ID, 5) }))
		e.requireWritten(t, OutboxSchema, before, seed.ID)
		require.Equal(t, map[string]any{"status": OutboxPublished, "locked_by": "relay-b"},
			e.stored(t, OutboxSchema, seed.ID, "status", "locked_by"))
	})

	t.Run("rollback releases claimed rows (scenario 9)", func(t *testing.T) {
		e := newEnv(t)
		msg := e.seedOutbox(t, 1, 1)[0][0]
		entries := e.changeLogEntries(t, OutboxSchema, msg.ID)
		abort := errors.New("the relay aborts after claiming")
		err := e.inTx(func(uow UnitOfWork) error {
			got, err := uow.Outbox().ClaimBatch(e.ctx, "relay-a", time.Hour, 1)
			if err != nil {
				return err
			}
			if len(got) != 1 {
				return fmt.Errorf("claimed %d messages, want 1", len(got))
			}
			return abort
		})
		require.ErrorIs(t, err, abort)
		require.Equal(t, map[string]any{"status": OutboxPending, "lock_generation": int64(0), "locked_by": ""},
			e.stored(t, OutboxSchema, msg.ID, "status", "lock_generation", "locked_by"))
		require.Equal(t, entries, e.changeLogEntries(t, OutboxSchema, msg.ID))
		again := e.claimBatch(t, "relay-b", time.Hour, 1)
		require.Len(t, again, 1, "the message is claimable again")
		require.EqualValues(t, 1, again[0].LockGeneration, "the generation is unchanged by the rolled-back claim")
	})

}

func concurrentClaimsOnPostgres(t *testing.T) {
	requirePostgresMechanisms(t)
	e := newEnv(t)
	var version string
	require.NoError(t, e.pool.QueryRow(e.ctx, "SELECT version()").Scan(&version))
	require.True(t, strings.HasPrefix(version, "PostgreSQL "), "the backend is %q", version)
	heads := ids(firsts(e.seedOutbox(t, 40, 2)))
	claimed := make([][]OutboxMessage, 8)
	errs := race(8, func(i int) error {
		return e.inTx(func(uow UnitOfWork) error {
			var err error
			claimed[i], err = uow.Outbox().ClaimBatch(e.ctx, fmt.Sprintf("relay-%d", i), time.Hour, 5)
			return err
		})
	})
	require.Zero(t, retryable(errs))
	requireDisjointClaims(t, e, heads, claimed, errs)
}

func TestClaimBatchByTopics(t *testing.T) {
	e := newEnv(t)
	base := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	// Partition a holds billing only; partition b an orders head, then
	// billing behind it.
	msgs := []OutboxMessage{message("a", base), message("b", base.Add(time.Millisecond)), message("b", base.Add(2*time.Millisecond))}
	msgs[0].Topic, msgs[2].Topic = "billing", "billing"
	require.NoError(t, e.inTx(func(uow UnitOfWork) error { return uow.Outbox().Insert(e.ctx, msgs) }))
	claimTopics := func(topics ...string) []OutboxMessage {
		var got []OutboxMessage
		require.NoError(t, e.inTx(func(uow UnitOfWork) error {
			var err error
			got, err = uow.Outbox().ClaimBatchByTopics(e.ctx, "relay-a", time.Hour, 5, topics)
			return err
		}))
		return got
	}

	require.Empty(t, claimTopics(), "no topics claims nothing")
	require.Equal(t, ids(msgs[:1]), ids(claimTopics("billing")),
		"billing's head in a; b's billing message waits behind b's orders head")
	require.Equal(t, ids(msgs[1:2]), ids(claimTopics("orders", "billing")), "b's head")
	require.Empty(t, claimTopics("billing"), "b's billing message still waits: its head is held")
	require.Equal(t, map[string]any{"status": OutboxPending, "lock_generation": int64(0)},
		e.stored(t, OutboxSchema, msgs[2].ID, "status", "lock_generation"))
}

// committedClaims counts the messages the committed claims took.
func committedClaims(claimed [][]OutboxMessage, errs []error) int {
	n := 0
	for i, msgs := range claimed {
		if errs[i] == nil {
			n += len(msgs)
		}
	}
	return n
}

// byRowID orders messages by row ID, the claim order's final tiebreak.
func byRowID(a, b OutboxMessage) int { return bytes.Compare(a.ID[:], b.ID[:]) }

// firsts answers each partition's first message.
func firsts(partitions [][]OutboxMessage) []OutboxMessage {
	out := make([]OutboxMessage, len(partitions))
	for i, p := range partitions {
		out[i] = p[0]
	}
	return out
}

// requireDisjointClaims asserts that no message is in two committed claims,
// that every claimed message is a partition head, and that each is stored
// as its claimer's. A claim that failed is ErrRetryableConflict, and a loser
// claims nothing.
func requireDisjointClaims(t *testing.T, e *env, heads []uuid.UUID, claimed [][]OutboxMessage, errs []error) {
	t.Helper()
	owner := map[uuid.UUID]int{}
	for i, msgs := range claimed {
		if errs[i] != nil {
			require.ErrorIs(t, errs[i], forma.ErrRetryableConflict)
			continue
		}
		for _, m := range msgs {
			prev, twice := owner[m.ID]
			require.False(t, twice, "message %s claimed by relays %d and %d", m.ID, prev, i)
			owner[m.ID] = i
			require.Contains(t, heads, m.ID, "only partition heads are claimable")
			require.Equal(t, map[string]any{"locked_by": fmt.Sprintf("relay-%d", i), "status": OutboxPublishing},
				e.stored(t, OutboxSchema, m.ID, "locked_by", "status"))
		}
	}
}

// requireDrained asserts a relay run's end state: every message published
// once, each partition in enqueued order, and nothing left unresolved.
func requireDrained(t *testing.T, e *env, run *relayRun, partitions [][]OutboxMessage) {
	t.Helper()
	all := flatten(partitions)
	require.Len(t, run.published, len(all), "every message is published exactly once")
	for p, msgs := range partitions {
		order := make([]int, len(msgs))
		for i, m := range msgs {
			order[i] = run.published[m.ID]
		}
		require.True(t, slices.IsSorted(order), "partition %d publishes in enqueued order: %v", p, order)
	}
	require.ElementsMatch(t, ids(all), e.liveIDs(t, OutboxSchema, "status", OutboxPublished))
	t.Logf("%d claims of %d messages, %d failed once, %d ErrRetryableConflict", run.claimed, len(all), len(run.failed), run.retryable)
}
