package publication

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestRebaseFollowsTheKindsBetween walks the three rebase rules on a live
// history: a flush over a compaction, a compaction refused over a cdc-init,
// and a cdc-init refused over a flush.
func TestRebaseFollowsTheKindsBetween(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	g1, err := f.publish(ctx, f.initialized(t, 100), f.next(t, f.read(t, 100), KindFlush), nil)
	require.NoError(t, err)

	flush := f.next(t, g1, KindFlush)
	g2, err := f.publish(ctx, g1, f.next(t, g1, KindCompact), nil)
	require.NoError(t, err)
	_, err = f.publish(ctx, g1, flush, nil)
	require.True(t, Conflict(err))
	since, err := KindsSince(ctx, f.db, f.t, g1, g2)
	require.NoError(t, err)
	require.Equal(t, []Kind{KindCompact}, since)
	require.NoError(t, Rebase(KindFlush, g1, g2, since))
	g3, err := f.publish(ctx, g2, f.next(t, g2, KindFlush), nil)
	require.NoError(t, err, "the rebased flush restages on the head under a fresh token")
	require.Equal(t, int64(2), g3.ParentGeneration)

	g4, err := f.publish(ctx, g3, f.next(t, g3, KindInit), nil)
	require.NoError(t, err)
	require.NotEqual(t, g3.Lineage, g4.Lineage)
	since, err = KindsSince(ctx, f.db, f.t, g3, g4)
	require.NoError(t, err)
	require.ErrorIs(t, Rebase(KindCompact, g3, g4, since), ErrRebaseRefused)

	g5, err := f.publish(ctx, g4, f.next(t, g4, KindFlush), nil)
	require.NoError(t, err)
	since, err = KindsSince(ctx, f.db, f.t, g4, g5)
	require.NoError(t, err)
	require.ErrorIs(t, Rebase(KindInit, g4, g5, since), ErrRebaseRefused)

	// History the collector already removed is a refusal, not "nothing
	// happened in between".
	ok, err := Forget(ctx, f.db, f.t, 100, 2, 0)
	require.NoError(t, err)
	require.True(t, ok)
	_, err = KindsSince(ctx, f.db, f.t, g1, g5)
	require.ErrorIs(t, err, ErrRebaseRefused)
}

// TestResetFencesEveryWriterOfTheOldEpoch pins the drop transition: the
// record is kept, its epoch moves on, and no writer or handoff of the old
// epoch commits; generation numbers are never reused.
func TestResetFencesEveryWriterOfTheOldEpoch(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	g1, err := f.publish(ctx, f.initialized(t, 100), f.next(t, f.read(t, 100), KindFlush), nil)
	require.NoError(t, err)
	staged := f.next(t, g1, KindFlush)

	tx, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	reset, err := Reset(ctx, tx, f.t, g1)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.Equal(t, Record{SchemaID: 100, Epoch: 2, State: Unpublished, Generation: 1, ParentGeneration: 1, Token: reset.Token, UpdatedAt: reset.UpdatedAt}, reset)
	require.Equal(t, reset, f.read(t, 100))

	_, err = f.publish(ctx, g1, staged, nil)
	require.ErrorIs(t, err, ErrRecordMoved, "a writer staged in epoch 1 cannot publish into epoch 2")
	require.ErrorIs(t, Rebase(KindFlush, g1, reset, nil), ErrRebaseRefused)
	hold, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = HoldLineage(ctx, hold, f.t, 100, g1.Epoch, g1.Lineage)
	require.ErrorIs(t, err, ErrLineageReplaced)
	require.NoError(t, hold.Rollback())

	again, err := Initialize(ctx, f.db, f.t, 100)
	require.NoError(t, err)
	require.False(t, again, "re-registration keeps the record and its epoch")
	g2, err := f.publish(ctx, reset, f.next(t, reset, KindFlush), nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), g2.Generation, "the first publication of epoch 2 does not reuse generation 1")
	require.Equal(t, int64(2), g2.Epoch)

	gens, err := Retained(ctx, f.db, f.t, 100, time.Hour)
	require.NoError(t, err)
	require.Len(t, gens, 2)
	require.NotZero(t, gens[0].SupersededAt, "the reset superseded epoch 1's generation")
	require.Zero(t, gens[1].SupersededAt)
}

// TestResolveAfterALostCommitAnswer pins the commit-outcome-unknown path: the
// writer resends the same publication, and whichever copy commits, Resolve
// tells it so.
func TestResolveAfterALostCommitAnswer(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	cur := f.initialized(t, 100)
	next := f.next(t, cur, KindFlush)

	outcome, err := Resolve(ctx, f.db, f.t, 100, 1, next.Token)
	require.NoError(t, err)
	require.Equal(t, NotCommitted, outcome)

	_, err = f.publish(ctx, cur, next, nil) // its answer is "lost"
	require.NoError(t, err)
	_, err = f.publish(ctx, cur, next, nil) // the resend
	require.True(t, Conflict(err))
	outcome, err = Resolve(ctx, f.db, f.t, 100, 1, next.Token)
	require.NoError(t, err)
	require.Equal(t, Committed, outcome)

	outcome, err = Resolve(ctx, f.db, f.t, 100, 1, uuid.New())
	require.NoError(t, err)
	require.Equal(t, Lost, outcome, "a rival's token at the number means this writer's copy can never commit")
}

// TestHoldLineageFencesTheHandoff pins the cdc-init handoff: its marks commit
// only while the lineage they mark for is the record's.
func TestHoldLineageFencesTheHandoff(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	rows := f.write(t, 100, 2)
	g1, err := f.publish(ctx, f.initialized(t, 100), f.next(t, f.read(t, 100), KindInit), nil)
	require.NoError(t, err)

	tx, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	gen, err := HoldLineage(ctx, tx, f.t, 100, g1.Epoch, g1.Lineage)
	require.NoError(t, err)
	require.Equal(t, int64(1), gen)
	require.NoError(t, f.mark(ctx, tx, 100, rows))
	require.NoError(t, tx.Commit())
	_, now := f.snapshot(t, 100)
	require.Equal(t, view{State: Published, Generation: 1, Dirty: 0}, now())

	g2, err := f.publish(ctx, g1, f.next(t, g1, KindCompact), nil)
	require.NoError(t, err)
	tx, err = f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = HoldLineage(ctx, tx, f.t, 100, g1.Epoch, g1.Lineage)
	require.NoError(t, err, "a compaction keeps the lineage, so the handoff goes on")
	require.NoError(t, tx.Rollback())

	_, err = f.publish(ctx, g2, f.next(t, g2, KindInit), nil)
	require.NoError(t, err)
	tx, err = f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = HoldLineage(ctx, tx, f.t, 100, g1.Epoch, g1.Lineage)
	require.ErrorIs(t, err, ErrLineageReplaced)
}

// TestHoldLineageHoldsOffAReplacement races a handoff transaction against the
// cdc-init that replaces its lineage. On PostgreSQL the init's compare-and-set
// waits for the handoff to end and commits after its marks. On a DSQL cluster
// nothing waits: one of the two fails with 40001 at COMMIT, which this test
// cannot stage.
func TestHoldLineageHoldsOffAReplacement(t *testing.T) {
	f := newFixture(t)
	if f.cluster {
		t.Skip("holding off the replacement needs a row lock wait, which DSQL does not have")
	}
	ctx := context.Background()
	rows := f.write(t, 100, 2)
	g1, err := f.publish(ctx, f.initialized(t, 100), f.next(t, f.read(t, 100), KindInit), nil)
	require.NoError(t, err)

	handoff, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = handoff.Rollback() }()
	_, err = HoldLineage(ctx, handoff, f.t, 100, g1.Epoch, g1.Lineage)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		_, err := f.publish(ctx, g1, f.next(t, g1, KindInit), nil)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("the replacing cdc-init finished while the handoff held the lineage: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	require.NoError(t, f.mark(ctx, handoff, 100, rows))
	require.NoError(t, handoff.Commit())

	err = <-done
	if err != nil {
		require.True(t, Conflict(err), "the waiting init may only lose the race, never fail otherwise: %v", err)
		return
	}
	require.Equal(t, int64(2), f.read(t, 100).Generation)
}
