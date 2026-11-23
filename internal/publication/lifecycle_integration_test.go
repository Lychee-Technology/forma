package publication

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestRebaseFollowsTheKindsBetween walks the rebase rules on a live history:
// a flush over a compaction, a compaction refused over a cdc-init, and a
// cdc-init refused over a flush.
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
	since, err := KindsSince(ctx, DB(f.db), f.t, g1, g2)
	require.NoError(t, err)
	require.Equal(t, []Kind{KindCompact}, since)
	require.NoError(t, Rebase(KindFlush, g1, g2, since))
	g3, err := f.publish(ctx, g2, f.next(t, g2, KindFlush), nil)
	require.NoError(t, err, "the rebased flush restages on the head under a fresh token")
	require.Equal(t, int64(2), g3.ParentGeneration)

	g4, err := f.publish(ctx, g3, f.next(t, g3, KindInit), nil)
	require.NoError(t, err)
	require.NotEqual(t, g3.Lineage, g4.Lineage)
	since, err = KindsSince(ctx, DB(f.db), f.t, g3, g4)
	require.NoError(t, err)
	require.ErrorIs(t, Rebase(KindCompact, g3, g4, since), ErrRebaseRefused)

	g5, err := f.publish(ctx, g4, f.next(t, g4, KindFlush), nil)
	require.NoError(t, err)
	since, err = KindsSince(ctx, DB(f.db), f.t, g4, g5)
	require.NoError(t, err)
	require.ErrorIs(t, Rebase(KindInit, g4, g5, since), ErrRebaseRefused)

	// History the collector already removed is a refusal, not "nothing
	// happened in between".
	f.forget(t, 2)
	_, err = KindsSince(ctx, DB(f.db), f.t, g1, g5)
	require.ErrorIs(t, err, ErrRebaseRefused)
}

// TestRebaseRefusesAFlushAcrossAnInit is the resurrection sequence: a flush
// exports a row live, a rival flush publishes and marks the row's deletion,
// and a cdc-init replaces the inventory with the live rows, without the
// tombstone. Had the stale flush rebased across both, its live copy would be
// the row's only parquet version while the deletion entry is clean, so no
// dirty barrier would hide it. Over a flush and a compaction the tombstone
// stays in the inventory, and the rebase is allowed.
func TestRebaseRefusesAFlushAcrossAnInit(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	written := f.write(t, 100, 1)
	g1, err := f.publish(ctx, f.initialized(t, 100), f.next(t, f.read(t, 100), KindFlush), written)
	require.NoError(t, err)
	var row uuid.UUID
	for id := range written {
		row = id
	}

	f.change(t, 100, row, 2000, false)
	stale := f.next(t, g1, KindFlush) // exports row@2000, live, then loses its lease
	f.change(t, 100, row, 3000, true)
	g2, err := f.publish(ctx, g1, f.next(t, g1, KindFlush), map[uuid.UUID]int64{row: 3000})
	require.NoError(t, err, "the rival flush publishes the tombstone and marks the deletion")
	g3, err := f.publish(ctx, g2, f.next(t, g2, KindCompact), nil)
	require.NoError(t, err)

	_, err = f.publish(ctx, g1, stale, map[uuid.UUID]int64{row: 2000})
	require.ErrorIs(t, err, ErrRecordMoved)
	since, err := KindsSince(ctx, DB(f.db), f.t, g1, g3)
	require.NoError(t, err)
	require.NoError(t, Rebase(KindFlush, g1, g3, since), "a compaction keeps the tombstone, which outranks the stale copy")

	g4, err := f.publish(ctx, g3, f.next(t, g3, KindInit), nil)
	require.NoError(t, err, "the init exports live rows only: no copy of the deleted row")
	_, now := f.snapshot(t, 100)
	require.Equal(t, view{State: Published, Generation: 4, Dirty: 0}, now(), "the deletion entry is clean, so nothing would hide a resurrected copy")
	since, err = KindsSince(ctx, DB(f.db), f.t, g1, g4)
	require.NoError(t, err)
	require.Equal(t, []Kind{KindFlush, KindCompact, KindInit}, since)
	require.ErrorIs(t, Rebase(KindFlush, g1, g4, since), ErrRebaseRefused)
}

// TestParentIsTheCASPredecessor pins parent_generation's one meaning in the
// record and the history: the generation the compare-and-set replaced in the
// same epoch, and 0 for an epoch's first. A rebased compaction's parent is its
// new predecessor, not the generation it read its input from, and the first
// publication after a drop and re-registration has none.
func TestParentIsTheCASPredecessor(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	g1, err := f.publish(ctx, f.initialized(t, 100), f.next(t, f.read(t, 100), KindFlush), nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), g1.ParentGeneration)

	compact := f.next(t, g1, KindCompact) // its input is generation 1
	g2, err := f.publish(ctx, g1, f.next(t, g1, KindFlush), nil)
	require.NoError(t, err)
	_, err = f.publish(ctx, g1, compact, nil)
	require.True(t, Conflict(err))
	since, err := KindsSince(ctx, DB(f.db), f.t, g1, g2)
	require.NoError(t, err)
	require.NoError(t, Rebase(KindCompact, g1, g2, since))
	g3, err := f.publish(ctx, g2, f.next(t, g2, KindCompact), nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), g3.ParentGeneration, "the rebased compaction's parent is the generation it replaced")

	dropped := f.reset(t, g3)
	active := f.activate(t, dropped)
	require.Equal(t, int64(0), active.ParentGeneration)
	g4, err := f.publish(ctx, active, f.next(t, active, KindFlush), nil)
	require.NoError(t, err)
	require.Equal(t, int64(4), g4.Generation, "the counter carries over the drop")
	require.Equal(t, int64(0), g4.ParentGeneration, "the epoch's first generation has no parent, whatever the counter held")
	require.Equal(t, g4, f.read(t, 100))

	parents := map[int64]int64{}
	for _, g := range mustRetained(t, f, time.Hour) {
		parents[g.Generation] = g.ParentGeneration
	}
	require.Equal(t, map[int64]int64{1: 0, 2: 1, 3: 2, 4: 0}, parents)
}

// TestResetFencesEveryWriterOfTheOldEpoch pins the drop transition: the
// record is kept and becomes DROPPED at the next epoch. No reader answers
// from it, no writer or handoff of the old epoch commits, and neither does a
// publisher that reads the record only after the drop. Re-registration cannot
// activate the id while the dropped schema's rows remain, and activation
// starts yet another epoch under a new token; generation numbers are never
// reused.
func TestResetFencesEveryWriterOfTheOldEpoch(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	rows := f.write(t, 100, 2)
	g1, err := f.publish(ctx, f.initialized(t, 100), f.next(t, f.read(t, 100), KindFlush), rows)
	require.NoError(t, err)
	staged := f.next(t, g1, KindFlush)
	f.write(t, 100, 1)

	dropped := f.reset(t, g1)
	require.Equal(t, Record{SchemaID: 100, Epoch: 2, State: Dropped, Generation: 1, Token: dropped.Token, UpdatedAt: dropped.UpdatedAt}, dropped)
	require.Equal(t, dropped, f.read(t, 100))
	require.ErrorIs(t, dropped.Readable(), ErrDropped, "a reader with the old schema's metadata fails closed while its rows remain")

	_, err = f.publish(ctx, g1, staged, nil)
	require.ErrorIs(t, err, ErrRecordMoved, "a writer staged in epoch 1 cannot publish after the drop")
	require.ErrorIs(t, Rebase(KindFlush, g1, dropped, nil), ErrRebaseRefused)
	_, err = f.publish(ctx, dropped, f.next(t, dropped, KindFlush), nil)
	require.ErrorIs(t, err, ErrDropped, "a publisher that reads the record after the drop stages nothing from the old rows")
	require.NoError(t, f.inTx(ctx, func(tx Tx) error {
		_, err := HoldLineage(ctx, tx, f.t, 100, g1.Epoch, g1.Lineage)
		require.ErrorIs(t, err, ErrLineageReplaced)
		return nil
	}))
	require.NoError(t, f.inTx(ctx, func(tx Tx) error {
		_, err := Reset(ctx, tx, f.t, dropped)
		require.ErrorIs(t, err, ErrDropped)
		return nil
	}))

	again, err := f.initialize(t, 100)
	require.NoError(t, err)
	require.False(t, again, "Initialize leaves a dropped record dropped")
	require.Equal(t, dropped, f.read(t, 100))
	require.ErrorIs(t, f.inTx(ctx, func(tx Tx) error {
		_, err := Activate(ctx, tx, f.t, dropped, []Residue{{Table: f.changeLog}})
		return err
	}), ErrResidue, "the id is not registered again while the dropped schema's rows remain")

	_, err = f.db.Exec("DELETE FROM "+f.changeLog+" WHERE schema_id = $1", 100)
	require.NoError(t, err)
	active := f.activate(t, dropped)
	require.Equal(t, Record{SchemaID: 100, Epoch: 3, State: Unpublished, Generation: 1, Token: active.Token, UpdatedAt: active.UpdatedAt}, active)
	require.NotEqual(t, dropped.Token, active.Token)
	require.NoError(t, active.Readable())
	_, err = f.publish(ctx, dropped, f.next(t, dropped, KindFlush), nil)
	require.ErrorIs(t, err, ErrDropped, "what was read while the schema was dropped never publishes into the new epoch")

	g2, err := f.publish(ctx, active, f.next(t, active, KindFlush), nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), g2.Generation, "the first publication of epoch 3 does not reuse generation 1")
	require.Equal(t, int64(3), g2.Epoch)

	gens := mustRetained(t, f, time.Hour)
	require.Len(t, gens, 2)
	require.NotZero(t, gens[0].SupersededAt, "the reset superseded epoch 1's generation")
	require.Zero(t, gens[0].RetiredAt, "no collector retired it yet")
	require.Zero(t, gens[1].SupersededAt)
}

// TestAwaitingInitAdmitsOnlyACDCInit pins the state init-db recovers a lost
// record with generation history in: readers fail closed, flushes and
// compactions are refused, and the next generation is a cdc-init's.
func TestAwaitingInitAdmitsOnlyACDCInit(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	require.NoError(t, f.inTx(ctx, func(tx Tx) error {
		ok, err := insertRecord(ctx, tx, f.t, 100, AwaitingInit)
		require.True(t, ok)
		return err
	}))
	cur := f.read(t, 100)
	require.Equal(t, AwaitingInit, cur.State)
	require.ErrorIs(t, cur.Readable(), ErrAwaitingInit)
	again, err := f.initialize(t, 100)
	require.NoError(t, err)
	require.False(t, again, "registration does not turn an AWAITING_INIT record into an unpublished one")

	_, err = f.publish(ctx, cur, f.next(t, cur, KindFlush), nil)
	require.ErrorContains(t, err, "only a cdc-init")
	_, err = f.publish(ctx, cur, f.next(t, cur, KindCompact), nil)
	require.ErrorContains(t, err, "only a cdc-init")
	require.Equal(t, cur, f.read(t, 100))

	g1, err := f.publish(ctx, cur, f.next(t, cur, KindInit), nil)
	require.NoError(t, err)
	require.Equal(t, Published, g1.State)
	require.Equal(t, int64(0), g1.ParentGeneration)
	require.NoError(t, f.read(t, 100).Readable())
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
	gen, err := HoldLineage(ctx, SQLTx(tx), f.t, 100, g1.Epoch, g1.Lineage)
	require.NoError(t, err)
	require.Equal(t, int64(1), gen)
	require.NoError(t, f.mark(ctx, tx, 100, rows))
	require.NoError(t, tx.Commit())
	_, now := f.snapshot(t, 100)
	require.Equal(t, view{State: Published, Generation: 1, Dirty: 0}, now())

	g2, err := f.publish(ctx, g1, f.next(t, g1, KindCompact), nil)
	require.NoError(t, err)
	require.NoError(t, f.inTx(ctx, func(tx Tx) error {
		_, err := HoldLineage(ctx, tx, f.t, 100, g1.Epoch, g1.Lineage)
		require.NoError(t, err, "a compaction keeps the lineage, so the handoff goes on")
		return nil
	}))

	_, err = f.publish(ctx, g2, f.next(t, g2, KindInit), nil)
	require.NoError(t, err)
	require.NoError(t, f.inTx(ctx, func(tx Tx) error {
		_, err := HoldLineage(ctx, tx, f.t, 100, g1.Epoch, g1.Lineage)
		require.ErrorIs(t, err, ErrLineageReplaced)
		return nil
	}))
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
	_, err = HoldLineage(ctx, SQLTx(handoff), f.t, 100, g1.Epoch, g1.Lineage)
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
