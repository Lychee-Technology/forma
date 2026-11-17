package publication

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestResolveAfterALostCommitAnswer pins the commit-outcome-unknown path: the
// writer resends the same publication, and whichever copy commits, Resolve
// tells it so.
func TestResolveAfterALostCommitAnswer(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	cur := f.initialized(t, 100)
	next := f.next(t, cur, KindFlush)

	outcome, err := Resolve(ctx, DB(f.db), f.t, cur, next)
	require.NoError(t, err)
	require.Equal(t, NotCommitted, outcome)

	_, err = f.publish(ctx, cur, next, nil) // its answer is "lost"
	require.NoError(t, err)
	_, err = f.publish(ctx, cur, next, nil) // the resend
	require.True(t, Conflict(err))
	outcome, err = Resolve(ctx, DB(f.db), f.t, cur, next)
	require.NoError(t, err)
	require.Equal(t, Committed, outcome)

	rival := next
	rival.Token = uuid.New()
	outcome, err = Resolve(ctx, DB(f.db), f.t, cur, rival)
	require.NoError(t, err)
	require.Equal(t, Lost, outcome, "a rival's token at the number means this writer's copy can never commit")
}

// TestResolveTellsAForgottenOutcomeFromAnUncommittedOne pins Resolve after an
// outage that outlasted the collector's grace: the attempt's generation
// committed, was superseded, and its history row was forgotten. Resolve never
// answers NotCommitted for it, because the record read in the same snapshot
// has moved past the attempt's expectation. It answers Committed when the
// record still carries the fresh lineage only the attempt introduced, and
// Unknown otherwise; a resend after Unknown can never commit.
func TestResolveTellsAForgottenOutcomeFromAnUncommittedOne(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	g0 := f.initialized(t, 100)
	first := f.next(t, g0, KindFlush) // a fresh lineage: the schema was unpublished
	g1, err := f.publish(ctx, g0, first, nil)
	require.NoError(t, err)
	second := f.next(t, g1, KindFlush) // keeps the lineage
	g2, err := f.publish(ctx, g1, second, nil)
	require.NoError(t, err)
	g3, err := f.publish(ctx, g2, f.next(t, g2, KindFlush), nil)
	require.NoError(t, err)
	f.forget(t, 1)
	f.forget(t, 2)

	outcome, err := Resolve(ctx, DB(f.db), f.t, g1, second)
	require.NoError(t, err)
	require.Equal(t, Unknown, outcome, "generation 2's row is gone and the record moved on: the outcome cannot be recovered")
	_, err = f.publish(ctx, g1, second, nil)
	require.ErrorIs(t, err, ErrRecordMoved, "an Unknown attempt can never commit, so the writer starts over from the head")

	outcome, err = Resolve(ctx, DB(f.db), f.t, g0, first)
	require.NoError(t, err)
	require.Equal(t, Committed, outcome, "the record still carries the lineage only the first flush introduced")

	pending := f.next(t, g3, KindFlush)
	outcome, err = Resolve(ctx, DB(f.db), f.t, g3, pending)
	require.NoError(t, err)
	require.Equal(t, NotCommitted, outcome, "the record still at the expectation is the one answer that permits a resend")

	g4, err := f.publish(ctx, g3, f.next(t, g3, KindInit), nil)
	require.NoError(t, err)
	outcome, err = Resolve(ctx, DB(f.db), f.t, g0, first)
	require.NoError(t, err)
	require.Equal(t, Unknown, outcome, "a later cdc-init replaced the lineage")

	dropped := f.reset(t, g4)
	outcome, err = Resolve(ctx, DB(f.db), f.t, g3, pending)
	require.NoError(t, err)
	require.Equal(t, Lost, outcome, "the cdc-init took generation 4")
	outcome, err = Resolve(ctx, DB(f.db), f.t, g4, f.next(t, g4, KindFlush))
	require.NoError(t, err)
	require.Equal(t, Unknown, outcome, "after a drop no attempt of the old epoch can commit")
	require.Equal(t, Dropped, dropped.State)
}
