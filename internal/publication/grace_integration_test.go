package publication

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/sqlgen"
)

// TestGraceMillisRoundsUpAndCoversTheStamp pins the conversion every
// retention statement compares with (R7-F3): a grace is never truncated to
// the millisecond, and one millisecond more covers the truncated stamp.
func TestGraceMillisRoundsUpAndCoversTheStamp(t *testing.T) {
	for grace, want := range map[time.Duration]int64{
		0:                                  1,
		time.Nanosecond:                    2,
		time.Millisecond:                   2,
		time.Millisecond + time.Nanosecond: 3,
		60 * time.Second:                   60001,
		60*time.Second + time.Nanosecond:   60002,
		time.Duration(1<<63 - 1):           int64((1<<63-1)/int64(time.Millisecond)) + 2,
	} {
		require.Equal(t, want, GraceMillis(grace), "grace %s", grace)
	}
}

// TestTheMinimumGraceIsComparedInFull is the boundary of the smallest grace
// manifest-reconcile accepts, the reader's 60 s pass deadline plus a 1 ns
// clock margin (R7-F3). Inside one transaction the database time is fixed,
// so each case stamps generation 1's retirement and the lifecycle epoch row
// an exact number of milliseconds before it. A stamp 60000 ms back records
// an event up to a millisecond later than itself, measured against a
// database time up to a millisecond earlier than the real one: as little as
// 59.999 s may have passed, so the generation is still retained, the fence
// still inside, and nothing is forgotten. Truncated to 60000 ms, as the
// grace was, every statement took that stamp for expired. Only a stamp
// ceil(grace) + 1 ms back, 60002 ms, is past the grace.
func TestTheMinimumGraceIsComparedInFull(t *testing.T) {
	const grace = 60*time.Second + time.Nanosecond
	f := newFixture(t)
	ctx := context.Background()
	cur := f.initialized(t, 100)
	for range 2 { // generation 1, then generation 2 supersedes it
		next, err := f.publish(ctx, cur, f.next(t, cur, KindFlush), nil)
		require.NoError(t, err)
		cur = next
	}
	stamp := func(tx Tx, table, column, where string, ago int64) {
		t.Helper()
		_, err := tx.runner().exec(ctx, fmt.Sprintf("UPDATE %s SET %s = %s - $1 WHERE schema_id = 100%s",
			table, column, sqlgen.ServerTimeMillisSQL, where), ago)
		require.NoError(t, err)
	}
	for ago, expired := range map[int64]bool{60000: false, 60001: false, 60002: true} {
		tx, err := f.db.BeginTx(ctx, nil)
		require.NoError(t, err)
		// A failed case must not leave the transaction open: the fixture's
		// DROP TABLE would wait on it.
		t.Cleanup(func() { _ = tx.Rollback() })
		q := SQLTx(tx)
		stamp(q, f.t.Generation, "retired_at", " AND generation = 1", ago)
		stamp(q, f.t.Epoch, "updated_at", "", ago)

		gens, err := Expired(ctx, q, f.t, 100, grace)
		require.NoError(t, err)
		require.Equal(t, expired, slices.ContainsFunc(gens, func(g Generation) bool { return g.Generation == 1 }), "expired, %d ms back", ago)
		gens, err = Retained(ctx, q, f.t, 100, grace)
		require.NoError(t, err)
		require.Equal(t, !expired, slices.ContainsFunc(gens, func(g Generation) bool { return g.Generation == 1 }), "retained, %d ms back", ago)
		fence, err := ReadLifecycleFence(ctx, q, f.t, 100, grace)
		require.NoError(t, err)
		require.True(t, fence.Present)
		require.Equal(t, !expired, fence.Inside, "the lifecycle fence, %d ms back", ago)
		forgot, err := Forget(ctx, q, f.t, 100, 1, grace)
		require.NoError(t, err)
		require.Equal(t, expired, forgot, "forgotten, %d ms back", ago)
		require.NoError(t, tx.Rollback())
	}
}
