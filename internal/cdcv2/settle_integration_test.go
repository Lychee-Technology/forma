package cdcv2

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// lostThenAtFence answers a failpoint that loses the first COMMIT's answer,
// the COMMIT not landing, and runs atFence at each fence, counting them.
func lostThenAtFence(fences *atomic.Int32, atFence func()) func(context.Context, Point) error {
	var commits atomic.Int32
	return func(_ context.Context, p Point) error {
		switch {
		case p == PointCommit && commits.Add(1) == 1:
			return ErrCommitAnswerLost
		case p == PointFence:
			if fences.Add(1) == 1 && atFence != nil {
				atFence()
			}
		}
		return nil
	}
}

// TestInitSettlesALostCommitAfterItsLastResolve: with one attempt, the
// init's COMMIT goes without an answer and Resolve finds the record where the
// init found it, so the attempts run out. The COMMIT lands only then, before
// the fence. The init takes it as committed and hands its entries off.
func TestInitSettlesALostCommitAfterItsLastResolve(t *testing.T) {
	f := newFixture(t)
	published(t, f)
	var fences atomic.Int32
	w := f.writer(t, func(c *Config) {
		c.Attempts = 1
		c.Failpoint = lostThenAtFence(&fences, func() { f.landStaged(t) })
	})
	rep, err := w.Init(context.Background(), schemaID, 1, &stager{f: f})
	require.NoError(t, err, "the lost COMMIT landed: the init committed")
	require.Equal(t, int32(1), fences.Load())
	require.Equal(t, rep.Generation.Token, f.record(t).Token)
	require.Equal(t, 2, rep.Marked, "the handoff runs")
	require.Len(t, f.history(t), 2)
	require.Empty(t, f.dirty(t))
	f.checkInvariants(t)
}

// TestInitFenceWaitsForACommitStillInFlight holds the init's lost COMMIT
// open in a transaction of its own that already moved the record, as a
// COMMIT still on its way leaves it, when the attempts run out. The fence
// waits on the record row until that transaction commits, then finds the
// record moved (READ COMMITTED), or fails with 40001 and the next fence finds
// it moved (MiniStack's REPEATABLE READ), so the init is committed and handed
// off. Only PostgreSQL waits on the row: on an Aurora DSQL cluster the fence
// commits first and the late COMMIT fails.
func TestInitFenceWaitsForACommitStillInFlight(t *testing.T) {
	f := newFixture(t)
	if f.cluster {
		t.Skip("nothing waits on a row lock on an Aurora DSQL cluster")
	}
	published(t, f)
	var fences atomic.Int32
	landed := make(chan error, 1)
	inFlight := func() {
		tx := f.publishStaged(t)
		go func() {
			time.Sleep(200 * time.Millisecond)
			landed <- tx.Commit()
		}()
	}
	w := f.writer(t, func(c *Config) { c.Attempts = 1; c.Failpoint = lostThenAtFence(&fences, inFlight) })
	rep, err := w.Init(context.Background(), schemaID, 1, &stager{f: f})
	require.NotZero(t, fences.Load(), "the init fenced its lost COMMIT before it gave up")
	select {
	case lerr := <-landed:
		require.NoError(t, lerr)
	case <-time.After(time.Minute):
		t.Fatal("the COMMIT in flight never landed")
	}
	require.NoError(t, err, "the fence waited for the COMMIT, which landed")
	require.Equal(t, rep.Generation.Token, f.record(t).Token)
	require.Equal(t, 2, rep.Marked, "the handoff runs")
	require.Empty(t, f.dirty(t))
	f.checkInvariants(t)
}

// TestFlushSettlesAStagingThatExpiresDuringAnUncertainResend: the flush's
// COMMIT goes without an answer, Resolve finds NotCommitted, and the resend
// finds the staging expired. When the lost COMMIT lands before the fence,
// the flush is committed, not abandoned. When nothing lands, the fence holds
// the record, so the flush abandons the staging for good and starts over.
func TestFlushSettlesAStagingThatExpiresDuringAnUncertainResend(t *testing.T) {
	for _, tc := range []struct {
		name     string
		lands    bool
		restarts int
	}{{"the lost commit lands", true, 0}, {"nothing lands", false, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			published(t, f)
			before := f.record(t)
			var fences, begins atomic.Int32
			var atFence func()
			if tc.lands {
				atFence = func() { f.landStaged(t) }
			}
			lost := lostThenAtFence(&fences, atFence)
			fp := func(ctx context.Context, p Point) error {
				if p == PointBegin && begins.Add(1) == 2 { // the resend
					time.Sleep(time.Second)
				}
				return lost(ctx, p)
			}
			w := f.writer(t, func(c *Config) { c.StagingBudget = 500 * time.Millisecond; c.Failpoint = fp })
			rep, err := w.Flush(context.Background(), schemaID, 1, &stager{f: f})
			require.NoError(t, err)
			require.Equal(t, int32(1), fences.Load(), "the expired resend was settled before it was given up")
			require.Equal(t, tc.restarts, rep.Restarts)
			require.Len(t, rep.Generations, 1)
			require.Equal(t, before.Generation+1, f.record(t).Generation, "one generation committed")
			require.Equal(t, rep.Generations[0].Token, f.record(t).Token)
			require.Empty(t, f.dirty(t))
			f.checkInvariants(t)
		})
	}
}

// TestFlushReportsAnOutcomeItCannotSettle: when the fence itself fails, the
// flush reports the publication unresolved, not abandoned, and does not
// start over; its expectation and token still resolve it afterwards.
func TestFlushReportsAnOutcomeItCannotSettle(t *testing.T) {
	f := newFixture(t)
	published(t, f)
	before, dirty := f.record(t), f.dirty(t)
	var commits atomic.Int32
	fp := func(_ context.Context, p Point) error {
		switch {
		case p == PointCommit && commits.Add(1) == 1:
			return ErrCommitAnswerLost
		case p == PointFence:
			return errInjected
		}
		return nil
	}
	rep, err := f.writer(t, func(c *Config) { c.Attempts = 1; c.Failpoint = fp }).Flush(context.Background(), schemaID, 1, &stager{f: f})
	require.ErrorIs(t, err, ErrUnresolved)
	require.ErrorIs(t, err, errInjected)
	require.NotErrorIs(t, err, ErrAbandoned)
	require.NotErrorIs(t, err, ErrAttemptsExhausted)
	require.Zero(t, rep.Restarts)
	var unresolved *UnresolvedError
	require.True(t, errors.As(err, &unresolved))
	require.Equal(t, before, unresolved.Cur)
	require.ErrorIs(t, unresolved.Cause, ErrAttemptsExhausted)
	outcome, err := publication.Resolve(context.Background(), publication.DB(f.db), f.tables, unresolved.Cur, unresolved.Next)
	require.NoError(t, err)
	require.Equal(t, publication.NotCommitted, outcome)
	require.Equal(t, before, f.record(t))
	require.Equal(t, dirty, f.dirty(t))
}

// publishStaged runs, in a transaction it leaves open, the compare-and-set
// of the one staged init manifest that no history row names: an init's
// COMMIT still on its way. The caller commits or rolls it back.
func (f *fixture) publishStaged(t *testing.T) *sql.Tx {
	t.Helper()
	ctx := context.Background()
	cur := f.record(t)
	m, raw := f.stagedNext(t, cur)
	require.Equal(t, publication.KindInit, m.Kind)
	tx, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	var now int64
	require.NoError(t, tx.QueryRow("SELECT floor(extract(epoch from now()) * 1000)::bigint").Scan(&now))
	_, err = publication.Publish(ctx, publication.SQLTx(tx), f.tables, cur, publication.Next{
		Kind: m.Kind, Token: m.Token, ManifestKey: f.layout.ManifestKey(m.SchemaID, m.Epoch, m.Generation, m.Token),
		ManifestSHA256: manifestv2.Digest(raw), Lineage: m.Lineage, StagedAt: now, StagingBudget: time.Minute,
	})
	require.NoError(t, err)
	return tx
}
