package cdcv2

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
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
	require.Nil(t, unresolved.Handoff, "a flush's marks commit with its publication")
	require.Equal(t, len(dirty), unresolved.Marked, "the unanswered COMMIT's marks counted every selected entry")
	require.Zero(t, unresolved.Unconfirmed)
	outcome, err := publication.Resolve(context.Background(), publication.DB(f.db), f.tables, unresolved.Cur, unresolved.Next)
	require.NoError(t, err)
	require.Equal(t, publication.NotCommitted, outcome)
	require.Equal(t, before, f.record(t))
	require.Equal(t, dirty, f.dirty(t))

	// Settled later, the fence holds the record: the lost COMMIT can never
	// land, so the flush is abandoned and the next one publishes its entries.
	_, err = f.writer(t).Settle(context.Background(), unresolved)
	require.ErrorIs(t, err, ErrAbandoned)
	require.Equal(t, before, f.record(t))
	rep, err = f.writer(t).Flush(context.Background(), schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	require.Equal(t, 2, rep.Marked)
	require.Empty(t, f.dirty(t))
	f.checkInvariants(t)
}

// TestResumeInitHandsOffAnInitWhoseFenceFailed: the init's COMMIT goes
// without an answer, the attempts run out, and the fence fails too, on a
// database outage or because the caller's context ended. Init returns
// ErrUnresolved with its exported entries and no handoff. When the COMMIT
// lands afterwards, ResumeInit finds the init committed and hands its entries
// off at their export versions: a row written again since stays dirty at its
// newer version. When nothing lands, ResumeInit's fence holds the record, the
// init is abandoned, and running Init again publishes and hands off.
func TestResumeInitHandsOffAnInitWhoseFenceFailed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		atFence func(cancel context.CancelFunc) error
		lands   bool
	}{
		{"the database is out at the fence", func(context.CancelFunc) error { return errInjected }, true},
		{"the caller's context ends at the fence", func(cancel context.CancelFunc) error { cancel(); return nil }, true},
		{"nothing lands", func(context.CancelFunc) error { return errInjected }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			published(t, f)
			before, dirty := f.record(t), f.dirty(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var commits atomic.Int32
			fp := func(_ context.Context, p Point) error {
				switch {
				case p == PointCommit && commits.Add(1) == 1:
					return ErrCommitAnswerLost
				case p == PointFence:
					return tc.atFence(cancel)
				}
				return nil
			}
			_, err := f.writer(t, func(c *Config) { c.Attempts = 1; c.Failpoint = fp }).Init(ctx, schemaID, 1, &stager{f: f})
			require.ErrorIs(t, err, ErrUnresolved)
			require.NotErrorIs(t, err, ErrAbandoned)
			var u *UnresolvedError
			require.True(t, errors.As(err, &u))
			require.Equal(t, publication.KindInit, u.Kind)
			require.Equal(t, dirty, entryMap(u.Handoff), "the error keeps the exported entries at their listed versions")
			require.Equal(t, before, f.record(t))
			require.Equal(t, dirty, f.dirty(t), "no handoff ran")

			if !tc.lands {
				_, err := f.writer(t).ResumeInit(context.Background(), u)
				require.ErrorIs(t, err, ErrAbandoned, "the fence holds the record: the init can never commit")
				require.Equal(t, before, f.record(t))
				require.Equal(t, dirty, f.dirty(t))
				rep, err := f.writer(t).Init(context.Background(), schemaID, 1, &stager{f: f})
				require.NoError(t, err)
				require.Equal(t, 2, rep.Marked)
				require.Empty(t, f.dirty(t))
				f.checkInvariants(t)
				return
			}
			f.landStaged(t) // the lost COMMIT lands late
			var moved uuid.UUID
			for row := range dirty {
				moved = row
				break
			}
			f.put(t, moved, 2000, false) // a write after the export

			// Still unable to fence, a resume stays unresolved and keeps the
			// entries for the next one.
			outage := func(_ context.Context, p Point) error {
				if p == PointFence {
					return errInjected
				}
				return nil
			}
			_, err = f.writer(t, func(c *Config) { c.Failpoint = outage }).ResumeInit(context.Background(), u)
			var again *UnresolvedError
			require.True(t, errors.As(err, &again), "%v", err)
			require.Equal(t, u.Handoff, again.Handoff)
			require.Len(t, f.dirty(t), 2)

			rep, err := f.writer(t).ResumeInit(context.Background(), again)
			require.NoError(t, err, "the late COMMIT landed: the init committed")
			require.Equal(t, f.record(t).Token, rep.Generation.Token)
			require.Equal(t, u.Next.Token, rep.Generation.Token)
			require.Equal(t, 2, rep.Entries)
			require.Equal(t, 1, rep.Marked, "the moved row's newer version is not marked")
			require.Zero(t, rep.Unconfirmed)
			require.False(t, rep.HandoffStopped)
			require.Equal(t, map[uuid.UUID]int64{moved: 2000}, f.dirty(t))
			f.checkInvariants(t)
		})
	}
}

// entryMap answers entries as change_log's dirty set reads.
func entryMap(entries []Entry) map[uuid.UUID]int64 {
	out := map[uuid.UUID]int64{}
	for _, e := range entries {
		out[e.RowID] = e.Version
	}
	return out
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
