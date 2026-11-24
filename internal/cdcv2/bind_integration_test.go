package cdcv2

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/publication"
)

// TestAFailedFirstPublicationBindsNothing pins R2-F1 of #911: a writer binds
// an unbound deployment inside its publication transaction, so a publication
// that never commits (a fault after the bind, a COMMIT that is refused, an
// expired staging, a compare-and-set lost to a rival with no rebase allowed,
// a COMMIT without an answer that did not land) leaves the deployment
// unbound. A misconfigured writer whose publication fails therefore cannot
// claim the deployment: the next writer that publishes binds its own target.
func TestAFailedFirstPublicationBindsNothing(t *testing.T) {
	slowBegin := func(_ context.Context, p Point) error {
		if p == PointBegin {
			time.Sleep(20 * time.Millisecond)
		}
		return nil
	}
	answerLost := func(_ context.Context, p Point) error {
		if p == PointCommit {
			return ErrCommitAnswerLost
		}
		return nil
	}
	for name, tc := range map[string]struct {
		mod  func(*Config)
		want error
	}{
		"a fault after the bind": {func(c *Config) { c.Failpoint = once(PointMarks, failWith(errInjected)) }, errInjected},
		"a refused COMMIT":       {func(c *Config) { c.Failpoint = once(PointCommit, failWith(errInjected)) }, errInjected},
		"an expired staging": {func(c *Config) {
			c.StagingBudget, c.Failpoint = time.Millisecond, slowBegin
		}, publication.ErrStagingExpired},
		"a COMMIT without an answer that did not land": {func(c *Config) {
			c.Attempts, c.Failpoint = 1, answerLost
		}, ErrCommitAnswerLost},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			published(t, f)
			wrong, right := newS3Origin(t, f.store), newS3Origin(t, f.store)
			before := f.record(t)
			_, err := f.writer(t, overS3(wrong.client(t)), tc.mod).Flush(context.Background(), schemaID, 1, &stager{f: f})
			require.ErrorIs(t, err, tc.want)
			require.NotZero(t, wrong.hits.Load(), "the writer staged and reached its publication")
			require.Equal(t, before, f.record(t), "nothing was published")
			_, ok := f.binding(t)
			require.False(t, ok, "a publication that never committed binds nothing")
			f.rightWriterBinds(t, right)
		})
	}

	t.Run("a compare-and-set lost to a rival", func(t *testing.T) {
		f := newFixture(t)
		ctx := context.Background()
		published(t, f)
		wrong, right := newS3Origin(t, f.store), newS3Origin(t, f.store)
		g := newGate(PointBegin)
		w := f.writer(t, overS3(wrong.client(t)), func(c *Config) { c.Failpoint = g.failpoint })
		out := async(func() (InitReport, error) { return w.Init(ctx, schemaID, 1, &stager{f: f}) })
		g.wait(t)
		_, err := f.writer(t).Flush(ctx, schemaID, 1, &stager{f: f})
		require.NoError(t, err, "a writer over a file store publishes and binds nothing")
		head := f.record(t)
		close(g.released)
		res := <-out
		require.ErrorIs(t, res.err, ErrAbandoned)
		require.ErrorIs(t, res.err, publication.ErrRebaseRefused)
		require.Equal(t, head, f.record(t))
		_, ok := f.binding(t)
		require.False(t, ok, "an init whose compare-and-set lost binds nothing")
		f.insert(t, 1)
		f.rightWriterBinds(t, right)
	})

	t.Run("a COMMIT without an answer that landed binds", func(t *testing.T) {
		f := newFixture(t)
		published(t, f)
		origin := newS3Origin(t, f.store)
		w := f.writer(t, overS3(origin.client(t)), func(c *Config) { c.Failpoint = once(PointCommitAnswer, failWith(errInjected)) })
		rep, err := w.Flush(context.Background(), schemaID, 1, &stager{f: f})
		require.NoError(t, err, "Resolve found the publication committed")
		require.Len(t, rep.Generations, 1)
		b, ok := f.binding(t)
		require.True(t, ok, "the binding committed with the generation")
		require.True(t, b.Target.Same(origin.target(f)), "bound to %s", b.Target)
		f.checkInvariants(t)
	})
}

// rightWriterBinds flushes the fixture's dirty entries through a writer over
// origin and checks that it published and bound the deployment to origin.
func (f *fixture) rightWriterBinds(t *testing.T, origin *s3Origin) {
	t.Helper()
	rep, err := f.writer(t, overS3(origin.client(t))).Flush(context.Background(), schemaID, 1, &stager{f: f})
	require.NoError(t, err, "a later writer is not blocked by a binding nobody committed")
	require.NotEmpty(t, rep.Generations)
	b, ok := f.binding(t)
	require.True(t, ok)
	require.True(t, b.Target.Same(origin.target(f)), "bound to %s, want %s", b.Target, origin.target(f))
	f.checkInvariants(t)
}

// TestConcurrentFirstPublishersBindOneTarget pins R2-F1 of #911 for two
// first publishers of an unbound deployment at different targets, both of
// which pass the check before staging and stage: the second's bind waits for
// the first's publication transaction. If that commits, the second fails
// closed with ErrStorageBoundElsewhere and publishes nothing; if it rolls
// back, the second binds its own target and publishes.
func TestConcurrentFirstPublishersBindOneTarget(t *testing.T) {
	for name, firstCommits := range map[string]bool{"the first commits": true, "the first rolls back": false} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			if f.cluster {
				t.Skip("nothing waits on a row lock on an Aurora DSQL cluster")
			}
			ctx := context.Background()
			published(t, f)
			one, two := newS3Origin(t, f.store), newS3Origin(t, f.store)
			reached, released := make(chan struct{}), make(chan struct{})
			hold := once(PointCommit, func(ctx context.Context) error {
				close(reached)
				select {
				case <-released:
				case <-time.After(time.Minute):
					return errors.New("the test never released the first publisher")
				}
				if firstCommits {
					return nil
				}
				return errInjected
			})
			first := f.writer(t, overS3(one.client(t)), func(c *Config) { c.Failpoint = hold })
			out1 := async(func() (FlushReport, error) { return first.Flush(ctx, schemaID, 1, &stager{f: f}) })
			<-reached
			_, ok := f.binding(t)
			require.False(t, ok, "the first's bind is not visible before its COMMIT")

			second := f.writer(t, overS3(two.client(t)))
			out2 := async(func() (FlushReport, error) { return second.Flush(ctx, schemaID, 1, &stager{f: f}) })
			select {
			case res := <-out2:
				t.Fatalf("the second publisher finished while the first's transaction was open: %v", res.err)
			case <-time.After(300 * time.Millisecond):
			}
			require.NotZero(t, two.hits.Load(), "the second passed the check and staged")
			close(released)

			res1, res2 := <-out1, <-out2
			winner := one
			if firstCommits {
				require.NoError(t, res1.err)
				require.ErrorIs(t, res2.err, publication.ErrStorageBoundElsewhere)
			} else {
				winner = two
				require.ErrorIs(t, res1.err, errInjected)
				require.NoError(t, res2.err)
				require.Len(t, res2.v.Generations, 1)
			}
			b, ok := f.binding(t)
			require.True(t, ok)
			require.True(t, b.Target.Same(winner.target(f)), "bound to %s, want %s", b.Target, winner.target(f))
			require.Len(t, f.history(t), 2, "one flush before, and the one that bound")
			require.Empty(t, f.dirty(t))
			f.checkInvariants(t)
		})
	}
}
