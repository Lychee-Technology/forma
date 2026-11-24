package cdcv2

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// The fixture's store is a local directory, which names no storage target, so
// these tests give each writer the target New derives for an S3 store
// (storage_test.go covers that derivation, and a writer over an S3 store).
var (
	writersTarget = publication.StorageTarget{Endpoint: "https://lake.s3.us-east-1.amazonaws.com", Bucket: "lake", Prefix: "data"}
	otherTarget   = publication.StorageTarget{Endpoint: "https://copy.s3.us-east-1.amazonaws.com", Bucket: "copy", Prefix: "data"}
)

// at answers w bound and checked against target.
func at(t *testing.T, w *Writer, target publication.StorageTarget) *Writer {
	t.Helper()
	n, err := target.Normalize()
	require.NoError(t, err)
	w.storage = &n
	return w
}

func (f *fixture) binding(t *testing.T) (publication.StorageTarget, bool) {
	t.Helper()
	b, ok, err := publication.ReadStorage(context.Background(), publication.DB(f.db), f.tables)
	require.NoError(t, err)
	return b.Target, ok
}

func (f *fixture) requireUnbound(t *testing.T, why string) {
	t.Helper()
	_, ok := f.binding(t)
	require.False(t, ok, why)
}

func (f *fixture) requireBound(t *testing.T, want publication.StorageTarget) {
	t.Helper()
	got, ok := f.binding(t)
	require.True(t, ok, "the deployment is bound")
	require.Equal(t, want, got)
}

// TestARunThatPublishesNothingBindsNothing: only a committed generation binds.
// An empty flush, a compaction of an unpublished schema, one under another
// prefix than the published one, and one that merges nothing publish nothing,
// and leave the deployment unbound for the writers that do publish.
func TestARunThatPublishesNothingBindsNothing(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	f.register(t)
	rep, err := at(t, f.writer(t), otherTarget).Flush(ctx, schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	require.Empty(t, rep.Generations)
	f.requireUnbound(t, "an empty flush")
	_, err = at(t, f.writer(t), otherTarget).Compact(ctx, schemaID, 1, &compactStager{f: f})
	require.NoError(t, err)
	f.requireUnbound(t, "a compaction of an unpublished schema")

	f.insert(t, 2)
	_, err = f.writer(t).Flush(ctx, schemaID, 1, &stager{f: f}) // a writer naming no target
	require.NoError(t, err)
	f.requireUnbound(t, "a writer that names no target")

	elsewhere := at(t, f.writer(t, func(c *Config) { c.Layout = manifestv2.Layout{Prefix: "other"} }), otherTarget)
	_, err = elsewhere.Compact(ctx, schemaID, 1, &compactStager{f: f})
	require.Error(t, err, "the published generation is not under the compactor's prefix")
	f.requireUnbound(t, "a refused compaction")
	none := &compactStager{f: f, pick: func(*manifestv2.Manifest) []manifestv2.File { return nil }}
	crep, err := at(t, f.writer(t), otherTarget).Compact(ctx, schemaID, 1, none)
	require.NoError(t, err)
	require.False(t, crep.Committed)
	f.requireUnbound(t, "a compaction that merges nothing")

	f.insert(t, 1)
	_, err = at(t, f.writer(t), writersTarget).Flush(ctx, schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	f.requireBound(t, writersTarget)
}

// TestABoundDeploymentRefusesAnotherTargetBeforeStaging: after the first
// publication bound the writers' target, a flush, an init and a compaction at
// another target fail before they stage anything, and nothing moves.
func TestABoundDeploymentRefusesAnotherTargetBeforeStaging(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	f.register(t)
	f.insert(t, 2)
	_, err := at(t, f.writer(t), writersTarget).Flush(ctx, schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	f.requireBound(t, writersTarget)
	head := f.record(t)
	f.insert(t, 1)
	dirty := f.dirty(t)

	st, cst := &stager{f: f}, &compactStager{f: f}
	other := at(t, f.writer(t), otherTarget)
	_, err = other.Flush(ctx, schemaID, 1, st)
	require.ErrorIs(t, err, publication.ErrStorageBoundElsewhere)
	_, err = other.Init(ctx, schemaID, 1, st)
	require.ErrorIs(t, err, publication.ErrStorageBoundElsewhere)
	_, err = other.Compact(ctx, schemaID, 1, cst)
	require.ErrorIs(t, err, publication.ErrStorageBoundElsewhere)
	require.Zero(t, st.calls, "nothing was staged")
	require.Zero(t, cst.calls, "nothing was staged")
	require.Equal(t, head, f.record(t))
	require.Equal(t, dirty, f.dirty(t))
	f.requireBound(t, writersTarget)

	spelled := writersTarget
	spelled.Endpoint, spelled.Prefix = "HTTPS://LAKE.s3.us-east-1.amazonaws.com/", "data/"
	_, err = at(t, f.writer(t), spelled).Flush(ctx, schemaID, 1, st)
	require.NoError(t, err, "the bound target, spelled another way")
	require.Empty(t, f.dirty(t))
}

// TestAFailedFirstPublicationBindsNothing: a first publication that fails
// after its bind, before or at COMMIT, or that is abandoned, leaves the
// deployment unbound, so a correctly configured writer binds its own target
// next. One whose COMMIT landed without an answer bound with its generation.
func TestAFailedFirstPublicationBindsNothing(t *testing.T) {
	faults := map[string]func(c *Config){
		"fails before its marks": func(c *Config) { c.Failpoint = once(PointMarks, failWith(errors.New("injected"))) },
		"fails before COMMIT":    func(c *Config) { c.Failpoint = once(PointCommit, failWith(errors.New("injected"))) },
		"lost COMMIT that did not land": func(c *Config) {
			c.Attempts = 1
			c.Failpoint = once(PointCommit, failWith(fmt.Errorf("reset: %w", ErrCommitAnswerLost)))
		},
		"staging expired": func(c *Config) { c.StagingBudget = 1; c.Restarts = 1 },
	}
	for name, fault := range faults {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			f := newFixture(t)
			before := f.register(t)
			f.insert(t, 2)
			_, err := at(t, f.writer(t, fault), otherTarget).Flush(ctx, schemaID, 1, &stager{f: f})
			require.Error(t, err)
			require.Equal(t, before, f.record(t), "nothing was published")
			f.requireUnbound(t, "a publication that never committed binds nothing")

			_, err = at(t, f.writer(t), writersTarget).Flush(ctx, schemaID, 1, &stager{f: f})
			require.NoError(t, err)
			f.requireBound(t, writersTarget)
		})
	}

	t.Run("lost COMMIT that landed", func(t *testing.T) {
		ctx := context.Background()
		f := newFixture(t)
		f.register(t)
		f.insert(t, 2)
		w := at(t, f.writer(t, func(c *Config) { c.Failpoint = once(PointCommitAnswer, failWith(errors.New("reset"))) }), writersTarget)
		rep, err := w.Flush(ctx, schemaID, 1, &stager{f: f})
		require.NoError(t, err)
		require.Len(t, rep.Generations, 1)
		f.requireBound(t, writersTarget)
	})
}

// TestAFirstPublicationLosesToABindingCommittedWhileItStaged: two first
// writers at different targets both pass the check before staging. The one
// whose publication commits second finds the other's binding in its
// transaction and fails closed before its compare-and-set, keeping the
// binding, and a running writer's next publication after a rebind is checked
// against the new binding.
func TestAFirstPublicationLosesToABindingCommittedWhileItStaged(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	before := f.register(t)
	f.insert(t, 2)
	rival := func(ctx context.Context) error {
		_, err := publication.BindStorage(ctx, publication.DB(f.db), f.tables, writersTarget)
		return err
	}
	st := &stager{f: f}
	w := at(t, f.writer(t, func(c *Config) { c.Failpoint = once(PointPublish, rival) }), otherTarget)
	_, err := w.Flush(ctx, schemaID, 1, st)
	require.ErrorIs(t, err, publication.ErrStorageBoundElsewhere)
	require.Equal(t, 1, st.calls, "it staged before the rival bound")
	require.Equal(t, before, f.record(t), "the record did not move")
	f.requireBound(t, writersTarget)

	running := at(t, f.writer(t), writersTarget)
	_, err = running.Flush(ctx, schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	_, err = f.db.Exec("DELETE FROM " + f.tables.Storage)
	require.NoError(t, err)
	_, err = publication.BindStorage(ctx, publication.DB(f.db), f.tables, otherTarget)
	require.NoError(t, err)
	f.insert(t, 1)
	_, err = running.Flush(ctx, schemaID, 1, &stager{f: f})
	require.ErrorIs(t, err, publication.ErrStorageBoundElsewhere, "the writer checks again after a rebind")
}

// TestTwoFirstPublishersAtDifferentTargetsBindOne races two first flushes of
// an unbound deployment at different targets, both staged and inside their
// publication transactions. Exactly one commits and binds its target; the
// other fails closed with ErrStorageBoundElsewhere before its compare-and-set,
// however its target's engine serializes them: on PostgreSQL its insert waits
// and its read sees the binding, at REPEATABLE READ or on DSQL it gets 40001
// and its resend reads the binding.
func TestTwoFirstPublishersAtDifferentTargetsBindOne(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	f.register(t)
	f.insert(t, 3)
	targets := []publication.StorageTarget{writersTarget, otherTarget}
	gates := make([]*gate, len(targets))
	stagers := make([]*stager, len(targets))
	outs := make([]<-chan struct {
		v   FlushReport
		err error
	}, len(targets))
	for i, target := range targets {
		gates[i], stagers[i] = newGate(PointPublish), &stager{f: f}
		w := at(t, f.writer(t, func(c *Config) { c.Failpoint = gates[i].failpoint; c.Attempts = 10 }), target)
		outs[i] = async(func() (FlushReport, error) { return w.Flush(ctx, schemaID, 1, stagers[i]) })
	}
	for _, g := range gates {
		g.wait(t)
	}
	for _, g := range gates {
		close(g.released)
	}
	winner := -1
	for i, out := range outs {
		res := <-out
		require.Equal(t, 1, stagers[i].calls, "publisher %d staged before either bound", i)
		if res.err == nil {
			require.Equal(t, -1, winner, "only one publisher commits")
			winner = i
			continue
		}
		require.ErrorIs(t, res.err, publication.ErrStorageBoundElsewhere, "publisher %d", i)
	}
	require.NotEqual(t, -1, winner, "one publisher commits")
	f.requireBound(t, targets[winner])
	require.Equal(t, int64(1), f.record(t).Generation, "the loser moved nothing")
	require.Empty(t, f.dirty(t))
}

// TestAWriterWithoutTheBindingTableFailsBeforeStaging: a deployment whose
// binding table was never created fails its writers before they stage.
func TestAWriterWithoutTheBindingTableFailsBeforeStaging(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	before := f.register(t)
	f.insert(t, 1)
	_, err := f.db.Exec("DROP TABLE " + f.tables.Storage)
	require.NoError(t, err)
	st := &stager{f: f}
	_, err = at(t, f.writer(t), writersTarget).Flush(ctx, schemaID, 1, st)
	require.ErrorIs(t, err, publication.ErrStorageTableMissing)
	require.Zero(t, st.calls)
	require.Equal(t, before, f.record(t))
}
