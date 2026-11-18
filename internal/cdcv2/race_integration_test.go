package cdcv2

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// gate holds a writer at the first hit of a point, after everything it
// staged, until the test releases it.
type gate struct {
	at       Point
	once     sync.Once
	reached  chan struct{}
	released chan struct{}
}

func newGate(at Point) *gate {
	return &gate{at: at, reached: make(chan struct{}), released: make(chan struct{})}
}

func (g *gate) failpoint(ctx context.Context, p Point) error {
	if p != g.at {
		return nil
	}
	held := false
	g.once.Do(func() { held = true })
	if !held {
		return nil
	}
	close(g.reached)
	select {
	case <-g.released:
		return nil
	case <-time.After(time.Minute):
		return errors.New("the test never released the gate")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *gate) wait(t *testing.T) {
	t.Helper()
	select {
	case <-g.reached:
	case <-time.After(time.Minute):
		t.Fatal("the writer never reached the gate")
	}
}

// async runs fn in a goroutine and answers its outcome.
func async[T any](fn func() (T, error)) <-chan struct {
	v   T
	err error
} {
	out := make(chan struct {
		v   T
		err error
	}, 1)
	go func() {
		v, err := fn()
		out <- struct {
			v   T
			err error
		}{v, err}
	}()
	return out
}

// TestConcurrentFlushersCommitOnePerParent races three flushers that staged
// the same entries on the same record: exactly one compare-and-set wins per
// parent, every loser rebases over the flushes and commits after it, and no
// entry is marked twice.
func TestConcurrentFlushersCommitOnePerParent(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.register(t)
	f.insert(t, 5)
	const racers = 3
	gates := make([]*gate, racers)
	outs := make([]<-chan struct {
		v   FlushReport
		err error
	}, racers)
	for i := range racers {
		gates[i] = newGate(PointBegin)
		w := f.writer(t, func(c *Config) { c.Failpoint = gates[i].failpoint; c.Attempts = 10 })
		outs[i] = async(func() (FlushReport, error) { return w.Flush(ctx, schemaID, 1, &stager{f: f}) })
	}
	for _, g := range gates {
		g.wait(t)
	}
	for _, g := range gates {
		close(g.released)
	}
	marked, rebases := 0, 0
	for _, out := range outs {
		res := <-out
		require.NoError(t, res.err)
		require.Len(t, res.v.Generations, 1)
		marked += res.v.Marked
		rebases += res.v.Rebases
	}
	require.Equal(t, 5, marked, "every entry is marked once")
	require.GreaterOrEqual(t, rebases, racers-1, "every loser rebased onto a winner")
	require.Len(t, f.history(t), racers)
	require.Empty(t, f.dirty(t))
	f.checkInvariants(t)
}

// TestFlushStagedBeforeAnInitStartsOver: a flush never rebases over a
// cdc-init, so a flush that lost its compare-and-set to one abandons its
// staging and selects the unflushed entries again, which the handoff
// already marked.
func TestFlushStagedBeforeAnInitStartsOver(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	published(t, f)
	g := newGate(PointBegin)
	w := f.writer(t, func(c *Config) { c.Failpoint = g.failpoint })
	out := async(func() (FlushReport, error) { return w.Flush(ctx, schemaID, 1, &stager{f: f}) })
	g.wait(t)
	_, err := f.writer(t).Init(ctx, schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	close(g.released)

	res := <-out
	require.NoError(t, res.err)
	require.Equal(t, 1, res.v.Restarts)
	require.Empty(t, res.v.Generations)
	require.Equal(t, []publication.Kind{publication.KindFlush, publication.KindInit}, kinds(f.history(t)))
	f.checkInvariants(t)
}

// TestInitStagedBeforeAFlushIsAbandoned: an init never rebases over a flush,
// whose rows its export may predate, so it abandons and leaves the flush's
// generation and marks as they are.
func TestInitStagedBeforeAFlushIsAbandoned(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	published(t, f)
	g := newGate(PointBegin)
	w := f.writer(t, func(c *Config) { c.Failpoint = g.failpoint })
	out := async(func() (InitReport, error) { return w.Init(ctx, schemaID, 1, &stager{f: f}) })
	g.wait(t)
	_, err := f.writer(t).Flush(ctx, schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	head := f.record(t)
	close(g.released)

	res := <-out
	require.ErrorIs(t, res.err, ErrAbandoned)
	require.ErrorIs(t, res.err, publication.ErrRebaseRefused)
	require.Equal(t, head, f.record(t))
	require.Equal(t, []publication.Kind{publication.KindFlush, publication.KindFlush}, kinds(f.history(t)))
	f.checkInvariants(t)
}

// TestWritersRebaseOverACompaction: a flush and an init staged before a
// compaction committed restage onto it under a fresh token; the flush keeps
// the compaction's files and adds its delta, the init replaces them.
func TestWritersRebaseOverACompaction(t *testing.T) {
	for _, kind := range []publication.Kind{publication.KindFlush, publication.KindInit} {
		t.Run(string(kind), func(t *testing.T) {
			f := newFixture(t)
			ctx := context.Background()
			published(t, f)
			g := newGate(PointBegin)
			w := f.writer(t, func(c *Config) { c.Failpoint = g.failpoint })
			out := async(func() (int, error) {
				if kind == publication.KindFlush {
					rep, err := w.Flush(ctx, schemaID, 1, &stager{f: f})
					return rep.Rebases, err
				}
				rep, err := w.Init(ctx, schemaID, 1, &stager{f: f})
				return rep.Rebases, err
			})
			g.wait(t)
			f.compact(t)
			close(g.released)

			res := <-out
			require.NoError(t, res.err)
			require.Equal(t, 1, res.v)
			require.Equal(t, []publication.Kind{publication.KindFlush, publication.KindCompact, kind}, kinds(f.history(t)))
			require.Empty(t, f.dirty(t))
			f.checkInvariants(t)
		})
	}
}

// TestWritersStagedBeforeADropNeverCommit: a drop moves the epoch, so a
// flush and an init staged before it miss and refuse to rebase. The flush
// then reads a record of another epoch than it pinned and fails closed. A
// re-created schema takes an init of its new epoch only.
func TestWritersStagedBeforeADropNeverCommit(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	published(t, f)
	flushGate, initGate := newGate(PointBegin), newGate(PointBegin)
	flusher := f.writer(t, func(c *Config) { c.Failpoint = flushGate.failpoint })
	initer := f.writer(t, func(c *Config) { c.Failpoint = initGate.failpoint })
	flushed := async(func() (FlushReport, error) { return flusher.Flush(ctx, schemaID, 1, &stager{f: f}) })
	inited := async(func() (InitReport, error) { return initer.Init(ctx, schemaID, 0, &stager{f: f}) })
	flushGate.wait(t)
	initGate.wait(t)
	dropped, dirty, history := f.drop(t), f.dirty(t), f.history(t)
	close(flushGate.released)
	close(initGate.released)

	res := <-flushed
	require.ErrorIs(t, res.err, ErrEpochChanged)
	require.Equal(t, 1, res.v.Restarts)
	require.ErrorIs(t, (<-inited).err, ErrAbandoned)
	require.Equal(t, dropped, f.record(t))
	require.Equal(t, dirty, f.dirty(t))
	require.Equal(t, history, f.history(t))

	recreated := f.recreate(t)
	require.Equal(t, int64(3), recreated.Epoch)
	f.insert(t, 2)
	_, err := f.writer(t).Init(ctx, schemaID, 1, &stager{f: f})
	require.ErrorIs(t, err, ErrEpochChanged)
	rep, err := f.writer(t).Init(ctx, schemaID, recreated.Epoch, &stager{f: f})
	require.NoError(t, err)
	require.Equal(t, 2, rep.Marked)
	require.Equal(t, recreated.Epoch, rep.Generation.Epoch)
	require.Zero(t, rep.Generation.ParentGeneration, "the epoch's first generation")
	require.Greater(t, rep.Generation.Generation, history[len(history)-1].Generation, "generation numbers are never reused")
	f.checkInvariants(t)
}

// compact publishes a compaction of the head that keeps its files, as a
// compactor that merged nothing away would.
func (f *fixture) compact(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	head := f.record(t)
	ref, err := manifestv2.RecordRef(head)
	require.NoError(t, err)
	parent, err := manifestv2.Fetch(ctx, f.store, ref)
	require.NoError(t, err)
	h, err := manifestv2.Stage(head, publication.KindCompact, head.Lineage, uuid.New())
	require.NoError(t, err)
	m := manifestv2.Manifest{
		Format: publication.ManifestFormat, SchemaID: schemaID, Epoch: h.Epoch, Generation: h.Generation,
		ParentGeneration: h.ParentGeneration, Lineage: h.Lineage, Token: h.Token, Kind: publication.KindCompact,
		InputGeneration: head.Generation, Files: parent.Files,
	}
	key, digest, err := manifestv2.PutManifest(ctx, f.store, f.layout, m)
	require.NoError(t, err)
	var stagedAt int64
	require.NoError(t, f.db.QueryRow("SELECT floor(extract(epoch from now()) * 1000)::bigint").Scan(&stagedAt))
	f.inTx(t, func(tx *sql.Tx) error {
		_, err := publication.Publish(ctx, publication.SQLTx(tx), f.tables, head, publication.Next{
			Kind: publication.KindCompact, Token: h.Token, ManifestKey: key, ManifestSHA256: digest,
			Lineage: head.Lineage, StagedAt: stagedAt, StagingBudget: time.Minute,
		})
		return err
	})
}

func kinds(gens []publication.Generation) []publication.Kind {
	out := make([]publication.Kind, len(gens))
	for i, g := range gens {
		out[i] = g.Kind
	}
	return out
}
