package cdcv2

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/publication"
)

// TestPublishResendsOnSerializationConflictAcrossAllWriters simulates Aurora
// DSQL OCC (SQLSTATE 40001) at both PointPublish and PointCommit for Flush,
// Init, and Compact. Every writer resends the exact same staged generation
// with the same cas_token (staging no duplicate manifest), and bounded
// exhaustion leaves the record, generation history and change_log unchanged.
func TestPublishResendsOnSerializationConflictAcrossAllWriters(t *testing.T) {
	for _, kind := range []publication.Kind{publication.KindFlush, publication.KindInit, publication.KindCompact} {
		for _, at := range []Point{PointPublish, PointCommit} {
			t.Run(string(kind)+"/"+string(at)+"/resend_succeeds", func(t *testing.T) {
				f := newFixture(t)
				twoFlushes(t, f)
				if kind == publication.KindFlush {
					f.insert(t, 2)
				}
				before := f.record(t)
				manifestsBefore, _ := f.objects(t)

				var injected atomic.Int32
				fp := func(_ context.Context, p Point) error {
					if p == at && injected.Load() < 2 {
						injected.Add(1)
						return serializationConflict
					}
					return nil
				}
				w := f.writer(t, func(c *Config) { c.Attempts = 5; c.Failpoint = fp })
				resends, gen := runWriterKind(t, f, w, kind)

				require.Equal(t, int32(2), injected.Load(), "injected 40001 twice")
				require.Equal(t, 2, resends, "same publication resent twice on 40001")
				require.Equal(t, before.Generation+1, gen.Generation)
				require.Equal(t, gen, f.record(t))
				manifestsAfter, _ := f.objects(t)
				require.Equal(t, manifestsBefore+1, manifestsAfter, "resending on 40001 stages no duplicate manifest")
				f.checkInvariants(t)
			})

			t.Run(string(kind)+"/"+string(at)+"/exhausted_leaves_nothing_visible", func(t *testing.T) {
				f := newFixture(t)
				twoFlushes(t, f)
				if kind == publication.KindFlush {
					f.insert(t, 2)
				}
				before, dirty, history := f.record(t), f.dirty(t), f.history(t)
				fp := func(_ context.Context, p Point) error {
					if p == at {
						return serializationConflict
					}
					return nil
				}
				w := f.writer(t, func(c *Config) { c.Attempts = 2; c.Failpoint = fp })
				err := runWriterKindErr(f, w, kind)

				require.ErrorIs(t, err, ErrAttemptsExhausted)
				require.ErrorIs(t, err, serializationConflict)
				require.Equal(t, before, f.record(t))
				require.Equal(t, dirty, f.dirty(t))
				require.Equal(t, history, f.history(t))
				f.checkInvariants(t)

				// A clean follow-up succeeds and preserves invariants.
				_, gen := runWriterKind(t, f, f.writer(t), kind)
				require.Equal(t, before.Generation+1, gen.Generation)
				f.checkInvariants(t)
			})
		}
	}
}

func runWriterKind(t *testing.T, f *fixture, w *Writer, kind publication.Kind) (int, publication.Record) {
	t.Helper()
	ctx := context.Background()
	switch kind {
	case publication.KindFlush:
		rep, err := w.Flush(ctx, schemaID, 1, &stager{f: f})
		require.NoError(t, err)
		require.Len(t, rep.Generations, 1)
		return rep.Resends, rep.Generations[0]
	case publication.KindInit:
		rep, err := w.Init(ctx, schemaID, 1, &stager{f: f})
		require.NoError(t, err)
		return rep.Resends, rep.Generation
	case publication.KindCompact:
		rep, err := w.Compact(ctx, schemaID, 1, &compactStager{f: f})
		require.NoError(t, err)
		require.True(t, rep.Committed)
		return rep.Resends, rep.Generation
	default:
		t.Fatalf("unexpected kind %q", kind)
		return 0, publication.Record{}
	}
}

func runWriterKindErr(f *fixture, w *Writer, kind publication.Kind) error {
	ctx := context.Background()
	switch kind {
	case publication.KindFlush:
		_, err := w.Flush(ctx, schemaID, 1, &stager{f: f})
		return err
	case publication.KindInit:
		_, err := w.Init(ctx, schemaID, 1, &stager{f: f})
		return err
	case publication.KindCompact:
		_, err := w.Compact(ctx, schemaID, 1, &compactStager{f: f})
		return err
	default:
		return errors.New("unexpected kind")
	}
}

// TestConcurrentFlushInitAndCompactionSerializeSafely races Flush, Init and
// Compact concurrently from the same parent generation on PostgreSQL and on
// the MiniStack strict DSQL lane: CAS linearizes every commit, rebase/restart
// rules hold, deleted rows never resurrect, and Invariant B holds throughout.
func TestConcurrentFlushInitAndCompactionSerializeSafely(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	deletedRows := twoFlushes(t, f)
	deletedID := deletedRows[0]
	added := f.insert(t, 3)

	flushGate, initGate, compactGate := newGate(PointBegin), newGate(PointBegin), newGate(PointBegin)
	flusher := f.writer(t, func(c *Config) { c.Failpoint = flushGate.failpoint; c.Attempts = 10; c.Restarts = 5 })
	initer := f.writer(t, func(c *Config) { c.Failpoint = initGate.failpoint; c.Attempts = 10; c.Restarts = 5 })
	compactor := f.writer(t, func(c *Config) { c.Failpoint = compactGate.failpoint; c.Attempts = 10; c.Restarts = 5 })

	flushed := async(func() (FlushReport, error) { return flusher.Flush(ctx, schemaID, 1, &stager{f: f}) })
	inited := async(func() (InitReport, error) { return initer.Init(ctx, schemaID, 1, &stager{f: f}) })
	compacted := async(func() (CompactReport, error) { return compactor.Compact(ctx, schemaID, 1, &compactStager{f: f}) })

	flushGate.wait(t)
	initGate.wait(t)
	compactGate.wait(t)
	close(flushGate.released)
	close(initGate.released)
	close(compactGate.released)

	fRes := <-flushed
	iRes := <-inited
	cRes := <-compacted

	committed := 0
	if fRes.err == nil {
		committed++
	} else {
		require.ErrorIs(t, fRes.err, ErrAbandoned)
	}
	if iRes.err == nil {
		committed++
	} else {
		require.ErrorIs(t, iRes.err, ErrAbandoned)
	}
	if cRes.err == nil {
		committed++
	} else {
		require.ErrorIs(t, cRes.err, ErrAbandoned)
	}
	require.GreaterOrEqual(t, committed, 1, "at least one racing publisher commits")
	f.checkInvariants(t)

	// Drain any entries left dirty (for example if the flusher restarted after
	// an init or compaction won) and verify final state and non-resurrection.
	_, err := f.writer(t).Flush(ctx, schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	require.Empty(t, f.dirty(t))
	f.checkInvariants(t)

	held := f.held(t)
	if copy, exists := held[deletedID]; exists {
		require.True(t, copy.Deleted, "deleted row %s must never resurrect", deletedID)
	}
	for _, id := range added {
		copy, exists := held[id]
		require.True(t, exists, "added row %s must be present in head generation", id)
		require.False(t, copy.Deleted, "added row %s must remain live", id)
	}
	require.NotEqual(t, uuid.Nil, f.record(t).Token)
}
