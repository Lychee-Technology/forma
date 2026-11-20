package cdcv2

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// twoFlushes sets up a published schema of two flush generations, a delete
// among them, so a compaction has deltas and a tombstone to merge.
func twoFlushes(t *testing.T, f *fixture) []uuid.UUID {
	t.Helper()
	published(t, f)
	rows := f.insert(t, 1)
	_, err := f.writer(t).Flush(context.Background(), schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	f.put(t, rows[0], 2000, true)
	_, err = f.writer(t).Flush(context.Background(), schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	require.Empty(t, f.dirty(t))
	return rows
}

// TestCompactPublishesOneGenerationFromItsInput: a compaction commits
// exactly one compact generation whose parent and input are the generation
// it read, listing the merge output in place of the sources. The head holds
// every row's newest version as before, tombstones included, and the
// replaced generation stays readable with every object it listed.
func TestCompactPublishesOneGenerationFromItsInput(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	rows := twoFlushes(t, f)
	before, held := f.record(t), f.held(t)
	require.True(t, held[rows[0]].Deleted)

	rep, err := f.writer(t).Compact(ctx, schemaID, 0, &compactStager{f: f})
	require.NoError(t, err)
	require.True(t, rep.Committed)
	require.Equal(t, before, rep.Input)
	require.Equal(t, before.Generation+1, rep.Generation.Generation)
	require.Equal(t, before.Generation, rep.Generation.ParentGeneration)
	require.Equal(t, before.Lineage, rep.Generation.Lineage, "a compaction keeps the lineage")
	require.Equal(t, rep.Generation, f.record(t))

	m, err := manifestv2.Fetch(ctx, f.store, mustRef(t, f.record(t)))
	require.NoError(t, err)
	require.Equal(t, publication.KindCompact, m.Kind)
	require.Equal(t, before.Generation, m.InputGeneration)
	require.Equal(t, []string{rep.Staging.Outputs[0].Key}, m.Keys())
	require.Len(t, rep.Staging.Merged, 3)

	require.Equal(t, held, f.held(t), "the merge keeps each row's newest version, tombstones included")
	require.Equal(t, []publication.Kind{publication.KindFlush, publication.KindFlush, publication.KindFlush, publication.KindCompact}, kinds(f.history(t)))
	f.generationReadable(t, before.Generation)
	f.checkInvariants(t)
}

// TestCompactOfNothingPublishedCommitsNothing: an unpublished schema and a
// compaction that merges nothing commit nothing; an AWAITING_INIT and a
// dropped schema are refused.
func TestCompactOfNothingPublishedCommitsNothing(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.register(t)
	rep, err := f.writer(t).Compact(ctx, schemaID, 0, &compactStager{f: f})
	require.NoError(t, err)
	require.False(t, rep.Committed)
	require.Empty(t, f.history(t))

	f.insert(t, 2)
	_, err = f.writer(t).Flush(ctx, schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	head := f.record(t)
	none := &compactStager{f: f, pick: func(*manifestv2.Manifest) []manifestv2.File { return nil }}
	rep, err = f.writer(t).Compact(ctx, schemaID, 0, none)
	require.NoError(t, err)
	require.False(t, rep.Committed)
	require.Equal(t, head, f.record(t))

	f.drop(t)
	_, err = f.writer(t).Compact(ctx, schemaID, 0, &compactStager{f: f})
	require.ErrorIs(t, err, publication.ErrDropped)
	_, err = f.writer(t).Compact(ctx, schemaID, 1, &compactStager{f: f})
	require.ErrorIs(t, err, ErrEpochChanged)
}

// TestCompactRebasesOverAFlush: a flush that commits while a compaction is
// staged appends files the compaction never read. The compaction rebases
// onto it under a fresh token, keeps the flush's delta and its marks, and
// names the generation it merged as its input.
func TestCompactRebasesOverAFlush(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	twoFlushes(t, f)
	input := f.record(t)
	g := newGate(PointBegin)
	w := f.writer(t, func(c *Config) { c.Failpoint = g.failpoint })
	out := async(func() (CompactReport, error) { return w.Compact(ctx, schemaID, 1, &compactStager{f: f}) })
	g.wait(t)
	f.insert(t, 3)
	flushed, err := f.writer(t).Flush(ctx, schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	close(g.released)

	res := <-out
	require.NoError(t, res.err)
	require.True(t, res.v.Committed)
	require.Equal(t, 1, res.v.Rebases)
	require.Zero(t, res.v.Restarts)
	head := f.record(t)
	require.Equal(t, flushed.Generations[0].Generation, head.ParentGeneration)
	m, err := manifestv2.Fetch(ctx, f.store, mustRef(t, head))
	require.NoError(t, err)
	require.Equal(t, input.Generation, m.InputGeneration, "the input stays the generation it merged")
	require.Len(t, m.Files, 2, "the merge output and the flush's delta")
	require.Empty(t, f.dirty(t))
	f.generationReadable(t, flushed.Generations[0].Generation)
	f.checkInvariants(t)
}

// TestCompactStagedBeforeACompactionOrAnInitStartsOver: another compaction
// or an init may have removed files the compaction merged, so it never
// rebases over either. It abandons its staging and stages again from the
// new head, within Config.Restarts; with no restart left it reports
// ErrAbandoned and leaves the record as the rival committed it.
func TestCompactStagedBeforeACompactionOrAnInitStartsOver(t *testing.T) {
	rivals := map[string]func(t *testing.T, f *fixture){
		"compaction": func(t *testing.T, f *fixture) { f.compact(t) },
		"init": func(t *testing.T, f *fixture) {
			_, err := f.writer(t).Init(context.Background(), schemaID, 1, &stager{f: f})
			require.NoError(t, err)
		},
	}
	for name, rival := range rivals {
		for _, restarts := range []int{1, -1} {
			t.Run(fmt.Sprintf("%s restarts %d", name, restarts), func(t *testing.T) {
				f := newFixture(t)
				ctx := context.Background()
				twoFlushes(t, f)
				g := newGate(PointBegin)
				w := f.writer(t, func(c *Config) { c.Failpoint = g.failpoint; c.Restarts = restarts })
				if restarts < 0 {
					w.cfg.Restarts = 0
				}
				out := async(func() (CompactReport, error) { return w.Compact(ctx, schemaID, 1, &compactStager{f: f}) })
				g.wait(t)
				rival(t, f)
				head := f.record(t)
				close(g.released)

				res := <-out
				if restarts < 0 {
					require.ErrorIs(t, res.err, ErrAbandoned)
					require.ErrorIs(t, res.err, publication.ErrRebaseRefused)
					require.Equal(t, head, f.record(t))
				} else {
					require.NoError(t, res.err)
					require.Equal(t, 1, res.v.Restarts)
					require.True(t, res.v.Committed)
					require.Equal(t, head, res.v.Input, "the second round merged the rival's generation")
					require.Equal(t, head.Generation, f.record(t).ParentGeneration)
				}
				f.checkInvariants(t)
			})
		}
	}
}

// TestCompactFaultAtEveryBoundaryLeavesOnlyOrphans fails a compaction at
// each boundary before COMMIT. Each leaves the record and the history as
// they were and the head readable; what it staged is unreferenced, and a
// later compaction publishes.
func TestCompactFaultAtEveryBoundaryLeavesOnlyOrphans(t *testing.T) {
	for _, at := range []Point{PointStageObjects, PointStageManifest, PointBegin, PointPublish, PointMarks, PointCommit} {
		t.Run(string(at), func(t *testing.T) {
			f := newFixture(t)
			ctx := context.Background()
			twoFlushes(t, f)
			before, history, held := f.record(t), f.history(t), f.held(t)

			_, err := f.writer(t, func(c *Config) { c.Failpoint = once(at, failWith(errInjected)) }).Compact(ctx, schemaID, 1, &compactStager{f: f})
			require.ErrorIs(t, err, errInjected)
			require.Equal(t, before, f.record(t))
			require.Equal(t, history, f.history(t))
			require.Equal(t, held, f.held(t))
			f.checkInvariants(t)

			rep, err := f.writer(t).Compact(ctx, schemaID, 1, &compactStager{f: f})
			require.NoError(t, err)
			require.True(t, rep.Committed)
			require.Equal(t, held, f.held(t))
			f.checkInvariants(t)
		})
	}

	t.Run("the merge", func(t *testing.T) {
		f := newFixture(t)
		twoFlushes(t, f)
		before := f.record(t)
		failing := &compactStager{f: f, fail: func(int) error { return errInjected }}
		_, err := f.writer(t).Compact(context.Background(), schemaID, 1, failing)
		require.ErrorIs(t, err, errInjected)
		require.Equal(t, before, f.record(t))
	})
}

// TestCompactResolvesACommitWithoutAnAnswer: a lost COMMIT that did not
// land is resent with the same token; one that landed is taken as
// committed. Neither commits a second generation.
func TestCompactResolvesACommitWithoutAnAnswer(t *testing.T) {
	for _, tc := range []struct {
		at      Point
		resends int
	}{{PointCommit, 1}, {PointCommitAnswer, 0}} {
		t.Run(string(tc.at), func(t *testing.T) {
			f := newFixture(t)
			twoFlushes(t, f)
			before := f.record(t)
			fp := once(tc.at, failWith(fmt.Errorf("connection reset: %w", ErrCommitAnswerLost)))
			if tc.at == PointCommitAnswer {
				fp = once(tc.at, failWith(errors.New("connection reset")))
			}
			rep, err := f.writer(t, func(c *Config) { c.Failpoint = fp }).Compact(context.Background(), schemaID, 1, &compactStager{f: f})
			require.NoError(t, err)
			require.True(t, rep.Committed)
			require.Equal(t, tc.resends, rep.Resends)
			head := f.record(t)
			require.Equal(t, before.Generation+1, head.Generation)
			require.Equal(t, rep.Generation.Token, head.Token)
			f.checkInvariants(t)
		})
	}
}

// TestCompactStagedBeforeADropNeverCommits: a drop moves the epoch, so the
// compaction's compare-and-set misses and it may not rebase; the restart
// reads a record of another epoch than the one it pinned and fails closed.
func TestCompactStagedBeforeADropNeverCommits(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	twoFlushes(t, f)
	g := newGate(PointBegin)
	w := f.writer(t, func(c *Config) { c.Failpoint = g.failpoint })
	out := async(func() (CompactReport, error) { return w.Compact(ctx, schemaID, 0, &compactStager{f: f}) })
	g.wait(t)
	dropped, history := f.drop(t), f.history(t)
	close(g.released)

	res := <-out
	require.ErrorIs(t, res.err, ErrEpochChanged)
	require.Equal(t, 1, res.v.Restarts)
	require.Equal(t, dropped, f.record(t))
	require.Equal(t, history, f.history(t))
}

// TestAFlushStagedBeforeACompactionCannotResurrectADeletedRow stages the
// sequence the tombstone rule exists for: a flush exports r live at v1 and
// stalls; r is deleted at v2 and a rival flush publishes the tombstone and
// marks it; a compaction merges everything. The stale flush rebases over the
// flush and the compaction, so its r@v1 joins the head beside the compacted
// tombstone, which LWW keeps winning: r stays deleted.
func TestAFlushStagedBeforeACompactionCannotResurrectADeletedRow(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	published(t, f)
	_, err := f.writer(t).Flush(ctx, schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	row := f.insert(t, 1)[0]
	g := newGate(PointBegin)
	stale := f.writer(t, func(c *Config) { c.Failpoint = g.failpoint })
	only := map[uuid.UUID]bool{row: true}
	out := async(func() (FlushReport, error) {
		return stale.Flush(ctx, schemaID, 1, &stager{f: f, beforeRead: func(_ context.Context, rows []uuid.UUID) error {
			if len(rows) != 1 || !only[rows[0]] {
				return fmt.Errorf("the stale flush selected %v, want only %s", rows, row)
			}
			return nil
		}})
	})
	g.wait(t)
	f.put(t, row, 5000, true)
	_, err = f.writer(t).Flush(ctx, schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	_, err = f.writer(t).Compact(ctx, schemaID, 1, &compactStager{f: f})
	require.NoError(t, err)
	close(g.released)

	res := <-out
	require.NoError(t, res.err)
	require.Equal(t, 1, res.v.Rebases, "the stale flush rebased over a flush and a compaction")
	require.Equal(t, rowCopy{Row: row, Version: 5000, Deleted: true}, f.held(t)[row])
	require.Equal(t, []publication.Kind{publication.KindFlush, publication.KindFlush, publication.KindFlush, publication.KindCompact, publication.KindFlush}, kinds(f.history(t)))
	f.checkInvariants(t)
}

func mustRef(t *testing.T, rec publication.Record) manifestv2.Ref {
	t.Helper()
	ref, err := manifestv2.RecordRef(rec)
	require.NoError(t, err)
	return ref
}
