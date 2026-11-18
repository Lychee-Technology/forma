package cdcv2

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// TestInitPublishesTheExportAndHandsItOff re-onboards a published schema:
// the init generation lists the export's base files only, under a new
// lineage, the handoff marks every exported entry at its export version in
// fenced chunks, a deleted row is neither exported nor marked, and no
// change_log entry is deleted.
func TestInitPublishesTheExportAndHandsItOff(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.register(t)
	flushed := f.insert(t, 3)
	_, err := f.writer(t).Flush(ctx, schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	before := f.record(t)
	f.put(t, flushed[0], 2000, true) // deleted, its deletion still dirty
	dirty := f.insert(t, 2*HandoffChunk+100)
	entries := f.entries(t)

	var handoffs atomic.Int32
	count := func(_ context.Context, p Point) error {
		if p == PointHandoff {
			handoffs.Add(1)
		}
		return nil
	}
	w := f.writer(t, func(c *Config) { c.InitBatchSize = 1000; c.Failpoint = count })
	rep, err := w.Init(ctx, schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	require.Equal(t, 2+len(dirty), rep.Rows, "every live row, flushed or not")
	require.Equal(t, 3, rep.Files)
	require.Equal(t, len(dirty), rep.Entries)
	require.Equal(t, len(dirty), rep.Marked)
	require.False(t, rep.HandoffStopped)
	require.Equal(t, int32(3), handoffs.Load(), "chunks of at most HandoffChunk entries")

	head := f.record(t)
	require.Equal(t, before.Generation+1, head.Generation)
	require.NotEqual(t, before.Lineage, head.Lineage, "an init starts a new lineage")
	require.Equal(t, publication.KindInit, f.history(t)[1].Kind)
	m, err := manifestv2.Fetch(ctx, f.store, manifestv2.GenerationRef(schemaID, f.history(t)[1]))
	require.NoError(t, err)
	for _, file := range m.Files {
		require.Equal(t, manifestv2.TierBase, file.Tier, "the flush's delta left the inventory")
	}
	require.NotContains(t, f.held(t), flushed[0])
	require.Equal(t, map[uuid.UUID]int64{flushed[0]: 2000}, f.dirty(t))
	require.Equal(t, entries, f.entries(t), "V2 never clears change_log")
	f.checkInvariants(t)
}

// TestInitOfAnEmptySchemaPublishesAnEmptyGeneration: an export of no rows is
// a statement that the parquet tiers hold nothing, from UNPUBLISHED as from
// AWAITING_INIT.
func TestInitOfAnEmptySchemaPublishesAnEmptyGeneration(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.inTx(t, func(tx *sql.Tx) error {
		_, err := publication.InitializeAwaitingInit(ctx, publication.SQLTx(tx), f.tables, schemaID)
		return err
	})
	rep, err := f.writer(t).Init(ctx, schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	require.Zero(t, rep.Rows)
	head := f.record(t)
	require.Equal(t, publication.Published, head.State)
	require.Empty(t, f.held(t))
	f.checkInvariants(t)
}

// TestInitLeavesRowsThatMovedDirty pins the export versions: a row written
// after its snapshot read it moved past its listed version, so the handoff
// skips it and it stays dirty, and an export every row of which moved is
// still a recorded init generation.
func TestInitLeavesRowsThatMovedDirty(t *testing.T) {
	for _, moved := range []int{1, 4} {
		t.Run(fmt.Sprintf("%d of 4 moved", moved), func(t *testing.T) {
			f := newFixture(t)
			ctx := context.Background()
			f.register(t)
			rows := f.insert(t, 4)
			st := &stager{f: f, during: func(context.Context, []uuid.UUID) error {
				for i, row := range rows[:moved] {
					f.put(t, row, int64(3000+i), false)
				}
				return nil
			}}
			rep, err := f.writer(t).Init(ctx, schemaID, 1, st)
			require.NoError(t, err)
			require.Equal(t, 4, rep.Entries)
			require.Equal(t, 4-moved, rep.Marked)
			require.Len(t, f.dirty(t), moved)
			for _, row := range rows[:moved] {
				require.Equal(t, int64(1000), f.held(t)[row].Version, "the export read the row in its snapshot")
			}
			require.Len(t, f.history(t), 1)
			require.Equal(t, publication.KindInit, f.history(t)[0].Kind, "the init generation stays recorded")
			f.checkInvariants(t)
		})
	}
}

// TestInitFaultLeavesNothingVisible covers an interrupted export, a fault
// after part of the export was staged, and every boundary of the
// publication: the record, the history and change_log stay as they were,
// and running the init again publishes it.
func TestInitFaultLeavesNothingVisible(t *testing.T) {
	interrupted := func(call int) error {
		if call == 2 {
			return errInjected
		}
		return nil
	}
	for _, tc := range []struct {
		name   string
		fail   func(int) error
		failAt Point
	}{
		{name: "interrupted export", fail: interrupted},
		{name: "partial staging", failAt: PointStageManifest},
		{name: "begin", failAt: PointBegin},
		{name: "publish", failAt: PointPublish},
		{name: "commit", failAt: PointCommit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			ctx := context.Background()
			published(t, f)
			before, dirty, history := f.record(t), f.dirty(t), f.history(t)
			w := f.writer(t, func(c *Config) {
				c.InitBatchSize = 2
				if tc.failAt != "" {
					c.Failpoint = once(tc.failAt, failWith(errInjected))
				}
			})
			_, err := w.Init(ctx, schemaID, 1, &stager{f: f, fail: tc.fail})
			require.ErrorIs(t, err, errInjected)
			require.Equal(t, before, f.record(t))
			require.Equal(t, dirty, f.dirty(t))
			require.Equal(t, history, f.history(t))
			_, parquet := f.objects(t)
			require.Positive(t, parquet)

			rep, err := f.writer(t).Init(ctx, schemaID, 1, &stager{f: f})
			require.NoError(t, err)
			require.Equal(t, 4, rep.Rows)
			require.Empty(t, f.dirty(t))
			f.checkInvariants(t)
		})
	}
}

// TestInitResolvesACommitWithoutAnAnswer: a lost answer of a COMMIT that
// landed is Committed, and the handoff runs; one of a COMMIT that did not
// land is resent with the same token. A handoff chunk whose answer was lost
// is sent again and marks nothing twice.
func TestInitResolvesACommitWithoutAnAnswer(t *testing.T) {
	for _, tc := range []struct {
		name string
		fp   func(context.Context, Point) error
	}{
		{"commit landed", once(PointCommitAnswer, failWith(errors.New("connection reset")))},
		{"commit lost", once(PointCommit, failWith(ErrCommitAnswerLost))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			ctx := context.Background()
			published(t, f)
			entries := f.entries(t)
			rep, err := f.writer(t, func(c *Config) { c.Failpoint = tc.fp }).Init(ctx, schemaID, 1, &stager{f: f})
			require.NoError(t, err)
			require.Equal(t, 2, rep.Marked)
			require.Equal(t, rep.Generation.Token, f.record(t).Token)
			require.Len(t, f.history(t), 2)
			require.Empty(t, f.dirty(t))
			require.Equal(t, entries, f.entries(t))
			f.checkInvariants(t)
		})
	}

	t.Run("the lost commit lands after its resend began", func(t *testing.T) {
		f := newFixture(t)
		published(t, f)
		rep, err := f.writer(t, func(c *Config) { c.Failpoint = f.lateCommit(t) }).Init(context.Background(), schemaID, 1, &stager{f: f})
		require.NoError(t, err, "an init never rebases over an init, so taking its own commit for a rival's would abandon it")
		require.Equal(t, 2, rep.Marked, "the handoff runs")
		require.Equal(t, rep.Generation.Token, f.record(t).Token)
		require.Len(t, f.history(t), 2)
		f.checkInvariants(t)
	})

	t.Run("handoff answer lost", func(t *testing.T) {
		f := newFixture(t)
		published(t, f)
		var answers atomic.Int32
		fp := func(_ context.Context, p Point) error {
			if p == PointCommitAnswer && answers.Add(1) == 2 { // the first handoff chunk
				return errors.New("connection reset")
			}
			return nil
		}
		rep, err := f.writer(t, func(c *Config) { c.Failpoint = fp }).Init(context.Background(), schemaID, 1, &stager{f: f})
		require.NoError(t, err)
		require.Zero(t, rep.Marked, "the resent chunk found its entries marked")
		require.Empty(t, f.dirty(t))
		f.checkInvariants(t)
	})
}

// TestInitRetryIsIdempotent runs the init twice: the second exports the
// same rows under another lineage, has nothing left to mark, and deletes
// no entry.
func TestInitRetryIsIdempotent(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	published(t, f)
	first, err := f.writer(t).Init(ctx, schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	entries := f.entries(t)
	second, err := f.writer(t).Init(ctx, schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	require.Equal(t, first.Rows, second.Rows)
	require.Zero(t, second.Entries)
	require.NotEqual(t, first.Generation.Lineage, second.Generation.Lineage)
	require.Equal(t, entries, f.entries(t))
	f.checkInvariants(t)
}

// TestReinitStopsTheReplacedLineagesHandoff: a cdc-init that publishes while
// an earlier one hands off replaces its lineage, so the earlier handoff
// stops at its next chunk and leaves its entries to the later one.
func TestReinitStopsTheReplacedLineagesHandoff(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.register(t)
	f.insert(t, HandoffChunk+10)
	var later InitReport
	reinit := func(ctx context.Context) error {
		var err error
		later, err = f.writer(t).Init(ctx, schemaID, 1, &stager{f: f})
		return err
	}
	rep, err := f.writer(t, func(c *Config) { c.Failpoint = once(PointHandoff, reinit) }).Init(ctx, schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	require.True(t, rep.HandoffStopped)
	require.Zero(t, rep.Marked)
	require.Equal(t, HandoffChunk+10, later.Marked)
	require.Equal(t, later.Generation.Lineage, f.record(t).Lineage)
	require.Empty(t, f.dirty(t))
	f.checkInvariants(t)
}
