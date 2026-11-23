package cdc

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/cdcv2"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/schemaledger"
)

// requireMarksHeld checks invariant B on the wired path: every entry the
// flush marked, the dirty entries before it that are gone after it, is held
// by the record's committed generation at exactly the version it was marked
// at.
func requireMarksHeld(t *testing.T, f *wiredFixture, before, after map[uuid.UUID]int64) {
	t.Helper()
	rec, ok := f.record(t)
	require.True(t, ok)
	held := f.generationRows(t, f.manifest(t, rec))
	for id, v := range before {
		if after[id] == v {
			continue
		}
		require.Contains(t, held[id], v, "row %s was marked at version %d, which the committed generation does not hold", id, v)
	}
}

// TestWiredFlushPublishesImmutableGenerationsThroughTheRecord: cdc-flush
// stages its delta objects and a manifest generation, then the record names
// that generation in the transaction that marks the batch. The next flush
// publishes a new generation that lists the parent's files and its own, and
// the parent's objects are never rewritten.
func TestWiredFlushPublishesImmutableGenerationsThroughTheRecord(t *testing.T) {
	f := newWiredFixture(t)
	ctx := context.Background()
	f.register(t, publication.Unpublished)
	base := time.Now().UnixMilli() - 60_000
	ids := f.seed(t, 3, base)

	before := f.dirty(t)
	rep, err := f.flush(t, nil, false)
	require.NoError(t, err)
	require.Len(t, rep.Flushed, 1)
	require.Equal(t, SchemaFlush{SchemaID: wiredSchema, Epoch: 1, Generations: []int64{1}, Selected: 3, Marked: 3}, rep.Flushed[0])
	require.Empty(t, f.dirty(t))
	requireMarksHeld(t, f, before, f.dirty(t))

	first, ok := f.record(t)
	require.True(t, ok)
	require.Equal(t, publication.Published, first.State)
	require.Equal(t, int64(1), first.Generation)
	m1 := f.manifest(t, first)
	require.Equal(t, publication.KindFlush, m1.Kind)
	require.Len(t, m1.Files, 1)
	delta := m1.Files[0]
	require.Equal(t, manifestv2.TierDelta, delta.Tier)
	require.True(t, strings.HasPrefix(delta.Key, "data/7/e1/delta/"), delta.Key)
	require.Equal(t, int64(3), delta.RowCount)
	raw, err := f.store.Get(ctx, delta.Key)
	require.NoError(t, err)
	require.Equal(t, manifestv2.Digest(raw), delta.SHA256, "the entry's sha256 is the stored object's content digest")
	require.Equal(t, int64(len(raw)), delta.SizeBytes)
	require.Contains(t, delta.Columns, "row_id")
	rows := f.fileRows(t, delta.Key)
	for i, id := range ids {
		require.Equal(t, base+int64(i)+1, rows[id].version)
	}
	manifest1, err := f.store.Get(ctx, first.ManifestKey)
	require.NoError(t, err)

	f.write(t, ids[0], base+100, "rewritten", false)
	f.seed(t, 1, base+200)
	before = f.dirty(t)
	rep, err = f.flush(t, nil, false)
	require.NoError(t, err)
	require.Equal(t, []int64{2}, rep.Flushed[0].Generations)
	requireMarksHeld(t, f, before, f.dirty(t))

	second, _ := f.record(t)
	require.Equal(t, int64(2), second.Generation)
	require.Equal(t, first.Lineage, second.Lineage, "a flush keeps its parent's lineage")
	m2 := f.manifest(t, second)
	require.Len(t, m2.Files, 2)
	require.Contains(t, m2.Keys(), delta.Key, "a flush generation lists its parent's files")
	again, err := f.store.Get(ctx, first.ManifestKey)
	require.NoError(t, err)
	require.True(t, bytes.Equal(manifest1, again), "the parent generation's manifest is never rewritten")
	require.Equal(t, rows, f.fileRows(t, delta.Key), "the parent's delta object is never rewritten")
}

// TestWiredFlushRefusesASchemaItMayNotPublish: a schema without a record,
// one AWAITING_INIT and a dropped one are refused before
// anything is staged or marked, each with what the operator does about it.
func TestWiredFlushRefusesASchemaItMayNotPublish(t *testing.T) {
	f := newWiredFixture(t)
	f.seed(t, 2, time.Now().UnixMilli()-60_000)
	dirty := f.dirty(t)

	_, err := f.flush(t, nil, false)
	require.ErrorIs(t, err, publication.ErrNoRecord)
	require.ErrorContains(t, err, "init-db writes it")

	f.register(t, publication.AwaitingInit)
	_, err = f.flush(t, nil, false)
	require.ErrorIs(t, err, publication.ErrAwaitingInit)
	require.ErrorContains(t, err, "run cdc-init for it")

	require.NoError(t, f.drop(context.Background()))
	_, err = f.flush(t, nil, false)
	require.ErrorIs(t, err, publication.ErrDropped)

	parquet, manifests := f.objects(t)
	require.Zero(t, parquet+manifests, "a refused flush stages nothing")
	require.Equal(t, dirty, f.dirty(t), "a refused flush marks nothing")
}

// TestWiredFlushFaultBeforeCommitLeavesNoPartialState fails cdc-flush at
// each boundary before COMMIT. Each leaves the record and every entry as
// they were, at most orphan objects no commit names, and the next flush
// publishes the entries once.
func TestWiredFlushFaultBeforeCommitLeavesNoPartialState(t *testing.T) {
	for _, point := range []cdcv2.Point{cdcv2.PointStageObjects, cdcv2.PointStageManifest, cdcv2.PointBegin, cdcv2.PointPublish, cdcv2.PointMarks, cdcv2.PointCommit} {
		t.Run(string(point), func(t *testing.T) {
			f := newWiredFixture(t)
			f.register(t, publication.Unpublished)
			base := time.Now().UnixMilli() - 60_000
			f.seed(t, 2, base)
			_, err := f.flush(t, nil, false)
			require.NoError(t, err)
			f.seed(t, 2, base+100)
			rec, dirty := mustRecord(t, f), f.dirty(t)

			_, err = f.flush(t, nil, false, failAt(string(point), 1, errWiredFault))
			require.ErrorIs(t, err, errWiredFault)
			require.Equal(t, rec, mustRecord(t, f), "a failed flush leaves the record")
			require.Equal(t, dirty, f.dirty(t), "a failed flush marks nothing")

			rep, err := f.flush(t, nil, false)
			require.NoError(t, err)
			require.Equal(t, 2, rep.Flushed[0].Marked)
			require.Empty(t, f.dirty(t))
			require.Equal(t, rec.Generation+1, mustRecord(t, f).Generation, "the retry publishes one generation")
			requireMarksHeld(t, f, dirty, f.dirty(t))
		})
	}
}

// TestWiredFlushResolvesALostCommitAnswer: a COMMIT that landed but whose
// answer was lost is resolved as committed, with its marks.
func TestWiredFlushResolvesALostCommitAnswer(t *testing.T) {
	f := newWiredFixture(t)
	f.register(t, publication.Unpublished)
	f.seed(t, 3, time.Now().UnixMilli()-60_000)
	dirty := f.dirty(t)

	rep, err := f.flush(t, nil, false, failAt(string(cdcv2.PointCommitAnswer), 1, errWiredFault))
	require.NoError(t, err)
	require.Equal(t, []int64{1}, rep.Flushed[0].Generations)
	require.Empty(t, f.dirty(t))
	requireMarksHeld(t, f, dirty, f.dirty(t))
}

// TestWiredFlushDryRunChangesNothing: a dry run stages no object or
// manifest, leaves the record and marks nothing (#180).
func TestWiredFlushDryRunChangesNothing(t *testing.T) {
	f := newWiredFixture(t)
	f.register(t, publication.Unpublished)
	f.seed(t, 2, time.Now().UnixMilli()-60_000)
	rec, dirty := mustRecord(t, f), f.dirty(t)

	rep, err := f.flush(t, nil, true)
	require.NoError(t, err)
	require.Empty(t, rep.Flushed)
	parquet, manifests := f.objects(t)
	require.Zero(t, parquet+manifests)
	require.Equal(t, rec, mustRecord(t, f))
	require.Equal(t, dirty, f.dirty(t))
}

// TestWiredFlushFailsClosedOnAnEpochChange: a drop while the flush stages
// moves the record to its next epoch. The compare-and-set misses, the writer
// may not rebase across epochs, and the flush fails closed on the epoch the
// run loaded: nothing is published or marked.
func TestWiredFlushFailsClosedOnAnEpochChange(t *testing.T) {
	f := newWiredFixture(t)
	f.register(t, publication.Unpublished)
	f.seed(t, 2, time.Now().UnixMilli()-60_000)
	dirty := f.dirty(t)

	_, err := f.flush(t, nil, false, runAt(string(cdcv2.PointStageManifest), f.drop))
	require.ErrorIs(t, err, cdcv2.ErrEpochChanged)
	require.ErrorContains(t, err, "run it again")
	rec := mustRecord(t, f)
	require.Equal(t, publication.Dropped, rec.State)
	require.Equal(t, int64(2), rec.Epoch)
	require.Equal(t, dirty, f.dirty(t))
}

// TestWiredFlushStagesNothingAcrossARecordedGeneration: an export written
// through the run's declared types while apply-schema recorded a new
// generation never becomes publishable (#733).
func TestWiredFlushStagesNothingAcrossARecordedGeneration(t *testing.T) {
	f := newWiredFixture(t)
	f.register(t, publication.Unpublished)
	f.seed(t, 2, time.Now().UnixMilli()-60_000)
	rec, dirty := mustRecord(t, f), f.dirty(t)
	registry := &trackingRegistry{active: 1}

	_, err := f.flush(t, registry, false, func(c *CDCConfig) {
		c.BeforeExportHook = func(context.Context, int16, []uuid.UUID, int64) error {
			registry.apply()
			return nil
		}
	})
	require.ErrorIs(t, err, schemaledger.ErrGenerationChanged)
	parquet, manifests := f.objects(t)
	require.Zero(t, parquet+manifests, "the export is never staged")
	require.Equal(t, rec, mustRecord(t, f))
	require.Equal(t, dirty, f.dirty(t))
}

// TestWiredFlushOfRewrittenRowsPublishesNothing: a batch whose every entry
// was rewritten past the export's cutoff between the selection and the
// export exports no row. No generation lists an object of no row, so nothing
// is published, the entries stay dirty at their new versions, and the next
// flush publishes them.
func TestWiredFlushOfRewrittenRowsPublishesNothing(t *testing.T) {
	f := newWiredFixture(t)
	f.register(t, publication.Unpublished)
	ids := f.seed(t, 2, time.Now().UnixMilli()-60_000)
	ahead := time.Now().Add(time.Hour).UnixMilli()

	_, err := f.flush(t, nil, false, func(c *CDCConfig) {
		c.BeforeExportHook = func(context.Context, int16, []uuid.UUID, int64) error {
			for i, id := range ids {
				f.write(t, id, ahead+int64(i), "rewritten", false)
			}
			return nil
		}
	})
	require.NoError(t, err)
	require.Equal(t, publication.Unpublished, mustRecord(t, f).State)
	parquet, _ := f.objects(t)
	require.Zero(t, parquet, "an export of no row is not staged")
	require.Equal(t, map[uuid.UUID]int64{ids[0]: ahead, ids[1]: ahead + 1}, f.dirty(t))

	dirty := f.dirty(t)
	_, err = f.flush(t, nil, false)
	require.NoError(t, err)
	require.Empty(t, f.dirty(t))
	requireMarksHeld(t, f, dirty, f.dirty(t))
}

func mustRecord(t *testing.T, f *wiredFixture) publication.Record {
	t.Helper()
	rec, ok := f.record(t)
	require.True(t, ok, "the schema has a publication record")
	return rec
}
