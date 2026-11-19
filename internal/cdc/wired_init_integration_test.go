package cdc

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/cdcv2"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// onboardingFixture is a schema with history the cutover left AWAITING_INIT:
// two live rows with unflushed entries, one live row whose entry a V1 flush
// marked, and one deleted row whose tombstone is unflushed.
type onboardingFixture struct {
	*wiredFixture
	live          []uuid.UUID
	clean, gone   uuid.UUID
	versions      map[uuid.UUID]int64
	entriesBefore int
}

func newOnboardingFixture(t *testing.T) *onboardingFixture {
	t.Helper()
	f := &onboardingFixture{wiredFixture: newWiredFixture(t), versions: map[uuid.UUID]int64{}}
	base := time.Now().UnixMilli() - 60_000
	f.live = f.seed(t, 2, base)
	f.clean, f.gone = uuid.New(), uuid.New()
	f.write(t, f.clean, base+10, "clean", false)
	_, err := f.db.Exec("UPDATE "+sanitizeIdentifier(f.cfg.ChangeLogTable)+" SET flushed_at = 500 WHERE schema_id = $1 AND row_id = $2", wiredSchema, f.clean)
	require.NoError(t, err)
	f.write(t, f.gone, base+20, "gone", true)
	for i, id := range f.live {
		f.versions[id] = base + int64(i) + 1
	}
	f.versions[f.clean] = base + 10
	f.register(t, publication.AwaitingInit)
	f.entriesBefore = f.entries(t)
	return f
}

// TestWiredInitPublishesAnInitGenerationAndHandsOffItsEntries: cdc-init
// exports every live row in the snapshot that read its version, publishes
// the base objects as an init generation, and hands off exactly the exported
// rows' unflushed entries. It never clears change_log: the tombstone of the
// deleted row stays dirty, and the next flush publishes it on top of the
// init. A second cdc-init re-onboards the published schema under a new
// lineage, and its generation lists only its own export.
func TestWiredInitPublishesAnInitGenerationAndHandsOffItsEntries(t *testing.T) {
	f := newOnboardingFixture(t)

	sum, err := f.runInit(t, nil, false)
	require.NoError(t, err)
	require.Equal(t, []SchemaInit{{SchemaID: wiredSchema, Epoch: 1, Generation: 1, Rows: 3, Files: 1, Entries: 2, Marked: 2}}, sum.Schemas)
	rec := mustRecord(t, f.wiredFixture)
	require.Equal(t, publication.Published, rec.State)
	m := f.manifest(t, rec)
	require.Equal(t, publication.KindInit, m.Kind)
	require.Len(t, m.Files, 1)
	require.Equal(t, manifestv2.TierBase, m.Files[0].Tier)
	require.True(t, strings.HasPrefix(m.Files[0].Key, "data/7/e1/base/"), m.Files[0].Key)
	held := f.generationRows(t, m)
	require.Len(t, held, 3, "the export holds the live rows only")
	for id, v := range f.versions {
		require.Equal(t, []int64{v}, held[id], "row %s exported at its version", id)
	}
	require.Equal(t, map[uuid.UUID]int64{f.gone: f.dirtyVersion(t, f.gone)}, f.dirty(t), "the handoff marks the exported entries only")
	require.Equal(t, f.entriesBefore, f.entries(t), "cdc-init never clears change_log")

	_, err = f.flush(t, nil, false)
	require.NoError(t, err, "a flush publishes on top of the init")
	afterFlush := mustRecord(t, f.wiredFixture)
	require.Equal(t, int64(2), afterFlush.Generation)
	require.Len(t, f.manifest(t, afterFlush).Files, 2)
	require.Empty(t, f.dirty(t))

	_, err = f.runInit(t, nil, false)
	require.NoError(t, err)
	again := mustRecord(t, f.wiredFixture)
	require.Equal(t, int64(3), again.Generation)
	require.NotEqual(t, afterFlush.Lineage, again.Lineage, "a re-init starts a new lineage")
	m3 := f.manifest(t, again)
	require.Len(t, m3.Files, 1, "an init generation lists its own export only")
	require.NotContains(t, m3.Keys(), m.Files[0].Key)
}

// dirtyVersion answers id's unflushed version.
func (f *onboardingFixture) dirtyVersion(t *testing.T, id uuid.UUID) int64 {
	t.Helper()
	v, ok := f.dirty(t)[id]
	require.True(t, ok, "row %s has an unflushed entry", id)
	return v
}

// TestWiredInitOfAnEmptySchemaPublishesAnEmptyGeneration: a schema with no
// live row publishes an init generation that lists no file, and no
// placeholder object is written.
func TestWiredInitOfAnEmptySchemaPublishesAnEmptyGeneration(t *testing.T) {
	f := newWiredFixture(t)
	f.register(t, publication.AwaitingInit)

	sum, err := f.runInit(t, nil, false)
	require.NoError(t, err)
	require.Zero(t, sum.TotalFilesCreated)
	rec := mustRecord(t, f)
	require.Equal(t, publication.Published, rec.State)
	require.Empty(t, f.manifest(t, rec).Files)
	parquet, manifests := f.objects(t)
	require.Zero(t, parquet, "no zero-row placeholder object")
	require.Equal(t, 1, manifests)
}

// TestWiredInitRefusesADroppedSchemaOrOneWithoutRecord.
func TestWiredInitRefusesADroppedSchemaOrOneWithoutRecord(t *testing.T) {
	f := newWiredFixture(t)
	f.seed(t, 1, time.Now().UnixMilli()-60_000)
	_, err := f.runInit(t, nil, false)
	require.ErrorIs(t, err, publication.ErrNoRecord)

	f.register(t, publication.Unpublished)
	require.NoError(t, f.drop(context.Background()))
	_, err = f.runInit(t, nil, false)
	require.ErrorIs(t, err, publication.ErrDropped)
	parquet, manifests := f.objects(t)
	require.Zero(t, parquet+manifests)
}

// TestWiredInitFaultBeforeCommitLeavesNoPartialState fails cdc-init at each
// boundary before its publication commits: the record stays AWAITING_INIT,
// no entry is marked, and a rerun onboards the schema.
func TestWiredInitFaultBeforeCommitLeavesNoPartialState(t *testing.T) {
	for _, point := range []cdcv2.Point{cdcv2.PointStageObjects, cdcv2.PointStageManifest, cdcv2.PointBegin, cdcv2.PointPublish, cdcv2.PointCommit} {
		t.Run(string(point), func(t *testing.T) {
			f := newOnboardingFixture(t)
			rec, dirty := mustRecord(t, f.wiredFixture), f.dirty(t)

			_, err := f.runInit(t, nil, false, failAt(string(point), 1, errWiredFault))
			require.ErrorIs(t, err, errWiredFault)
			require.Equal(t, rec, mustRecord(t, f.wiredFixture))
			require.Equal(t, dirty, f.dirty(t))

			sum, err := f.runInit(t, nil, false)
			require.NoError(t, err)
			require.Equal(t, 2, sum.Schemas[0].Marked)
			require.Equal(t, int64(1), mustRecord(t, f.wiredFixture).Generation)
		})
	}
}

// TestWiredInitHandoffFaultLeavesTheEntriesDirty: an init whose handoff
// fails after its generation committed is published, and the entries it did
// not mark stay dirty, which the next flush publishes.
func TestWiredInitHandoffFaultLeavesTheEntriesDirty(t *testing.T) {
	f := newOnboardingFixture(t)
	dirty := f.dirty(t)

	_, err := f.runInit(t, nil, false, failAt(string(cdcv2.PointHandoff), 1, errWiredFault))
	require.ErrorIs(t, err, errWiredFault)
	require.Equal(t, publication.Published, mustRecord(t, f.wiredFixture).State)
	require.Equal(t, dirty, f.dirty(t), "nothing is marked without its handoff")

	_, err = f.flush(t, nil, false)
	require.NoError(t, err)
	require.Empty(t, f.dirty(t))
	requireMarksHeld(t, f.wiredFixture, dirty, f.dirty(t))
}

// TestWiredInitResolvesALostCommitAnswer: an init whose COMMIT landed
// without an answer is resolved as committed, and its handoff runs.
func TestWiredInitResolvesALostCommitAnswer(t *testing.T) {
	f := newOnboardingFixture(t)

	sum, err := f.runInit(t, nil, false, failAt(string(cdcv2.PointCommitAnswer), 1, errWiredFault))
	require.NoError(t, err)
	require.Equal(t, 2, sum.Schemas[0].Marked)
	require.Equal(t, int64(1), mustRecord(t, f.wiredFixture).Generation)
}

// TestWiredInitIsAbandonedByAFlushThatCommitsDuringIt: a flush that commits
// between the init's export and its publication makes the init's
// compare-and-set miss, and an init may not rebase over a flush. The init is
// abandoned with nothing visible; a rerun onboards the schema.
func TestWiredInitIsAbandonedByAFlushThatCommitsDuringIt(t *testing.T) {
	f := newWiredFixture(t)
	f.register(t, publication.Unpublished)
	base := time.Now().UnixMilli() - 60_000
	f.seed(t, 2, base)
	_, err := f.flush(t, nil, false)
	require.NoError(t, err)
	f.seed(t, 1, base+100)

	rival := func(ctx context.Context) error {
		cfg := f.cfg
		f.callerEpochs(t, &cfg)
		flushCtx, err := newSchemaFlushContext(ctx, flushContextParams{
			cfg: cfg, db: f.db, duck: f.duck, store: f.store, logger: f.logger, schemaRegistry: stubSchemaRegistry{cache: testAttrCache()},
		})
		if err != nil {
			return err
		}
		// The init holds the schema lock; the rival flush stands for a
		// writer whose lease expired, which the compare-and-set alone fences.
		flushCtx.locker = SchemaLockerFunc(func(ctx context.Context, _ int16) (context.Context, func(), bool, error) {
			return ctx, func() {}, true, nil
		})
		_, err = flushCtx.processSchemas(ctx, []int64{int64(wiredSchema)})
		return err
	}
	_, err = f.runInit(t, nil, false, runAt(string(cdcv2.PointStageManifest), rival))
	require.ErrorIs(t, err, cdcv2.ErrAbandoned)
	rec := mustRecord(t, f)
	require.Equal(t, int64(2), rec.Generation, "the rival flush's generation stays")
	require.Equal(t, publication.KindFlush, f.manifest(t, rec).Kind)

	_, err = f.runInit(t, nil, false)
	require.NoError(t, err)
	require.Equal(t, publication.KindInit, f.manifest(t, mustRecord(t, f)).Kind)
}

// TestWiredInitDryRunChangesNothing.
func TestWiredInitDryRunChangesNothing(t *testing.T) {
	f := newOnboardingFixture(t)
	rec, dirty := mustRecord(t, f.wiredFixture), f.dirty(t)

	sum, err := f.runInit(t, nil, true)
	require.NoError(t, err)
	require.Equal(t, int64(3), sum.TotalRowsExported, "a dry run counts what it would export")
	require.Equal(t, 1, sum.TotalFilesCreated)
	parquet, manifests := f.objects(t)
	require.Zero(t, parquet+manifests)
	require.Equal(t, rec, mustRecord(t, f.wiredFixture))
	require.Equal(t, dirty, f.dirty(t))
}

// lostCommits answers a PublishFailpoint whose every publication COMMIT goes
// without an answer and does not land, and whose first fenceFails fences
// fail: the writer cannot settle the outcome, and returns ErrUnresolved.
func lostCommits(fenceFails int32) func(*CDCConfig) {
	commit := failAt(string(cdcv2.PointCommit), 1<<30, cdcv2.ErrCommitAnswerLost)
	fence := failAt(string(cdcv2.PointFence), fenceFails, errWiredFault)
	return func(c *CDCConfig) {
		commit(c)
		onCommit := c.PublishFailpoint
		fence(c)
		onFence := c.PublishFailpoint
		c.PublishFailpoint = func(ctx context.Context, p string) error {
			if p == string(cdcv2.PointCommit) {
				return onCommit(ctx, p)
			}
			return onFence(ctx, p)
		}
	}
}

// TestWiredWritersSettleAnUnresolvedCommitOnce: when the writer cannot tell
// whether a COMMIT it sent landed, the tools settle it once in-process. A
// settling that proves the COMMIT can never land reports the publication
// abandoned; one that still cannot tell reports it unresolved, with what was
// left. Either way the record and change_log are as they were.
func TestWiredWritersSettleAnUnresolvedCommitOnce(t *testing.T) {
	t.Run("flush/abandoned", func(t *testing.T) {
		f := newWiredFixture(t)
		f.register(t, publication.Unpublished)
		f.seed(t, 2, time.Now().UnixMilli()-60_000)
		rec, dirty := mustRecord(t, f), f.dirty(t)
		_, err := f.flush(t, nil, false, lostCommits(1))
		require.ErrorIs(t, err, cdcv2.ErrAbandoned)
		require.ErrorContains(t, err, "stay dirty for the next flush")
		require.Equal(t, rec, mustRecord(t, f))
		require.Equal(t, dirty, f.dirty(t))
	})
	t.Run("flush/unresolved", func(t *testing.T) {
		f := newWiredFixture(t)
		f.register(t, publication.Unpublished)
		f.seed(t, 2, time.Now().UnixMilli()-60_000)
		dirty := f.dirty(t)
		_, err := f.flush(t, nil, false, lostCommits(1<<30))
		require.ErrorIs(t, err, cdcv2.ErrUnresolved)
		require.ErrorContains(t, err, "settling it in-process failed too")
		require.Equal(t, dirty, f.dirty(t))
	})
	t.Run("init/abandoned", func(t *testing.T) {
		f := newOnboardingFixture(t)
		rec, dirty := mustRecord(t, f.wiredFixture), f.dirty(t)
		_, err := f.runInit(t, nil, false, lostCommits(1))
		require.ErrorIs(t, err, cdcv2.ErrAbandoned)
		require.ErrorContains(t, err, "run cdc-init again")
		require.Equal(t, rec, mustRecord(t, f.wiredFixture))
		require.Equal(t, dirty, f.dirty(t))
	})
	t.Run("init/unresolved", func(t *testing.T) {
		f := newOnboardingFixture(t)
		rec, dirty := mustRecord(t, f.wiredFixture), f.dirty(t)
		_, err := f.runInit(t, nil, false, lostCommits(1<<30))
		require.ErrorIs(t, err, cdcv2.ErrUnresolved)
		require.ErrorContains(t, err, "2 exported entries were not handed off")
		require.Equal(t, rec, mustRecord(t, f.wiredFixture))
		require.Equal(t, dirty, f.dirty(t))
	})
}

// TestWiredInitFailsClosedOnAnEpochChange: a drop while the init stages
// moves the record to its next epoch; the init's compare-and-set misses and
// it is abandoned, so no generation of the old epoch is ever committed.
func TestWiredInitFailsClosedOnAnEpochChange(t *testing.T) {
	f := newOnboardingFixture(t)
	dirty := f.dirty(t)

	_, err := f.runInit(t, nil, false, runAt(string(cdcv2.PointStageManifest), f.drop))
	require.ErrorIs(t, err, cdcv2.ErrAbandoned)
	rec := mustRecord(t, f.wiredFixture)
	require.Equal(t, publication.Dropped, rec.State)
	require.Equal(t, int64(2), rec.Epoch)
	require.Equal(t, dirty, f.dirty(t))
}
