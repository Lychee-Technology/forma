package compaction

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/telemetry"
)

// TestCompactorPromotesTheDeltaTierIntoOneBaseObject: a delta tier at the
// target size is merged into one new base object, written once under a
// fresh key, and published as one compact generation from the generation
// the pass read. The merge keeps each row's newest version, tombstones
// included, and the replaced generation and every object it listed stay
// readable.
func TestCompactorPromotesTheDeltaTierIntoOneBaseObject(t *testing.T) {
	f := newV2Fixture(t)
	f.publish(t, publication.KindFlush, f.object(t, manifestv2.TierDelta, []mergeFixtureRow{{rowA, 100, "0", "a-v1"}, {rowB, 100, "0", "b-v1"}}))
	input := f.publish(t, publication.KindFlush, f.object(t, manifestv2.TierDelta, []mergeFixtureRow{{rowA, 200, "0", "a-v2"}, {rowB, 300, "300", "NULL"}}))
	replaced := f.manifest(t, input)

	sink, rec := recordingSink()
	result, err := f.compactor(t, cdc.CompactionConfig{TargetBaseSizeBytes: 1}, func(c *Compactor) { c.Metrics = sink }).RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, PromotionApplied, result.Outcome)
	require.Equal(t, input.Generation, result.InputGeneration)
	require.Equal(t, input.Generation+1, result.Generation)
	require.Equal(t, 2, result.FilesMerged)
	require.Equal(t, int64(4), result.RowsIn)
	require.Equal(t, int64(2), result.RowsOut)

	head := f.record(t)
	require.Equal(t, input.Generation, head.ParentGeneration)
	require.Equal(t, input.Lineage, head.Lineage)
	m := f.manifest(t, head)
	require.Equal(t, publication.KindCompact, m.Kind)
	require.Equal(t, input.Generation, m.InputGeneration)
	require.Equal(t, []string{result.NewBaseKey}, m.Keys())
	require.Equal(t, manifestv2.TierBase, m.Files[0].Tier)
	require.Equal(t, map[string][2]int64{rowA: {200, 0}, rowB: {300, 300}}, f.rowsOf(t, result.NewBaseKey),
		"row A's newest version and row B's tombstone")
	require.Equal(t, "flush,flush,compact", f.kinds(t))

	again := f.manifest(t, input)
	require.Equal(t, replaced, again, "the replaced generation's manifest is unchanged")
	for _, file := range again.Files {
		_, err := os.Stat(f.path(file.Key))
		require.NoError(t, err, "the replaced generation's %s is still there", file.Key)
	}
	names := map[string]int{}
	for _, e := range rec.events {
		names[e.Name]++
	}
	require.Equal(t, 1, names["compaction_dirty_ratio"])
	require.Zero(t, names["compaction_rewrite_applied_total"], "a promotion is not a dirty-ratio rewrite")
}

// TestCompactorRewritesADirtyBaseWithItsDeltas: past the dirty ratio, every
// base and delta file is merged into one base object.
func TestCompactorRewritesADirtyBaseWithItsDeltas(t *testing.T) {
	f := newV2Fixture(t)
	f.publish(t, publication.KindInit, f.object(t, manifestv2.TierBase, []mergeFixtureRow{{rowA, 100, "0", "a-v1"}, {rowC, 100, "0", "c-v1"}}))
	f.publish(t, publication.KindFlush, f.object(t, manifestv2.TierDelta, []mergeFixtureRow{{rowC, 200, "200", "NULL"}}))

	sink, rec := recordingSink()
	result, err := f.compactor(t, cdc.CompactionConfig{}, func(c *Compactor) { c.Metrics = sink }).RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, RewriteApplied, result.Outcome)
	require.Equal(t, 1, countMetric(rec, "compaction_rewrite_applied_total"))
	require.Equal(t, 2, result.FilesMerged)
	require.InDelta(t, 0.5, result.DirtyRatio, 1e-9)
	m := f.manifest(t, f.record(t))
	require.Equal(t, []string{result.NewBaseKey}, m.Keys())
	require.Equal(t, map[string][2]int64{rowA: {100, 0}, rowC: {200, 200}}, f.rowsOf(t, result.NewBaseKey))
	require.Equal(t, "init,flush,compact", f.kinds(t))
}

// TestCompactorPublishesNothingItHasNoReasonOrMeansToMerge: an unpublished
// schema, a generation below both thresholds, and a merge called for with
// no merger leave the record as it was.
func TestCompactorPublishesNothingItHasNoReasonOrMeansToMerge(t *testing.T) {
	f := newV2Fixture(t)
	result, err := f.compactor(t, cdc.CompactionConfig{}).RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, Noop, result.Outcome, "an unpublished schema has nothing to compact")

	f.publish(t, publication.KindFlush, f.object(t, manifestv2.TierDelta, []mergeFixtureRow{{rowA, 100, "0", "a-v1"}}))
	head := f.record(t)
	result, err = f.compactor(t, cdc.CompactionConfig{}).RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, Noop, result.Outcome)
	require.Equal(t, head.Generation, result.InputGeneration)
	require.Zero(t, result.Generation)
	require.Equal(t, head, f.record(t))

	result, err = f.compactor(t, cdc.CompactionConfig{TargetBaseSizeBytes: 1}, func(c *Compactor) { c.Merger = nil }).RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, RewritePending, result.Outcome)
	require.Equal(t, head, f.record(t))
}

// TestCompactorCountsOnlyRewritesAsPending pins R1-F7 of #907:
// compaction_rewrite_pending_total keeps its catalogued meaning, a dirty-ratio
// rewrite called for with no merge wiring. A promotion called for without
// one is reported as RewritePending but not counted.
func TestCompactorCountsOnlyRewritesAsPending(t *testing.T) {
	f := newV2Fixture(t)
	f.publish(t, publication.KindInit, f.object(t, manifestv2.TierBase, []mergeFixtureRow{{rowA, 100, "0", "a-v1"}, {rowC, 100, "0", "c-v1"}}))
	f.publish(t, publication.KindFlush, f.object(t, manifestv2.TierDelta, []mergeFixtureRow{{rowC, 200, "200", "NULL"}}))
	noMerger := func(sink *telemetry.Sink) func(*Compactor) {
		return func(c *Compactor) { c.Merger, c.Metrics = nil, sink }
	}

	sink, rec := recordingSink()
	result, err := f.compactor(t, cdc.CompactionConfig{TargetBaseSizeBytes: 1}, noMerger(sink)).RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, RewritePending, result.Outcome)
	require.Zero(t, countMetric(rec, "compaction_rewrite_pending_total"), "a promotion is not a pending rewrite")

	sink, rec = recordingSink()
	result, err = f.compactor(t, cdc.CompactionConfig{}, noMerger(sink)).RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, RewritePending, result.Outcome)
	require.Equal(t, 1, countMetric(rec, "compaction_rewrite_pending_total"))
	require.Zero(t, countMetric(rec, "compaction_rewrite_applied_total"))
}

func countMetric(rec *metricRecorder, name string) int {
	n := 0
	for _, e := range rec.events {
		if e.Name == name {
			n++
		}
	}
	return n
}

// TestCompactorRefusesASourceWhoseBytesChanged: the #347 gate runs before
// the merge, so a corrupt source publishes nothing; the opt-out merges it.
func TestCompactorRefusesASourceWhoseBytesChanged(t *testing.T) {
	f := newV2Fixture(t)
	good := f.object(t, manifestv2.TierDelta, []mergeFixtureRow{{rowA, 100, "0", "a-v1"}})
	f.publish(t, publication.KindFlush, good)
	other := f.object(t, manifestv2.TierDelta, []mergeFixtureRow{{rowA, 200, "0", "a-v2"}})
	raw, err := os.ReadFile(f.path(other.Key))
	require.NoError(t, err)
	require.NoError(t, os.Chmod(f.path(good.Key), 0o644))
	require.NoError(t, os.WriteFile(f.path(good.Key), raw, 0o644))
	head := f.record(t)

	_, err = f.compactor(t, cdc.CompactionConfig{TargetBaseSizeBytes: 1}).RunOnce(context.Background())
	require.ErrorIs(t, err, ErrSourceChecksumMismatch)
	require.Equal(t, head, f.record(t))

	result, err := f.compactor(t, cdc.CompactionConfig{TargetBaseSizeBytes: 1, SkipInputChecksumVerify: true}).RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, PromotionApplied, result.Outcome)
}

// TestCompactorRefusesWhatTheProtocolForbids: a pass of an AWAITING_INIT
// schema or of a schema at another epoch than it pinned publishes nothing.
func TestCompactorRefusesWhatTheProtocolForbids(t *testing.T) {
	f := newV2Fixture(t)
	f.publish(t, publication.KindFlush, f.object(t, manifestv2.TierDelta, []mergeFixtureRow{{rowA, 100, "0", "a-v1"}}))
	head := f.record(t)
	_, err := f.compactor(t, cdc.CompactionConfig{TargetBaseSizeBytes: 1}, func(c *Compactor) { c.Epoch = head.Epoch + 1 }).RunOnce(context.Background())
	require.Error(t, err)
	require.Equal(t, head, f.record(t))

	_, err = (&Compactor{Config: cdc.CompactionConfig{SchemaID: fixtureSchema}}).RunOnce(context.Background())
	require.ErrorContains(t, err, "writer")
}

// emptyMerger is a Merger whose merge writes no row, whatever it reads: the
// outcome R1-F1 of #907 produced when a payload column captured the rank.
type emptyMerger struct{ rowsIn int64 }

func (m emptyMerger) MergeToTmp(context.Context, []string, string) (MergeStats, error) {
	return MergeStats{RowsIn: m.rowsIn}, nil
}

// TestCompactorRefusesAnEmptyMergeOfListedRows: sources whose entries declare
// rows cannot merge to nothing, so a merge that wrote none is refused rather
// than published as the removal of every source with no replacement.
func TestCompactorRefusesAnEmptyMergeOfListedRows(t *testing.T) {
	f := newV2Fixture(t)
	f.publish(t, publication.KindFlush, f.object(t, manifestv2.TierDelta, []mergeFixtureRow{{rowA, 100, "0", "a-v1"}, {rowB, 100, "0", "b-v1"}}))
	head := f.record(t)
	for _, rowsIn := range []int64{0, 2} {
		result, err := f.compactor(t, cdc.CompactionConfig{TargetBaseSizeBytes: 1}, func(c *Compactor) { c.Merger = emptyMerger{rowsIn: rowsIn} }).RunOnce(context.Background())
		require.ErrorContains(t, err, "wrote none", "rows in %d", rowsIn)
		require.Zero(t, result.Generation)
		require.Equal(t, head, f.record(t), "the record is left as it was")
	}
}
