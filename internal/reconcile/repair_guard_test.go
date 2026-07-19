package reconcile

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/manifest"
)

// The repair guard (#203 review): a delta-shaped orphan is only appended
// when it still carries rows the manifest-listed inventory does not cover
// AND every such row is live in Postgres. Anything else is a compaction
// leftover (or ambiguous) — re-appending it could resurrect rows whose
// tombstones the full merge already dropped.

func guardReconciler(t *testing.T, lister *fakeLister, manifests *fakeManifests, stats *fakeStats, live *fakeLiveRows, deleter *fakeDeleter, opts Options) *Reconciler {
	t.Helper()
	r := newTestReconciler(lister, manifests, deleter, &fakeLocker{}, &fakeEnum{ids: []int16{7}})
	r.Stats = stats
	r.LiveRows = live
	r.Opts = opts
	return r
}

func oldObject(key string) ObjectInfo {
	return ObjectInfo{Key: key, Size: 10, LastModified: testClock().Add(-time.Hour)}
}

func TestRepair_FullyCoveredLeftoverNotAppended(t *testing.T) {
	leftover := "data/7/" + uuidA + ".parquet"
	mergedBase := "data/7/base-" + uuidB + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {oldObject(leftover), oldObject(mergedBase)},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "base", Path: mergedBase},
	}})
	stats := &fakeStats{uncovered: map[string][]string{leftover: {}}} // every row covered by the merged base
	r := guardReconciler(t, lister, manifests, stats, &fakeLiveRows{}, &fakeDeleter{}, Options{Repair: true, MaxETagRetries: 3})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, report.Schemas[0].Repaired, "covered leftover must not be re-appended")
	require.Empty(t, manifests.saves)
	require.Equal(t, []string{leftover}, report.Schemas[0].DeltaLeftovers)
	require.True(t, report.HasResidualDiscrepancies(), "leftover not yet deleted stays residual")
}

func TestRepair_TombstoneDroppedLeftoverNotAppended(t *testing.T) {
	leftover := "data/7/" + uuidA + ".parquet"
	mergedBase := "data/7/base-" + uuidB + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {oldObject(leftover), oldObject(mergedBase)},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "base", Path: mergedBase},
	}})
	// The orphan's uncovered row was deleted in Postgres: its tombstone won
	// the merge and was dropped — re-appending would resurrect it.
	stats := &fakeStats{uncovered: map[string][]string{leftover: {uuidA}}}
	live := &fakeLiveRows{missing: map[string]bool{uuidA: true}}
	r := guardReconciler(t, lister, manifests, stats, live, &fakeDeleter{}, Options{Repair: true, MaxETagRetries: 3})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, report.Schemas[0].Repaired)
	require.Empty(t, manifests.saves)
	require.Equal(t, []string{leftover}, report.Schemas[0].DeltaLeftovers)
}

func TestRepair_MixedLiveDeadRefusedEntirely(t *testing.T) {
	orphan := "data/7/" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{"data/7/": {oldObject(orphan)}}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	stats := &fakeStats{uncovered: map[string][]string{orphan: {uuidA, uuidB}}}
	live := &fakeLiveRows{missing: map[string]bool{uuidB: true}} // uuidA live, uuidB deleted
	deleter := &fakeDeleter{}
	r := guardReconciler(t, lister, manifests, stats, live, deleter,
		Options{Repair: true, GC: true, GCGrace: 15 * time.Minute, MaxETagRetries: 3})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Empty(t, s.Repaired, "mixed live/dead file needs manual surgery, not auto-append")
	require.Empty(t, s.DeltaLeftovers, "mixed file is not a provable leftover")
	require.Empty(t, deleter.deleted, "mixed file must never be deleted")
	require.Empty(t, manifests.saves)
	require.True(t, report.HasResidualDiscrepancies())
}

func TestRepair_GCDeletesClassifiedLeftoverPastGrace(t *testing.T) {
	leftover := "data/7/" + uuidA + ".parquet"
	mergedBase := "data/7/base-" + uuidB + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {oldObject(leftover), oldObject(mergedBase)},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "base", Path: mergedBase},
	}})
	stats := &fakeStats{uncovered: map[string][]string{leftover: {}}}
	deleter := &fakeDeleter{}
	r := guardReconciler(t, lister, manifests, stats, &fakeLiveRows{}, deleter,
		Options{Repair: true, GC: true, GCGrace: 15 * time.Minute, MaxETagRetries: 3})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{leftover}, deleter.deleted)
	require.Equal(t, []string{leftover}, report.Schemas[0].Deleted)
	require.False(t, report.HasResidualDiscrepancies(), "deleted leftover is resolved")
}

func TestRepair_UncoveredProbeErrorSkipsFile(t *testing.T) {
	orphan := "data/7/" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{"data/7/": {oldObject(orphan)}}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	stats := &fakeStats{
		uncoveredErr: map[string]error{orphan: context.DeadlineExceeded},
		stats:        map[string]compaction.MergeStats{orphan: {RowsOut: 1}},
	}
	r := guardReconciler(t, lister, manifests, stats, &fakeLiveRows{}, &fakeDeleter{}, Options{Repair: true, MaxETagRetries: 3})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, report.Schemas[0].Repaired)
	require.Empty(t, manifests.saves)
	require.True(t, report.HasResidualDiscrepancies())
}
