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
	stats := &fakeStats{uncovered: map[string][]compaction.UncoveredRow{leftover: {}}} // every version superseded by the merged base
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
	// The orphan's uncovered LIVE version belongs to a Postgres-deleted
	// row: its tombstone won the merge and was dropped — re-appending
	// would resurrect it.
	stats := &fakeStats{uncovered: map[string][]compaction.UncoveredRow{leftover: {{RowID: uuidA}}}}
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
	stats := &fakeStats{uncovered: map[string][]compaction.UncoveredRow{orphan: {{RowID: uuidA}, {RowID: uuidB}}}}
	live := &fakeLiveRows{missing: map[string]bool{uuidB: true}} // uuidA live (append-worthy), uuidB deleted (resurrect risk)
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
	stats := &fakeStats{uncovered: map[string][]compaction.UncoveredRow{leftover: {}}}
	deleter := &fakeDeleter{}
	r := guardReconciler(t, lister, manifests, stats, &fakeLiveRows{}, deleter,
		Options{Repair: true, GC: true, GCGrace: 15 * time.Minute, MaxETagRetries: 3})
	seedGCSighting(t, r, 7, testClock().Add(-16*time.Minute), leftover)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{leftover}, deleter.deleted)
	require.Equal(t, []string{leftover}, report.Schemas[0].Deleted)
	require.False(t, report.HasResidualDiscrepancies(), "deleted leftover is resolved")
}

func TestRepair_LostUpdateAppendedNotLeftover(t *testing.T) {
	// P0 regression (PR #289 review finding 1): an orphan carrying a NEWER
	// version of a row the listed files also contain is the #197 lost
	// update — a row_id-only coverage check would call it a leftover and
	// --repair --gc would delete the only copy of the newest version.
	orphan := "data/7/" + uuidA + ".parquet"
	mergedBase := "data/7/base-" + uuidB + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {oldObject(orphan), oldObject(mergedBase)},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "base", Path: mergedBase},
	}})
	// Version-aware probe reports the updated row as uncovered even though
	// the base contains an older version of the same row_id.
	stats := &fakeStats{
		uncovered: map[string][]compaction.UncoveredRow{orphan: {{RowID: uuidA}}},
		stats:     map[string]compaction.MergeStats{orphan: {RowsOut: 1, RowIDMin: uuidA, RowIDMax: uuidA}},
	}
	deleter := &fakeDeleter{}
	r := guardReconciler(t, lister, manifests, stats, &fakeLiveRows{}, deleter,
		Options{Repair: true, GC: true, GCGrace: 15 * time.Minute, MaxETagRetries: 3})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{orphan}, report.Schemas[0].Repaired)
	require.Empty(t, report.Schemas[0].DeltaLeftovers)
	require.Empty(t, deleter.deleted, "a lost-update orphan must never be GCed")
}

func TestRepair_LostTombstoneAppendedToRestoreDelete(t *testing.T) {
	// An orphan whose uncovered newest version is a TOMBSTONE of a
	// Postgres-deleted row is a lost delete: while unlisted, older listed
	// versions of the row resurrect in reads. Appending restores the
	// delete — classifying it as a leftover and GCing it would bake the
	// resurrection in permanently.
	orphan := "data/7/" + uuidA + ".parquet"
	mergedBase := "data/7/base-" + uuidB + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {oldObject(orphan), oldObject(mergedBase)},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "base", Path: mergedBase},
	}})
	stats := &fakeStats{
		uncovered: map[string][]compaction.UncoveredRow{orphan: {{RowID: uuidA, Tombstone: true}}},
		stats:     map[string]compaction.MergeStats{orphan: {RowsOut: 1, RowIDMin: uuidA, RowIDMax: uuidA}},
	}
	live := &fakeLiveRows{missing: map[string]bool{uuidA: true}} // deleted in Postgres, consistent with the tombstone
	deleter := &fakeDeleter{}
	r := guardReconciler(t, lister, manifests, stats, live, deleter,
		Options{Repair: true, GC: true, GCGrace: 15 * time.Minute, MaxETagRetries: 3})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{orphan}, report.Schemas[0].Repaired)
	require.Empty(t, deleter.deleted)
}

func TestRepair_TombstoneOfLivePGRowRefused(t *testing.T) {
	// A tombstone for a row Postgres still considers live contradicts the
	// entity state — never auto-handled in either direction.
	orphan := "data/7/" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{"data/7/": {oldObject(orphan)}}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	stats := &fakeStats{
		uncovered: map[string][]compaction.UncoveredRow{orphan: {{RowID: uuidA, Tombstone: true}}},
	}
	deleter := &fakeDeleter{}
	r := guardReconciler(t, lister, manifests, stats, &fakeLiveRows{}, deleter,
		Options{Repair: true, GC: true, GCGrace: 15 * time.Minute, MaxETagRetries: 3})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Empty(t, s.Repaired)
	require.Empty(t, s.DeltaLeftovers)
	require.Empty(t, deleter.deleted)
	require.True(t, report.HasResidualDiscrepancies())
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
