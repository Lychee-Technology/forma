package reconcile

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/manifest"
)

// The promotion save loop (#292): optimistic-concurrency retries, the
// manifest/etag handed on to the delta-repair pass, error escalation, and
// the GC interaction. Compaction does not take the schema advisory lock, so
// every save races it and every retry must re-prove the inventory-dependent
// guards.

func TestPromote_RetriesOnETagConflict(t *testing.T) {
	lister, stats, live, file1, file2 := completeInitSet()
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	manifests.saveErrs = []error{errTestConflict}
	// The writer that won the race listed an existing (pre-init) delta object
	// and moved the etag. The object predates the init export and is in this
	// run's listing, so the survivor fence clears it on ground 1 and the retry
	// is what the test actually exercises.
	concurrentDelta := "data/7/" + uuidB + ".parquet"
	lister.objects["data/7/"] = append(lister.objects["data/7/"],
		ObjectInfo{Key: concurrentDelta, Size: 1, LastModified: preInitClock()})
	manifests.onSaveConflict = func(f *fakeManifests) {
		f.manifests[7] = &manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
			{Tier: "delta", Path: concurrentDelta},
		}}
		f.etags[7] = "etag-7-concurrent"
	}
	r := promoteReconciler(t, lister, manifests, stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{file1, file2}, report.Schemas[0].PromotedBase)
	require.Len(t, manifests.saves, 1, "retry after 412 must land exactly one save")
	require.GreaterOrEqual(t, manifests.loads, 2, "conflict must trigger a manifest reload")
	// The retry saves against the RELOADED etag, and the concurrently added
	// delta survives the base-tier splice.
	require.Equal(t, "etag-7-concurrent", manifests.saves[0].etag)
	paths := make([]string, 0, len(manifests.manifests[7].Files))
	for _, f := range manifests.manifests[7].Files {
		paths = append(paths, f.Path)
	}
	require.ElementsMatch(t, []string{concurrentDelta, file1, file2}, paths)
}

func TestPromote_RefusesWhenConflictRevealsUnsafeEviction(t *testing.T) {
	// The eviction proof must be re-run per attempt: at attempt 0 the
	// manifest has no base entries, so a hoisted one-shot check would pass
	// and the retry would blindly replace the base tier the concurrent
	// compaction just committed.
	lister, stats, live, _, _ := completeInitSet()
	mergedKey := "data/7/base-" + uuidB + ".parquet"
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	manifests.saveErrs = []error{errTestConflict}
	manifests.onSaveConflict = func(f *fakeManifests) {
		f.manifests[7] = &manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
			{Tier: "base", Path: mergedKey},
		}}
		f.etags[7] = "etag-7-concurrent"
	}
	// Datable and strictly older than the init set, so the date fence clears
	// it and the version probe is what refuses.
	lister.objects["data/7/"] = append(lister.objects["data/7/"],
		ObjectInfo{Key: mergedKey, Size: 1, LastModified: preInitClock()})
	stats.uncoveredVs = map[string][]compaction.UncoveredRow{mergedKey: {{RowID: rid1}}}
	r := promoteReconciler(t, lister, manifests, stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Empty(t, s.PromotedBase)
	require.Contains(t, s.InitPromotionRefusal, "would lose")
	require.Empty(t, manifests.saves, "the conflicting attempt must not be retried into a save")
}

func TestPromote_RefusesUndatableEvictedBase(t *testing.T) {
	// The concurrent compaction committed its base entry after this run took
	// its listing, so the entry cannot be dated against the init export at
	// all. Undatable is refused, not probed: the eviction proof's
	// equal-timestamp coverage is only sound for entries provably written no
	// later than the promoted set.
	lister, stats, live, _, _ := completeInitSet()
	mergedKey := "data/7/base-" + uuidB + ".parquet" // deliberately absent from the listing
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	manifests.saveErrs = []error{errTestConflict}
	manifests.onSaveConflict = func(f *fakeManifests) {
		f.manifests[7] = &manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
			{Tier: "base", Path: mergedKey},
		}}
		f.etags[7] = "etag-7-concurrent"
	}
	stats.uncoveredVs = map[string][]compaction.UncoveredRow{mergedKey: {}} // coverage would pass
	r := promoteReconciler(t, lister, manifests, stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Empty(t, s.PromotedBase)
	require.Contains(t, s.InitPromotionRefusal, "absent from this run's object listing")
	require.Empty(t, manifests.saves)
}

func TestPromote_ThenDeltaRepairUsesFreshETag(t *testing.T) {
	// Promotion saves first and hands its post-save manifest+etag to the
	// delta-repair pass; repair must not save against the stale pre-promotion
	// etag.
	lister, stats, live, file1, file2 := completeInitSet()
	deltaOrphan := "data/7/" + uuidA + ".parquet"
	lister.objects["data/7/"] = append(lister.objects["data/7/"],
		ObjectInfo{Key: deltaOrphan, Size: 9, LastModified: testClock()})
	stats.stats[deltaOrphan] = compaction.MergeStats{RowsOut: 1, RowIDMin: uuidA, RowIDMax: uuidA}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	r := promoteReconciler(t, lister, manifests, stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.ElementsMatch(t, []string{file1, file2}, s.PromotedBase)
	require.Equal(t, []string{deltaOrphan}, s.Repaired)
	require.NoError(t, s.Err)

	require.Len(t, manifests.saves, 2, "promotion and repair each save once")
	require.Equal(t, "etag-7-1", manifests.saves[1].etag,
		"repair must save against the etag promotion's save returned")

	final := manifests.manifests[7]
	require.Len(t, final.Files, 3)
	var baseCount int
	repairedListed := false
	for _, f := range final.Files {
		if f.Tier == "base" {
			baseCount++
		}
		if f.Tier == "delta" && f.Path == deltaOrphan {
			repairedListed = true
		}
	}
	require.Equal(t, 2, baseCount)
	require.True(t, repairedListed)
}

func TestPromote_NonConflictSaveErrorFailsSchema(t *testing.T) {
	lister, stats, live, _, _ := completeInitSet()
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	manifests.saveErrs = []error{errors.New("s3 timeout")}
	r := promoteReconciler(t, lister, manifests, stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Error(t, s.Err)
	require.Contains(t, s.Err.Error(), "after init promotion")
	require.Empty(t, s.PromotedBase)
	require.Empty(t, manifests.saves, "an ambiguous save error must not retry blindly")
}

func TestPromote_ETagConflictExhaustsRetries(t *testing.T) {
	lister, stats, live, _, _ := completeInitSet()
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	manifests.saveErrs = []error{errTestConflict, errTestConflict, errTestConflict}
	r := promoteReconciler(t, lister, manifests, stats, live)
	r.Opts.MaxETagRetries = 1

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Error(t, s.Err)
	require.Contains(t, s.Err.Error(), "still conflicting")
	require.Empty(t, s.PromotedBase)
	require.True(t, report.HasResidualDiscrepancies())
}

func TestPromote_ReloadFailureAfterConflictFailsSchema(t *testing.T) {
	lister, stats, live, _, _ := completeInitSet()
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	manifests.saveErrs = []error{errTestConflict}
	manifests.onSaveConflict = func(f *fakeManifests) { f.loadErr = errors.New("s3 unavailable") }
	r := promoteReconciler(t, lister, manifests, stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Error(t, s.Err)
	require.Contains(t, s.Err.Error(), "reload")
	require.Empty(t, s.PromotedBase)
}

func TestPromote_PromotedSetNotGCed(t *testing.T) {
	lister, stats, live, file1, file2 := completeInitSet()
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	r := promoteReconciler(t, lister, manifests, stats, live)
	r.Opts.GC = true
	r.Opts.GCGrace = time.Minute
	// Both files sighted long ago and old enough: without promotion GC
	// would delete them this run (the #290 baseline).
	seedGCSighting(t, r, 7, testClock().Add(-2*time.Hour), file1, file2)
	lister.objects["data/7/"][0].LastModified = testClock().Add(-2 * time.Hour)
	lister.objects["data/7/"][1].LastModified = testClock().Add(-2 * time.Hour)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{file1, file2}, report.Schemas[0].PromotedBase)
	require.Empty(t, report.Schemas[0].Deleted, "promoted files must not be GC candidates in the same run")

	// The sighting entries must also be pruned: the files are listed
	// inventory again, so a later unlisting has to restart the grace clock.
	gcState := r.GCStates.(*fakeGCState)
	require.NotContains(t, gcState.state[7], file1)
	require.NotContains(t, gcState.state[7], file2)
}

func TestPromote_RefusedSetStaysGCEligible(t *testing.T) {
	lister, stats, live, file1, file2 := completeInitSet()
	live.liveCount = 4 // partial → refusal
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	r := promoteReconciler(t, lister, manifests, stats, live)
	r.Opts.GC = true
	r.Opts.GCGrace = time.Minute
	seedGCSighting(t, r, 7, testClock().Add(-2*time.Hour), file1, file2)
	lister.objects["data/7/"][0].LastModified = testClock().Add(-2 * time.Hour)
	lister.objects["data/7/"][1].LastModified = testClock().Add(-2 * time.Hour)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, report.Schemas[0].InitPromotionRefusal)
	require.ElementsMatch(t, []string{file1, file2}, report.Schemas[0].Deleted,
		"refused set keeps the #290 GC behavior")
}

func TestPromote_ErrorWhenStatsUnconfigured(t *testing.T) {
	lister, _, _, _, _ := completeInitSet()
	r := newTestReconciler(lister, newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}}),
		&fakeDeleter{}, &fakeLocker{}, &fakeEnum{ids: []int16{7}})
	r.Opts = Options{Repair: true, MaxETagRetries: 3} // Stats/LiveRows left nil

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Error(t, report.Schemas[0].Err)
	require.Contains(t, report.Schemas[0].Err.Error(), "not configured")
}
