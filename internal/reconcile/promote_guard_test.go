package reconcile

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/manifest"
)

// The two inventory-dependent proofs a proven-complete init set must still
// pass before it may replace the base tier (#292):
//
//   - eviction safety: no listed base entry carries a row version the
//     promoted set (or a surviving delta) fails to supersede.
//   - no resurrection: no init file carries a stale LIVE version of a row
//     deleted since the export that no surviving delta supersedes. Coverage
//     counting alone cannot see this — dead rows simply do not count — so
//     without this guard promotion silently undoes deletes whose tombstone
//     delta was already compacted away.
//
// Both probe against the CURRENT manifest, so both re-run per save attempt
// (see TestPromote_RefusesWhenConflictRevealsUnsafeEviction).

func TestPromote_RefusesWhenEvictionWouldLoseVersions(t *testing.T) {
	lister, stats, live, file1, file2 := completeInitSet()
	// A listed merged-base entry (compaction ran after the failed init)
	// carries a version the promoted set does not supersede.
	mergedKey := "data/7/base-" + uuidB + ".parquet"
	deltaKey := "data/7/" + uuidA + ".parquet"
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "base", Path: mergedKey},
		{Tier: "delta", Path: deltaKey},
	}})
	lister.objects["data/7/"] = append(lister.objects["data/7/"],
		ObjectInfo{Key: mergedKey, Size: 1, LastModified: testClock()},
		ObjectInfo{Key: deltaKey, Size: 1, LastModified: testClock()})
	stats.uncoveredVs = map[string][]compaction.UncoveredRow{
		mergedKey: {{RowID: rid1}}, // newer version only here
	}
	r := promoteReconciler(t, lister, manifests, stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Empty(t, s.PromotedBase)
	require.Contains(t, s.InitPromotionRefusal, "would lose")
	require.Empty(t, manifests.saves)

	// The survivor set the probe masks against must be exactly what outlives
	// the splice: the promoted set plus surviving deltas, never the entry
	// being evicted.
	probe := stats.maskedProbe(t, mergedKey)
	require.Subset(t, probe, []string{file1, file2, deltaKey})
	require.NotContains(t, probe, mergedKey)
}

func TestPromote_EvictsCoveredStaleBase(t *testing.T) {
	lister, stats, live, file1, file2 := completeInitSet()
	staleKey := "data/7/base-" + uuidB + ".parquet"
	deltaKey := "data/7/" + uuidA + ".parquet"
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "base", Path: staleKey},
		{Tier: "delta", Path: deltaKey},
	}})
	lister.objects["data/7/"] = append(lister.objects["data/7/"],
		ObjectInfo{Key: staleKey, Size: 1, LastModified: testClock()},
		ObjectInfo{Key: deltaKey, Size: 1, LastModified: testClock()})
	stats.uncoveredVs = map[string][]compaction.UncoveredRow{
		staleKey: {}, // fully superseded by the promoted set
	}
	r := promoteReconciler(t, lister, manifests, stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{file1, file2}, report.Schemas[0].PromotedBase)
	for _, f := range manifests.manifests[7].Files {
		require.NotEqual(t, staleKey, f.Path, "stale base entry must be evicted")
	}

	probe := stats.maskedProbe(t, staleKey)
	require.Subset(t, probe, []string{file1, file2, deltaKey})
	require.NotContains(t, probe, staleKey)
}

func TestPromote_RefusesUnverifiableBaseEntry(t *testing.T) {
	// A base entry in another bucket cannot be probed from this listing, so
	// wholesale replacement cannot be proven safe.
	lister, stats, live, _, _ := completeInitSet()
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "base", Path: "s3://other-bucket/data/7/base-" + uuidB + ".parquet"},
	}})
	r := promoteReconciler(t, lister, manifests, stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Empty(t, s.PromotedBase)
	require.Contains(t, s.InitPromotionRefusal, "cannot be verified")
	require.Empty(t, manifests.saves)
}

func TestPromote_GlobDeltaEntryIsNotASurvivor(t *testing.T) {
	// A glob delta entry cannot be resolved to a probeable key, so it must
	// never be counted as masking coverage — but it also must not block
	// promotion the way an unverifiable BASE entry does.
	lister, stats, live, file1, file2 := completeInitSet()
	globPath := "data/7/*.parquet"
	staleKey := "data/7/base-" + uuidB + ".parquet"
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: globPath},
		{Tier: "base", Path: staleKey},
	}})
	lister.objects["data/7/"] = append(lister.objects["data/7/"],
		ObjectInfo{Key: staleKey, Size: 1, LastModified: testClock()})
	stats.uncoveredVs = map[string][]compaction.UncoveredRow{staleKey: {}}
	r := promoteReconciler(t, lister, manifests, stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{file1, file2}, report.Schemas[0].PromotedBase)

	probe := stats.maskedProbe(t, staleKey)
	require.NotContains(t, probe, globPath, "unresolvable glob must not be treated as coverage")
	require.Subset(t, probe, []string{file1, file2})
}

// deletedRowInitSet returns the completeInitSet fakes adjusted so rid3 —
// file2's only row — was deleted in Postgres AFTER the init export. Coverage
// still balances (2 live rows, both in file1), so only the resurrection
// guard stands between promotion and an undone delete.
func deletedRowInitSet() (*fakeLister, *fakeStats, *fakeLiveRows, string, string) {
	lister, stats, live, file1, file2 := completeInitSet()
	live.missing = map[string]bool{rid3: true}
	live.liveCount = 2
	return lister, stats, live, file1, file2
}

func TestPromote_RefusesResurrectionOfDeletedRow(t *testing.T) {
	lister, stats, live, _, file2 := deletedRowInitSet()
	deltaKey := "data/7/" + uuidA + ".parquet"
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: deltaKey},
	}})
	lister.objects["data/7/"] = append(lister.objects["data/7/"],
		ObjectInfo{Key: deltaKey, Size: 1, LastModified: testClock()})
	// No surviving delta supersedes rid3's stale live version: its tombstone
	// was already merged away.
	stats.uncoveredVs = map[string][]compaction.UncoveredRow{file2: {{RowID: rid3}}}
	r := promoteReconciler(t, lister, manifests, stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Empty(t, s.PromotedBase, "promotion must not undo a delete")
	require.Contains(t, s.InitPromotionRefusal, "resurrect")
	require.Contains(t, s.InitPromotionRefusal, rid3)
	require.Empty(t, manifests.saves)
}

func TestPromote_TombstoneDeltaMasksDeletedRow(t *testing.T) {
	lister, stats, live, file1, file2 := deletedRowInitSet()
	deltaKey := "data/7/" + uuidA + ".parquet"
	staleKey := "data/7/base-" + uuidB + ".parquet"
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: deltaKey},
		{Tier: "base", Path: staleKey},
	}})
	lister.objects["data/7/"] = append(lister.objects["data/7/"],
		ObjectInfo{Key: deltaKey, Size: 1, LastModified: testClock()},
		ObjectInfo{Key: staleKey, Size: 1, LastModified: testClock()})
	stats.uncoveredVs = map[string][]compaction.UncoveredRow{
		file2:    {}, // the surviving delta's tombstone supersedes rid3
		staleKey: {}, // and the evicted base is fully superseded
	}
	r := promoteReconciler(t, lister, manifests, stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{file1, file2}, report.Schemas[0].PromotedBase)

	// The resurrection probe must mask ONLY against surviving deltas:
	// including the init set itself would make the check vacuous, and the
	// evicted base cannot mask anything after the splice.
	require.Equal(t, []string{deltaKey}, stats.maskedProbe(t, file2))
}

// Real-DuckDB verification of the resurrection guard: a genuine tombstone
// delta (deleted_at set, newer changed_at) read through the production
// UncoveredRows query.
func TestPromote_RealStats_TombstoneDeltaMasksDeletedRow(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	dir := t.TempDir()
	file1 := initKey(rid1, rid2)
	file2 := initKey(rid3, rid3)
	deltaKey := "data/7/" + uuidA + ".parquet"
	writeInitFixture(t, db, filepath.Join(dir, filepath.Base(file1)),
		initFixtureRow{rid1, 100}, initFixtureRow{rid2, 200})
	writeInitFixture(t, db, filepath.Join(dir, filepath.Base(file2)),
		initFixtureRow{rid3, 300})
	writeTombstoneFixture(t, db, filepath.Join(dir, filepath.Base(deltaKey)), rid3, 400)

	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {
			{Key: file1, Size: 11, LastModified: testClock()},
			{Key: file2, Size: 22, LastModified: testClock()},
			{Key: deltaKey, Size: 33, LastModified: testClock()},
		},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: deltaKey},
	}})
	r := promoteReconciler(t, lister, manifests, &localStatsReader{db: db, dir: dir},
		&fakeLiveRows{liveCount: 2, missing: map[string]bool{rid3: true}})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{file1, file2}, report.Schemas[0].PromotedBase)
	require.Empty(t, report.Schemas[0].InitPromotionRefusal)
	require.Len(t, manifests.manifests[7].Files, 3, "the tombstone delta must survive the splice")
}

func TestPromote_RealStats_RefusesResurrectionWithoutTombstoneDelta(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	dir := t.TempDir()
	file1 := initKey(rid1, rid2)
	file2 := initKey(rid3, rid3)
	writeInitFixture(t, db, filepath.Join(dir, filepath.Base(file1)),
		initFixtureRow{rid1, 100}, initFixtureRow{rid2, 200})
	writeInitFixture(t, db, filepath.Join(dir, filepath.Base(file2)),
		initFixtureRow{rid3, 300})

	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {
			{Key: file1, Size: 11, LastModified: testClock()},
			{Key: file2, Size: 22, LastModified: testClock()},
		},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	// rid3 deleted in Postgres and its tombstone already compacted away:
	// nothing survives to mask file2's stale live version.
	r := promoteReconciler(t, lister, manifests, &localStatsReader{db: db, dir: dir},
		&fakeLiveRows{liveCount: 2, missing: map[string]bool{rid3: true}})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, report.Schemas[0].PromotedBase)
	require.Contains(t, report.Schemas[0].InitPromotionRefusal, "resurrect")
	require.Contains(t, report.Schemas[0].InitPromotionRefusal, rid3)
	require.Empty(t, manifests.saves)
}
