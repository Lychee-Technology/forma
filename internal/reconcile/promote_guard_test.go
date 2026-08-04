package reconcile

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

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

// The eviction date fence. The version probe's coverage predicate is
// `changed_at >= changed_at`, so an equal-timestamp survivor counts as
// superseding — and the probe cannot see values. A base entry written AFTER
// the init export may fold a post-snapshot same-millisecond write, whose
// value differs from the init snapshot's; accepting the tie there regresses
// readers. Strict `>` cannot be the fix (unchanged rows tie across
// generations), so the entry is fenced by object date instead.

func TestPromote_RefusesEvictionOfPostInitBase(t *testing.T) {
	lister, stats, live, _, _ := completeInitSet()
	mergedKey := "data/7/base-" + uuidB + ".parquet"
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "base", Path: mergedKey},
	}})
	// Compaction folded a post-init-failure delta an hour after the init
	// objects were written.
	lister.objects["data/7/"] = append(lister.objects["data/7/"],
		ObjectInfo{Key: mergedKey, Size: 1, LastModified: testClock().Add(time.Hour)})
	// Version coverage would PASS: the >= anti-join reports nothing uncovered.
	stats.uncoveredVs = map[string][]compaction.UncoveredRow{mergedKey: {}}
	r := promoteReconciler(t, lister, manifests, stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Empty(t, s.PromotedBase)
	require.Contains(t, s.InitPromotionRefusal, "postdates the init export")
	require.Empty(t, manifests.saves)

	// The fence precedes the probe, so the entry is never even probed.
	for i, k := range stats.uncoveredKeys {
		if k == mergedKey {
			require.Empty(t, stats.uncoveredCalls[i],
				"date fence must refuse before the version probe runs")
		}
	}
}

// TestPromote_RefusesOlderGenerationAgainstNewerBase closes the generational
// ping-pong the docs previously left open: an init-shaped file evicted by a
// NEWER init generation stays an orphan and is re-examined by the next
// --repair run. The date fence makes that re-evaluation deterministically a
// refusal — an older generation can never evict a newer base — so the
// unlisted old files have one settled destiny: the GC candidate path.
func TestPromote_RefusesOlderGenerationAgainstNewerBase(t *testing.T) {
	lister, stats, live, _, _ := completeInitSet()
	newerKey := initKey(rid1, rid3) // the generation that actually got published
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "base", Path: newerKey},
	}})
	lister.objects["data/7/"] = append(lister.objects["data/7/"],
		ObjectInfo{Key: newerKey, Size: 1, LastModified: testClock().Add(time.Minute)})
	stats.uncoveredVs = map[string][]compaction.UncoveredRow{newerKey: {}}
	r := promoteReconciler(t, lister, manifests, stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Empty(t, s.PromotedBase, "an older generation must not evict a newer base")
	require.Contains(t, s.InitPromotionRefusal, "postdates the init export")
	require.Empty(t, manifests.saves)
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

// TestPromote_UnlistedTombstoneDeltaRefusesThenConverges pins the sequence
// raised as Finding-1 in this PR's review: the init publish failed AND the
// tombstone delta recording a later delete is itself unlisted. Coverage
// balances (dead rows do not count) and eviction safety has no base entry to
// check, so only the resurrection guard stands between promotion and a delete
// silently shadowed by a promoted base carrying the row's stale live version.
//
// The invariant: an unlisted newer tombstone can NEVER be shadowed by a
// promoted base. Run 1 refuses promotion — the probe may only mask against
// LISTED deltas — while the delta-repair pass in the SAME run restores the
// tombstone to the manifest (tombstone + dead row = lost delete). Run 2 can
// now mask rid3 against that listed tombstone, so the set converges and
// promotes with the delta entry preserved. Refusal is therefore transient and
// self-healing, never a permanent stall.
func TestPromote_UnlistedTombstoneDeltaRefusesThenConverges(t *testing.T) {
	lister, stats, live, file1, file2 := deletedRowInitSet()
	deltaKey := "data/7/" + uuidA + ".parquet"
	lister.objects["data/7/"] = append(lister.objects["data/7/"],
		ObjectInfo{Key: deltaKey, Size: 33, LastModified: testClock()})
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	// The unlisted delta carries rid3's newer tombstone. With an empty
	// manifest both the resurrection probe and the repair guard's probe run
	// with zero listed keys, which the fake answers from `uncovered` — the
	// bare-enumeration map — for every key.
	stats.stats[deltaKey] = compaction.MergeStats{
		RowsOut: 1, RowIDMin: rid3, RowIDMax: rid3, CreatedMin: 400, CreatedMax: 400,
	}
	stats.uncovered[deltaKey] = []compaction.UncoveredRow{{RowID: rid3, Tombstone: true}}
	r := promoteReconciler(t, lister, manifests, stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Contains(t, s.InitPromotionRefusal, "resurrect")
	require.Empty(t, s.PromotedBase, "an unlisted tombstone must block wholesale replacement")
	// Same run: the repair pass classifies the delta orphan as a lost delete
	// and restores it, which is exactly what unblocks the next run.
	require.Contains(t, s.Repaired, deltaKey)
	require.Equal(t, []string{deltaKey}, manifestPaths(manifests, 7, "delta"))
	require.Empty(t, manifestPaths(manifests, 7, "base"), "nothing may reach the base tier yet")

	// Run 2: the tombstone is listed, so the resurrection probe masks rid3 —
	// the real anti-join outcome once the surviving delta supersedes it.
	stats.uncoveredVs = map[string][]compaction.UncoveredRow{file2: {}}
	report, err = r.Run(context.Background())
	require.NoError(t, err)
	s = report.Schemas[0]
	require.Empty(t, s.InitPromotionRefusal)
	require.ElementsMatch(t, []string{file1, file2}, s.PromotedBase)
	require.ElementsMatch(t, []string{file1, file2}, manifestPaths(manifests, 7, "base"))
	require.Equal(t, []string{deltaKey}, manifestPaths(manifests, 7, "delta"),
		"the restored tombstone must survive the splice")
}

// manifestPaths returns the saved manifest's paths in one tier, in file order.
func manifestPaths(manifests *fakeManifests, schemaID int16, tier string) []string {
	var paths []string
	for _, f := range manifests.manifests[schemaID].Files {
		if f.Tier == tier {
			paths = append(paths, f.Path)
		}
	}
	return paths
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

// writeValueFixture writes a one-row live parquet with an explicit title, so
// two files can carry the SAME row id at the SAME changed_at with DIFFERENT
// values — the tie the version anti-join is blind to.
func writeValueFixture(t *testing.T, db *sql.DB, path, rowID string, changedAt int64, title string) {
	t.Helper()
	q := fmt.Sprintf(
		"COPY (SELECT CAST('%s' AS UUID) AS row_id, CAST(%d AS BIGINT) AS changed_at, CAST(0 AS BIGINT) AS deleted_at, '%s' AS title) TO '%s' (FORMAT PARQUET)",
		rowID, changedAt, title, path)
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("write value fixture: %v", err)
	}
}

// TestPromote_RealStats_RefusesEqualTimestampDifferentPayload is the fence's
// reason for existing, run through the production UncoveredRows query: the
// failed init holds rid1='old'@100, and a merged base written afterwards
// holds rid1='new'@100 — the post-init-failure write that landed in the same
// millisecond. The >= anti-join reports the base entry fully covered (equal
// changed_at supersedes, and payloads are invisible to it), so without the
// date fence promotion would evict 'new' and regress readers to 'old'.
func TestPromote_RealStats_RefusesEqualTimestampDifferentPayload(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	dir := t.TempDir()
	initFile := initKey(rid1, rid1)
	baseKey := "data/7/base-" + uuidB + ".parquet"
	writeValueFixture(t, db, filepath.Join(dir, filepath.Base(initFile)), rid1, 100, "old")
	writeValueFixture(t, db, filepath.Join(dir, filepath.Base(baseKey)), rid1, 100, "new")

	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {
			{Key: initFile, Size: 11, LastModified: testClock()},
			{Key: baseKey, Size: 22, LastModified: testClock().Add(time.Hour)},
		},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "base", Path: baseKey},
	}})
	r := promoteReconciler(t, lister, manifests, &localStatsReader{db: db, dir: dir},
		&fakeLiveRows{liveCount: 1})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Empty(t, s.PromotedBase)
	require.Contains(t, s.InitPromotionRefusal, "postdates the init export")
	require.Empty(t, manifests.saves, "the manifest must be left untouched")
	require.Equal(t, []string{baseKey}, manifestPaths(manifests, 7, "base"),
		"the newer base entry must stay listed")
}
