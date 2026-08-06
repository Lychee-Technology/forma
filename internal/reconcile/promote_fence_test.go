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

// The two object-date fences an init-orphan promotion must clear (#292),
// mirroring the production split in promote_fence.go. Both exist because the
// version anti-join accepts an equal changed_at as superseding and cannot see
// values, while the read path leaves an equal-changed_at live/live cold tie
// with an unspecified winner (#274) — either copy may be served, so the
// fences are what keeps divergent-value ties from being published at all.
// checkEvictionDates covers the entries a promotion REMOVES;
// checkSurvivorDates covers the ones it LEAVES BEHIND.
//
// The inventory-dependent version and resurrection proofs live in
// promote_guard_test.go; shared fakes and fixtures in promote_test.go.

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

func TestPromote_RefusesUndatableSurvivor_Glob(t *testing.T) {
	// TIGHTENED in round 4. A glob delta entry cannot be resolved to a
	// probeable key, so the resurrection probe has always skipped it — but it
	// stays LISTED after the splice and therefore still participates in the
	// read path's equal-changed_at tie, whose live/live winner is
	// unspecified (#274) — either copy may be served. Previously
	// such an entry was silently tolerated and promotion went ahead; now an
	// entry this run cannot date at all refuses the whole promotion, exactly
	// like an unverifiable BASE entry.
	lister, stats, live, _, _ := completeInitSet()
	globPath := "data/7/*.parquet"
	staleKey := "data/7/base-" + uuidB + ".parquet"
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: globPath},
		{Tier: "base", Path: staleKey},
	}})
	lister.objects["data/7/"] = append(lister.objects["data/7/"],
		ObjectInfo{Key: staleKey, Size: 1, LastModified: preInitClock()})
	stats.uncoveredVs = map[string][]compaction.UncoveredRow{staleKey: {}}
	r := promoteReconciler(t, lister, manifests, stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Empty(t, s.PromotedBase)
	require.Contains(t, s.InitPromotionRefusal, "cannot be dated against the init export")
	require.Contains(t, s.InitPromotionRefusal, globPath)
	require.Empty(t, manifests.saves)

	// The survivor fence is pure arithmetic and runs first, so the eviction
	// version probe never runs.
	require.NotContains(t, stats.uncoveredKeys, staleKey)
}

// TestPromote_RefusesUndatableSurvivor_Unlisted covers the other undatable
// shape: a resolvable key the run's listing does not carry (a concurrent
// flusher committed the entry after this run took its listing).
func TestPromote_RefusesUndatableSurvivor_Unlisted(t *testing.T) {
	lister, stats, live, _, _ := completeInitSet()
	deltaKey := "data/7/" + uuidA + ".parquet" // deliberately absent from the listing
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: deltaKey, CreatedMin: 500, CreatedMax: 500},
	}})
	r := promoteReconciler(t, lister, manifests, stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Empty(t, s.PromotedBase, "an undatable survivor must block promotion")
	require.Contains(t, s.InitPromotionRefusal, "cannot be dated against the init export")
	require.Empty(t, manifests.saves)
}

// The survivor fence's two acceptance grounds and their boundary, driven
// directly through a post-init survivor object (ground 1 unavailable).
func TestPromote_SurvivorFenceVersionRange(t *testing.T) {
	deltaKey := "data/7/" + uuidA + ".parquet"
	// The init set's newest row version is CreatedMax = 300 (file2).
	cases := []struct {
		name       string
		createdMin int64
		promotes   bool
	}{
		{"strictly above the init set promotes", 301, true},
		{"equal to the init set refuses", 300, false},
		{"below the init set refuses", 200, false},
		{"unset created_min refuses", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lister, stats, live, file1, file2 := completeInitSet()
			manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
				{Tier: "delta", Path: deltaKey, CreatedMin: tc.createdMin, CreatedMax: 500},
			}})
			// Written AFTER the init export, so only the version range can
			// clear this survivor.
			lister.objects["data/7/"] = append(lister.objects["data/7/"],
				ObjectInfo{Key: deltaKey, Size: 1, LastModified: testClock().Add(time.Hour)})
			r := promoteReconciler(t, lister, manifests, stats, live)

			report, err := r.Run(context.Background())
			require.NoError(t, err)
			s := report.Schemas[0]
			if tc.promotes {
				require.ElementsMatch(t, []string{file1, file2}, s.PromotedBase)
				require.Empty(t, s.InitPromotionRefusal)
				return
			}
			require.Empty(t, s.PromotedBase)
			require.Contains(t, s.InitPromotionRefusal, "surviving non-base entry")
			require.Empty(t, manifests.saves)
		})
	}
}

// TestPromote_RefusesEqualSecondEvictedBase pins the round-4 tightening of the
// eviction fence from "no later than" to "strictly earlier than". S3
// LastModified is second-granular, so an evicted base entry stamped in the
// SAME second as the last init object could still fold a lock release, delta
// flush and compaction that all landed inside that second — the exact
// equal-changed_at regression the fence exists to stop.
func TestPromote_RefusesEqualSecondEvictedBase(t *testing.T) {
	lister, stats, live, _, _ := completeInitSet()
	mergedKey := "data/7/base-" + uuidB + ".parquet"
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "base", Path: mergedKey},
	}})
	lister.objects["data/7/"] = append(lister.objects["data/7/"],
		ObjectInfo{Key: mergedKey, Size: 1, LastModified: testClock()}) // == max init write
	stats.uncoveredVs = map[string][]compaction.UncoveredRow{mergedKey: {}} // coverage would pass
	r := promoteReconciler(t, lister, manifests, stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Empty(t, s.PromotedBase)
	require.Contains(t, s.InitPromotionRefusal, "postdates the init export")
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

// TestPromote_RealStats_RefusesSurvivingDeltaTie is the survivor fence's
// reason for existing, run through the production stats queries. There is NO
// listed base entry at all, so the eviction fence has nothing to check and
// coverage balances cleanly. The hazard lives entirely on the SURVIVING side:
// the failed init holds rid1='old'@100 and a listed delta — written after the
// failed init released the lock — holds rid1='new'@100. Promotion would
// publish 'old' as base, and the read path's equal-changed_at live/live tie
// has an unspecified winner (#274), so readers may serve 'old', flapping per
// scan with no probe able to see it. The
// delta's manifest CreatedMin ties the init set's max changed_at rather than
// exceeding it, so neither acceptance ground applies and the fence refuses.
func TestPromote_RealStats_RefusesSurvivingDeltaTie(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	dir := t.TempDir()
	initFile := initKey(rid1, rid1)
	deltaKey := "data/7/" + uuidA + ".parquet"
	writeValueFixture(t, db, filepath.Join(dir, filepath.Base(initFile)), rid1, 100, "old")
	writeValueFixture(t, db, filepath.Join(dir, filepath.Base(deltaKey)), rid1, 100, "new")

	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {
			{Key: initFile, Size: 11, LastModified: testClock()},
			{Key: deltaKey, Size: 22, LastModified: testClock().Add(time.Hour)},
		},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: deltaKey, CreatedMin: 100, CreatedMax: 100},
	}})
	r := promoteReconciler(t, lister, manifests, &localStatsReader{db: db, dir: dir},
		&fakeLiveRows{liveCount: 1})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Empty(t, s.PromotedBase)
	require.Contains(t, s.InitPromotionRefusal, "surviving non-base entry")
	require.Contains(t, s.InitPromotionRefusal, deltaKey)
	require.Empty(t, manifests.saves, "the manifest must be left untouched")
	require.Equal(t, []string{deltaKey}, manifestPaths(manifests, 7, "delta"),
		"the delta carrying the newer value must stay listed")
	require.Empty(t, manifestPaths(manifests, 7, "base"))
}
