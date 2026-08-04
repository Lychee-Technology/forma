package reconcile

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/manifest"
)

// Coverage proof (#292): an init-shaped orphan set may replace the base tier
// only once its parquet contents are readable, self-consistent, disjoint,
// tombstone-free, and provably cover every live entity_main row. The
// eviction and resurrection guards live in promote_guard_test.go; the save
// loop, error escalation, and GC interaction in promote_save_test.go.

// Fixed UUIDv7-shaped ids whose lexicographic order is their byte order.
const (
	rid1 = "00000000-0000-7000-8000-000000000001"
	rid2 = "00000000-0000-7000-8000-000000000002"
	rid3 = "00000000-0000-7000-8000-000000000003"
	rid4 = "00000000-0000-7000-8000-000000000004"
)

func initKey(minID, maxID string) string { return "data/7/" + minID + "_" + maxID + ".parquet" }

// preInitClock is the listing time an object must carry to clear the survivor
// fence on ground 1 (checkSurvivorDates): a listed non-base entry may only
// survive the splice when its object is written STRICTLY EARLIER than every
// promoted init object, or when its version range starts strictly above the
// promoted set's. The same strictness now applies to evicted base entries
// (checkEvictionDates), so both sides of a promotion fixture predate the init
// set unless the test is exercising a refusal.
func preInitClock() time.Time { return testClock().Add(-time.Hour) }

// promoteReconciler wires a repair-mode reconciler whose fakes describe a
// two-file init orphan set covering live rows rid1..rid3.
func promoteReconciler(t *testing.T, lister *fakeLister, manifests *fakeManifests, stats StatsReader, live *fakeLiveRows) *Reconciler {
	t.Helper()
	r := newTestReconciler(lister, manifests, &fakeDeleter{}, &fakeLocker{}, &fakeEnum{ids: []int16{7}})
	r.Stats = stats
	r.LiveRows = live
	r.Opts = Options{Repair: true, MaxETagRetries: 3}
	return r
}

func completeInitSet() (*fakeLister, *fakeStats, *fakeLiveRows, string, string) {
	file1 := initKey(rid1, rid2)
	file2 := initKey(rid3, rid3)
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {
			{Key: file1, Size: 111, LastModified: testClock()},
			{Key: file2, Size: 222, LastModified: testClock()},
		},
	}}
	stats := &fakeStats{
		stats: map[string]compaction.MergeStats{
			file1: {RowsOut: 2, RowIDMin: rid1, RowIDMax: rid2, CreatedMin: 100, CreatedMax: 200},
			file2: {RowsOut: 1, RowIDMin: rid3, RowIDMax: rid3, CreatedMin: 300, CreatedMax: 300},
		},
		uncovered: map[string][]compaction.UncoveredRow{
			file1: {{RowID: rid1}, {RowID: rid2}},
			file2: {{RowID: rid3}},
		},
		columns: map[string]map[string]string{
			file1: {"row_id": "UUID", "changed_at": "BIGINT"},
			file2: {"row_id": "UUID", "changed_at": "BIGINT"},
		},
	}
	live := &fakeLiveRows{liveCount: 3}
	return lister, stats, live, file1, file2
}

func TestPromote_PromotesCompleteInitSet(t *testing.T) {
	lister, stats, live, file1, file2 := completeInitSet()
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: "data/7/" + uuidA + ".parquet"},
	}})
	// The pre-existing delta entry must survive the splice; it is listed, so
	// it is not an orphan, and eviction safety has no base entries to check.
	// It predates the init export, so the survivor fence clears it on ground 1.
	lister.objects["data/7/"] = append(lister.objects["data/7/"],
		ObjectInfo{Key: "data/7/" + uuidA + ".parquet", Size: 1, LastModified: preInitClock()})
	r := promoteReconciler(t, lister, manifests, stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)

	s := report.Schemas[0]
	require.ElementsMatch(t, []string{file1, file2}, s.PromotedBase)
	require.Empty(t, s.InitPromotionRefusal)
	require.False(t, report.HasResidualDiscrepancies())

	require.Len(t, manifests.saves, 1)
	saved := manifests.saves[0].m
	var base []manifest.FileEntry
	deltaSurvived := false
	for _, f := range saved.Files {
		if f.Tier == "base" {
			base = append(base, f)
		}
		if f.Tier == "delta" {
			deltaSurvived = true
		}
	}
	require.True(t, deltaSurvived)
	require.Len(t, base, 2)
	byPath := map[string]manifest.FileEntry{base[0].Path: base[0], base[1].Path: base[1]}
	e1 := byPath[file1]
	require.Equal(t, rid1, e1.RowIDMin)
	require.Equal(t, rid2, e1.RowIDMax)
	require.Equal(t, int64(2), e1.RowCount)
	require.Equal(t, int64(100), e1.CreatedMin)
	require.Equal(t, int64(200), e1.CreatedMax)
	require.Equal(t, int64(111), e1.SizeBytes)
	require.Equal(t, map[string]string{"row_id": "UUID", "changed_at": "BIGINT"}, e1.Columns)

	// No dead rows and no listed base entries: neither guard needs a masked
	// probe, so every UncoveredRows call is a bare enumeration.
	for i, listed := range stats.uncoveredCalls {
		require.Empty(t, listed, "unexpected masked probe of %s", stats.uncoveredKeys[i])
	}
}

func TestPromote_RefusesPartialCoverage(t *testing.T) {
	lister, stats, live, _, _ := completeInitSet()
	live.liveCount = 4 // one live row not covered by the set
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	r := promoteReconciler(t, lister, manifests, stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Empty(t, s.PromotedBase)
	require.Contains(t, s.InitPromotionRefusal, "covers 3 of 4 live rows")
	require.Empty(t, manifests.saves, "refusal must not touch the manifest")
	require.True(t, report.HasResidualDiscrepancies())
}

func TestPromote_RefusesDeadRowsDoNotCount(t *testing.T) {
	lister, stats, live, _, file2 := completeInitSet()
	live.missing = map[string]bool{rid3: true} // rid3 in parquet but deleted in PG
	live.liveCount = 3                         // three live rows exist; set matches only 2
	_ = file2
	r := promoteReconciler(t, lister, newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}}), stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Contains(t, report.Schemas[0].InitPromotionRefusal, "covers 2 of 3 live rows")
}

func TestPromote_RefusesZeroLiveRows(t *testing.T) {
	lister, stats, live, _, _ := completeInitSet()
	live.liveCount = 0
	r := promoteReconciler(t, lister, newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}}), stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Contains(t, report.Schemas[0].InitPromotionRefusal, "no live rows")
}

func TestPromote_RefusesOverlappingRanges(t *testing.T) {
	// Two generations: [rid1,rid3] overlaps [rid2,rid4].
	fileA := initKey(rid1, rid3)
	fileB := initKey(rid2, rid4)
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {
			{Key: fileA, Size: 1, LastModified: testClock()},
			{Key: fileB, Size: 1, LastModified: testClock()},
		},
	}}
	stats := &fakeStats{stats: map[string]compaction.MergeStats{
		fileA: {RowsOut: 2, RowIDMin: rid1, RowIDMax: rid3},
		fileB: {RowsOut: 2, RowIDMin: rid2, RowIDMax: rid4},
	}}
	live := &fakeLiveRows{liveCount: 2}
	r := promoteReconciler(t, lister, newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}}), stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Contains(t, report.Schemas[0].InitPromotionRefusal, "overlap")
	require.Contains(t, report.Schemas[0].InitPromotionRefusal, "init generations")
}

func TestPromote_RefusesTombstoneRows(t *testing.T) {
	lister, stats, live, file1, _ := completeInitSet()
	stats.uncovered[file1] = []compaction.UncoveredRow{{RowID: rid1, Tombstone: true}, {RowID: rid2}}
	r := promoteReconciler(t, lister, newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}}), stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Contains(t, report.Schemas[0].InitPromotionRefusal, "tombstone")
}

func TestPromote_RefusesUnreadableStats(t *testing.T) {
	lister, stats, live, file1, _ := completeInitSet()
	stats.errFor = map[string]error{file1: context.DeadlineExceeded}
	r := promoteReconciler(t, lister, newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}}), stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Contains(t, report.Schemas[0].InitPromotionRefusal, "unreadable parquet stats")
	require.Empty(t, report.Schemas[0].PromotedBase, "one unreadable file refuses the whole set")
}

func TestPromote_RefusesFilenameStatsMismatch(t *testing.T) {
	lister, stats, live, file1, _ := completeInitSet()
	st := stats.stats[file1]
	st.RowIDMax = rid4 // parquet contents disagree with the {min}_{max} name
	stats.stats[file1] = st
	r := promoteReconciler(t, lister, newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}}), stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Contains(t, report.Schemas[0].InitPromotionRefusal, "does not match parquet contents")
}

func TestPromote_ColumnsBestEffort(t *testing.T) {
	lister, stats, live, file1, _ := completeInitSet()
	stats.columnsErr = map[string]error{file1: context.DeadlineExceeded}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	r := promoteReconciler(t, lister, manifests, stats, live)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Schemas[0].PromotedBase, 2, "describe failure must not block promotion")
	for _, f := range manifests.manifests[7].Files {
		if f.Path == file1 {
			require.Nil(t, f.Columns, "failed describe leaves the entry unstamped")
		}
	}
}

// Real-DuckDB end-to-end verification: fixtures written with the exact
// column layout cdc-init exports (init_exporter.go: row_id, changed_at =
// ltbase_updated_at, deleted_at = 0), read back through the production
// stats queries.
func TestPromote_RealStats_CompleteSetPromotes(t *testing.T) {
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
	r := promoteReconciler(t, lister, manifests, &localStatsReader{db: db, dir: dir},
		&fakeLiveRows{liveCount: 3})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{file1, file2}, report.Schemas[0].PromotedBase)
	require.False(t, report.HasResidualDiscrepancies())

	saved := manifests.manifests[7]
	require.Len(t, saved.Files, 2)
	for _, f := range saved.Files {
		require.Equal(t, "base", f.Tier)
		require.NotEmpty(t, f.Columns, "real describe must stamp columns")
	}
}

type initFixtureRow struct {
	rowID     string
	changedAt int64
}

func writeInitFixture(t *testing.T, db *sql.DB, path string, rows ...initFixtureRow) {
	t.Helper()
	selects := make([]string, 0, len(rows))
	for _, row := range rows {
		selects = append(selects, fmt.Sprintf(
			"SELECT CAST('%s' AS UUID) AS row_id, CAST(%d AS BIGINT) AS changed_at, CAST(0 AS BIGINT) AS deleted_at, 'x' AS title",
			row.rowID, row.changedAt))
	}
	q := fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET)", strings.Join(selects, " UNION ALL "), path)
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("write init fixture: %v", err)
	}
}

// writeTombstoneFixture writes a delta-shaped parquet carrying one row's
// tombstone at the given changed_at — the flushed delete that must mask a
// deleted row's stale live version inside an init export.
func writeTombstoneFixture(t *testing.T, db *sql.DB, path, rowID string, changedAt int64) {
	t.Helper()
	q := fmt.Sprintf(
		"COPY (SELECT CAST('%s' AS UUID) AS row_id, CAST(%d AS BIGINT) AS changed_at, CAST(%d AS BIGINT) AS deleted_at, 'x' AS title) TO '%s' (FORMAT PARQUET)",
		rowID, changedAt, changedAt, path)
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("write tombstone fixture: %v", err)
	}
}
