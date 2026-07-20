package reconcile

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/manifest"
)

var errTestConflict = &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "test conflict"}

// localStatsReader resolves bucket-relative keys to local fixture files and
// recomputes stats with the real DuckDB query the production reader uses.
type localStatsReader struct {
	db  *sql.DB
	dir string
}

func (l *localStatsReader) FileStats(ctx context.Context, key string) (compaction.MergeStats, error) {
	return compaction.SingleFileStats(ctx, l.db, filepath.Join(l.dir, filepath.Base(key)))
}

func (l *localStatsReader) UncoveredRows(ctx context.Context, key string, listedKeys []string) ([]compaction.UncoveredRow, error) {
	listed := make([]string, 0, len(listedKeys))
	for _, k := range listedKeys {
		listed = append(listed, filepath.Join(l.dir, filepath.Base(k)))
	}
	return compaction.UncoveredRows(ctx, l.db, filepath.Join(l.dir, filepath.Base(key)), listed)
}

func writeDeltaFixture(t *testing.T, db *sql.DB, path string) {
	t.Helper()
	q := fmt.Sprintf(`COPY (
		SELECT CAST('%s' AS UUID) AS row_id, CAST(100 AS BIGINT) AS changed_at, CAST(NULL AS BIGINT) AS deleted_at, 'a' AS title
		UNION ALL
		SELECT CAST('%s' AS UUID) AS row_id, CAST(300 AS BIGINT) AS changed_at, CAST(NULL AS BIGINT) AS deleted_at, 'b' AS title
	) TO '%s' (FORMAT PARQUET)`, uuidA, uuidB, path)
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("write delta fixture: %v", err)
	}
}

func repairReconciler(t *testing.T, lister *fakeLister, manifests *fakeManifests, enum *fakeEnum, stats StatsReader) *Reconciler {
	t.Helper()
	r := newTestReconciler(lister, manifests, &fakeDeleter{}, &fakeLocker{}, enum)
	r.Stats = stats
	r.LiveRows = &fakeLiveRows{} // every row live: the safe-append default
	r.Opts = Options{Repair: true, MaxETagRetries: 3}
	return r
}

func TestRepair_AppendsDeltaOrphanWithRecomputedStats(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	dir := t.TempDir()
	orphanKey := "data/7/" + uuidA + ".parquet"
	writeDeltaFixture(t, db, filepath.Join(dir, uuidA+".parquet"))

	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {{Key: orphanKey, Size: 4242, LastModified: testClock()}},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	r := repairReconciler(t, lister, manifests, &fakeEnum{ids: []int16{7}}, &localStatsReader{db: db, dir: dir})

	report, err := r.Run(context.Background())
	require.NoError(t, err)

	require.Len(t, manifests.saves, 1)
	saved := manifests.saves[0].m
	require.Len(t, saved.Files, 1)
	entry := saved.Files[0]
	require.Equal(t, "delta", entry.Tier)
	require.Equal(t, orphanKey, entry.Path)
	require.Equal(t, uuidA, entry.RowIDMin)
	require.Equal(t, uuidB, entry.RowIDMax)
	require.Equal(t, int64(100), entry.CreatedMin)
	require.Equal(t, int64(300), entry.CreatedMax)
	require.Equal(t, int64(2), entry.RowCount)
	require.Equal(t, int64(4242), entry.SizeBytes)

	require.Equal(t, []string{orphanKey}, report.Schemas[0].Repaired)
	require.False(t, report.HasResidualDiscrepancies())
}

func TestRepair_Idempotent_SecondRunAppendsNothing(t *testing.T) {
	orphanKey := "data/7/" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {{Key: orphanKey, Size: 10, LastModified: testClock()}},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	stats := &fakeStats{stats: map[string]compaction.MergeStats{
		orphanKey: {RowsOut: 1, RowIDMin: uuidA, RowIDMax: uuidA, CreatedMin: 1, CreatedMax: 1},
	}}
	r := repairReconciler(t, lister, manifests, &fakeEnum{ids: []int16{7}}, stats)

	_, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, manifests.saves, 1)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, manifests.saves, 1, "second run must not save again")
	require.Empty(t, report.Schemas[0].DeltaOrphans, "repaired file is no longer an orphan")
	require.Len(t, manifests.manifests[7].Files, 1, "no duplicate path entries")
}

func TestRepair_SkipsBaseAndTmpOrphans(t *testing.T) {
	deltaKey := "data/7/" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {
			{Key: deltaKey, Size: 10, LastModified: testClock()},
			{Key: "data/7/base-" + uuidB + ".parquet", Size: 20, LastModified: testClock()},
			{Key: "data/7/_tmp/" + uuidB + ".parquet", Size: 30, LastModified: testClock()},
		},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	stats := &fakeStats{stats: map[string]compaction.MergeStats{
		deltaKey: {RowsOut: 1, RowIDMin: uuidA, RowIDMax: uuidA, CreatedMin: 1, CreatedMax: 1},
	}}
	r := repairReconciler(t, lister, manifests, &fakeEnum{ids: []int16{7}}, stats)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{deltaKey}, report.Schemas[0].Repaired)
	require.Len(t, manifests.manifests[7].Files, 1)
	require.Equal(t, deltaKey, manifests.manifests[7].Files[0].Path)
	require.Equal(t, []string{deltaKey}, stats.calls, "stats must only be read for delta orphans")
}

func TestRepair_StatsErrorSkipsFileLeavesResidual(t *testing.T) {
	goodKey := "data/7/" + uuidA + ".parquet"
	badKey := "data/7/" + uuidB + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {
			{Key: goodKey, Size: 10, LastModified: testClock()},
			{Key: badKey, Size: 20, LastModified: testClock()},
		},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	stats := &fakeStats{
		stats:  map[string]compaction.MergeStats{goodKey: {RowsOut: 1, RowIDMin: uuidA, RowIDMax: uuidA}},
		errFor: map[string]error{badKey: fmt.Errorf("corrupt footer")},
	}
	r := repairReconciler(t, lister, manifests, &fakeEnum{ids: []int16{7}}, stats)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{goodKey}, report.Schemas[0].Repaired)
	require.Len(t, manifests.manifests[7].Files, 1)
	require.True(t, report.HasResidualDiscrepancies(), "unreadable orphan stays residual")
}

func TestRepair_ETagConflictReloadsAndRetries(t *testing.T) {
	keyA := "data/7/" + uuidA + ".parquet"
	keyB := "data/7/" + uuidB + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {
			{Key: keyA, Size: 10, LastModified: testClock()},
			{Key: keyB, Size: 20, LastModified: testClock()},
		},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	// First save hits a 412; the concurrent writer that won the race added
	// keyB. The retry must reload, drop keyB from its append set, and save
	// only keyA — never a duplicate keyB entry.
	manifests.saveErrs = []error{errTestConflict}
	manifests.onSaveConflict = func(f *fakeManifests) {
		f.manifests[7] = &manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
			{Tier: "delta", Path: keyB},
		}}
		f.etags[7] = "etag-7-concurrent"
	}
	stats := &fakeStats{stats: map[string]compaction.MergeStats{
		keyA: {RowsOut: 1, RowIDMin: uuidA, RowIDMax: uuidA},
		keyB: {RowsOut: 1, RowIDMin: uuidB, RowIDMax: uuidB},
	}}
	r := repairReconciler(t, lister, manifests, &fakeEnum{ids: []int16{7}}, stats)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.NoError(t, report.Schemas[0].Err)

	final := manifests.manifests[7]
	require.Len(t, final.Files, 2)
	paths := []string{final.Files[0].Path, final.Files[1].Path}
	require.ElementsMatch(t, []string{keyA, keyB}, paths)
	require.ElementsMatch(t, []string{keyA, keyB}, report.Schemas[0].Repaired)
	require.False(t, report.HasResidualDiscrepancies())
	// The successful retry save carries the reloaded etag.
	require.Equal(t, "etag-7-concurrent", manifests.saves[len(manifests.saves)-1].etag)
}

func TestRepair_ETagConflictExhaustsRetries(t *testing.T) {
	keyA := "data/1/" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/1/": {{Key: keyA, Size: 10, LastModified: testClock()}},
		"data/2/": nil,
	}}
	manifests := newFakeManifests(
		&manifest.Manifest{SchemaID: 1, Files: []manifest.FileEntry{}},
		&manifest.Manifest{SchemaID: 2, Files: []manifest.FileEntry{}},
	)
	manifests.saveErrs = []error{errTestConflict, errTestConflict, errTestConflict, errTestConflict}
	stats := &fakeStats{stats: map[string]compaction.MergeStats{
		keyA: {RowsOut: 1, RowIDMin: uuidA, RowIDMax: uuidA},
	}}
	r := repairReconciler(t, lister, manifests, &fakeEnum{ids: []int16{1, 2}}, stats)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Error(t, report.Schemas[0].Err, "retry exhaustion must surface per schema")
	require.NoError(t, report.Schemas[1].Err, "other schemas still reconcile")
	require.True(t, report.HasResidualDiscrepancies())
}

func TestRepair_NonConflictSaveErrorFailsSchema(t *testing.T) {
	keyA := "data/7/" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {{Key: keyA, Size: 10, LastModified: testClock()}},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	manifests.saveErrs = []error{fmt.Errorf("s3 timeout")}
	stats := &fakeStats{stats: map[string]compaction.MergeStats{
		keyA: {RowsOut: 1, RowIDMin: uuidA, RowIDMax: uuidA},
	}}
	r := repairReconciler(t, lister, manifests, &fakeEnum{ids: []int16{7}}, stats)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Error(t, report.Schemas[0].Err)
	require.Len(t, manifests.saves, 0, "ambiguous save error must not retry blindly")
}
