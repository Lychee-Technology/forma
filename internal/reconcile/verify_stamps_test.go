package reconcile

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifest"
)

// stampCols is the healthy delta-export footer shape. Each call returns a
// fresh map so a test that mutates one side cannot alias the other into
// accidental equality.
func stampCols() map[string]string {
	return map[string]string{
		"row_id":     "UUID",
		"changed_at": "BIGINT",
		"deleted_at": "BIGINT",
		"title":      "VARCHAR",
	}
}

func verifyReconciler(t *testing.T, lister *fakeLister, manifests *fakeManifests, stats StatsReader) *Reconciler {
	t.Helper()
	r := newTestReconciler(lister, manifests, &fakeDeleter{}, &fakeLocker{}, &fakeEnum{ids: []int16{7}})
	r.Stats = stats
	r.Opts = Options{VerifyStamps: true, MaxETagRetries: 3}
	return r
}

// listedEntry wires one object that is both present in S3 and listed in the
// manifest — the only shape --verify-stamps probes.
func listedEntry(key string, stamp map[string]string) (*fakeLister, *fakeManifests) {
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {{Key: key, Size: 10, LastModified: testClock()}},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: key, Columns: stamp},
	}})
	return lister, manifests
}

func TestVerifyStamps_MatchingFooterIsClean(t *testing.T) {
	key := "data/7/" + uuidA + ".parquet"
	lister, manifests := listedEntry(key, stampCols())
	stats := &fakeStats{columns: map[string]map[string]string{key: stampCols()}}
	r := verifyReconciler(t, lister, manifests, stats)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{key}, stats.columnsCalls, "the listed stamped entry must be probed")
	require.Empty(t, report.Schemas[0].StampDivergences)
	require.False(t, report.Schemas[0].Residual())
	require.False(t, report.HasResidualDiscrepancies())

	var out bytes.Buffer
	report.Render(&out)
	require.Equal(t, "schema 7: clean\n", out.String())
}

func TestVerifyStamps_MissingDeletedAtIsDivergence(t *testing.T) {
	// F1: the object no longer carries deleted_at. At the read path a
	// NULL-filled deleted_at is indistinguishable from a live delta row
	// (#274), so only this offline map comparison can see it.
	key := "data/7/" + uuidA + ".parquet"
	lister, manifests := listedEntry(key, stampCols())
	footer := stampCols()
	delete(footer, "deleted_at")
	stats := &fakeStats{columns: map[string]map[string]string{key: footer}}
	r := verifyReconciler(t, lister, manifests, stats)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Len(t, s.StampDivergences, 1)
	require.Equal(t, fmt.Sprintf("%s: column %q stamp %q vs footer %q", key, "deleted_at", "BIGINT", "(absent)"),
		s.StampDivergences[0])
	require.True(t, s.Residual(), "a byte-truth breach is actionable")
	require.True(t, report.HasResidualDiscrepancies(), "must reach the exit-2 path")

	var out bytes.Buffer
	report.Render(&out)
	require.Contains(t, out.String(), "stamp divergence: "+s.StampDivergences[0])
}

func TestVerifyStamps_RetypedRowIDIsDivergence(t *testing.T) {
	// F2: row_id re-typed to VARCHAR admits case-variant UUID spellings that
	// partition as duplicate rows; the scan guard pins only changed_at and
	// deleted_at types, never row_id (#147).
	key := "data/7/" + uuidA + ".parquet"
	lister, manifests := listedEntry(key, stampCols())
	footer := stampCols()
	footer["row_id"] = "VARCHAR"
	stats := &fakeStats{columns: map[string]map[string]string{key: footer}}
	r := verifyReconciler(t, lister, manifests, stats)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Len(t, s.StampDivergences, 1)
	require.Equal(t, fmt.Sprintf("%s: column %q stamp %q vs footer %q", key, "row_id", "UUID", "VARCHAR"),
		s.StampDivergences[0])
	require.True(t, report.HasResidualDiscrepancies())
}

func TestVerifyStamps_LegacyUnstampedEntrySkipped(t *testing.T) {
	// #256 never backfills: an entry written before stamping has no Columns
	// and the read path probes it lazily. There is nothing to compare.
	key := "data/7/" + uuidA + ".parquet"
	lister, manifests := listedEntry(key, nil)
	stats := &fakeStats{columns: map[string]map[string]string{key: stampCols()}}
	r := verifyReconciler(t, lister, manifests, stats)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, stats.columnsCalls, "a legacy entry must not be probed")
	require.Empty(t, report.Schemas[0].StampDivergences)
	require.False(t, report.HasResidualDiscrepancies())
}

func TestVerifyStamps_DanglingEntryNotProbed(t *testing.T) {
	// A confirmed-dangling entry has no bytes behind it; probing it would
	// turn an already-reported condition into a spurious tool failure.
	key := "data/7/" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{"data/7/": nil}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: key, Columns: stampCols()},
	}})
	stats := &fakeStats{columns: map[string]map[string]string{}}
	r := verifyReconciler(t, lister, manifests, stats)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Equal(t, []string{key}, s.Dangling)
	require.Empty(t, stats.columnsCalls, "a dangling entry must not be probed")
	require.Empty(t, s.StampDivergences)
}

func TestVerifyStamps_ProbeErrorFailsSchemaOnly(t *testing.T) {
	// A storage outage is a tool failure (exit 1), never a divergence
	// verdict — the same rule confirmDangling follows.
	keyA := "data/1/" + uuidA + ".parquet"
	keyB := "data/2/" + uuidB + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/1/": {{Key: keyA, Size: 10, LastModified: testClock()}},
		"data/2/": {{Key: keyB, Size: 10, LastModified: testClock()}},
	}}
	manifests := newFakeManifests(
		&manifest.Manifest{SchemaID: 1, Files: []manifest.FileEntry{{Tier: "delta", Path: keyA, Columns: stampCols()}}},
		&manifest.Manifest{SchemaID: 2, Files: []manifest.FileEntry{{Tier: "delta", Path: keyB, Columns: stampCols()}}},
	)
	stats := &fakeStats{
		columns:    map[string]map[string]string{keyB: stampCols()},
		columnsErr: map[string]error{keyA: fmt.Errorf("s3 unavailable")},
	}
	r := verifyReconciler(t, lister, manifests, stats)
	r.Schemas = &fakeEnum{ids: []int16{1, 2}}

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Error(t, report.Schemas[0].Err)
	require.Contains(t, report.Schemas[0].Err.Error(), "stamp verification")
	require.Empty(t, report.Schemas[0].StampDivergences, "a probe failure is never a divergence verdict")
	require.NoError(t, report.Schemas[1].Err, "other schemas still reconcile")
}

func TestVerifyStamps_DisabledByDefault(t *testing.T) {
	key := "data/7/" + uuidA + ".parquet"
	lister, manifests := listedEntry(key, stampCols())
	footer := stampCols()
	delete(footer, "deleted_at")
	stats := &fakeStats{columns: map[string]map[string]string{key: footer}}
	r := verifyReconciler(t, lister, manifests, stats)
	r.Opts.VerifyStamps = false

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, stats.columnsCalls, "verification is opt-in")
	require.Empty(t, report.Schemas[0].StampDivergences)
	require.False(t, report.HasResidualDiscrepancies())
}

// TestVerifyStamps_RealFooterCatchesMissingDeletedAt pins the review's F1
// repro at the tool level against real parquet bytes: an object exported
// without deleted_at behind an entry stamped as carrying it.
func TestVerifyStamps_RealFooterCatchesMissingDeletedAt(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	dir := t.TempDir()
	key := "data/7/" + uuidA + ".parquet"
	q := fmt.Sprintf(`COPY (
		SELECT CAST('%s' AS UUID) AS row_id, CAST(100 AS BIGINT) AS changed_at, 'x' AS title
	) TO '%s' (FORMAT PARQUET)`, uuidA, filepath.Join(dir, uuidA+".parquet"))
	_, err = db.Exec(q)
	require.NoError(t, err)

	lister, manifests := listedEntry(key, stampCols())
	r := verifyReconciler(t, lister, manifests, &localStatsReader{db: db, dir: dir})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.NoError(t, s.Err)
	require.Equal(t, []string{fmt.Sprintf("%s: column %q stamp %q vs footer %q", key, "deleted_at", "BIGINT", "(absent)")},
		s.StampDivergences)
	require.True(t, report.HasResidualDiscrepancies())
}
