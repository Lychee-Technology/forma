package reconcile

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/manifest"
	"go.uber.org/zap"
)

func testClock() time.Time {
	return time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
}

func newTestReconciler(lister *fakeLister, manifests *fakeManifests, deleter *fakeDeleter, locker *fakeLocker, enum *fakeEnum) *Reconciler {
	return &Reconciler{
		Lister:     lister,
		Deleter:    deleter,
		Manifests:  manifests,
		Locker:     locker,
		Schemas:    enum,
		GCStates:   newFakeGCState(),
		Now:        testClock,
		Bucket:     "bkt",
		DataPrefix: "data",
		Logger:     zap.NewNop(),
	}
}

// seedGCSighting marks keys as already observed unlisted at the given time,
// so single-run GC tests can exercise the deletion path directly.
func seedGCSighting(t *testing.T, r *Reconciler, schemaID int16, at time.Time, keys ...string) {
	t.Helper()
	fake, ok := r.GCStates.(*fakeGCState)
	if !ok {
		t.Fatalf("reconciler GCStates is %T, want *fakeGCState", r.GCStates)
	}
	for _, key := range keys {
		fake.seed(schemaID, key, at)
	}
}

func TestReconcileSchema_LockNotAcquired_Skips(t *testing.T) {
	lister := &fakeLister{}
	locker := &fakeLocker{denied: map[int16]bool{7: true}}
	r := newTestReconciler(lister, newFakeManifests(), &fakeDeleter{}, locker, &fakeEnum{ids: []int16{7}})

	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(report.Schemas) != 1 || !report.Schemas[0].Skipped {
		t.Fatalf("expected schema 7 skipped, got %+v", report.Schemas)
	}
	if len(lister.calls) != 0 {
		t.Fatalf("skipped schema must not be listed, got calls %v", lister.calls)
	}
	if !report.HasResidualDiscrepancies() {
		t.Fatal("skipped schema must count as residual discrepancy")
	}
}

func TestRun_EnumeratesAllSchemasAndReleasesLocks(t *testing.T) {
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/1/": {{Key: "data/1/" + uuidA + ".parquet", Size: 10, LastModified: testClock()}},
		"data/2/": {{Key: "data/2/" + uuidB + ".parquet", Size: 20, LastModified: testClock()}},
	}}
	manifests := newFakeManifests(
		&manifest.Manifest{SchemaID: 1, Files: []manifest.FileEntry{}},
		&manifest.Manifest{SchemaID: 2, Files: []manifest.FileEntry{
			{Tier: "delta", Path: "data/2/" + uuidB + ".parquet"},
		}},
	)
	locker := &fakeLocker{}
	r := newTestReconciler(lister, manifests, &fakeDeleter{}, locker, &fakeEnum{ids: []int16{1, 2}})

	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(report.Schemas) != 2 {
		t.Fatalf("expected 2 schema reports, got %d", len(report.Schemas))
	}
	assertKeys(t, "schema 1 delta orphans", report.Schemas[0].DeltaOrphans,
		[]string{"data/1/" + uuidA + ".parquet"})
	if len(report.Schemas[1].DeltaOrphans) != 0 {
		t.Fatalf("schema 2 must be clean, got %+v", report.Schemas[1])
	}
	if len(locker.locked) != 2 || len(locker.unlocked) != 2 {
		t.Fatalf("locks acquired=%v released=%v, want both [1 2]", locker.locked, locker.unlocked)
	}
}

func TestRun_ReportOnly_MutatesNothing(t *testing.T) {
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {
			{Key: "data/7/" + uuidA + ".parquet", LastModified: testClock().Add(-time.Hour)},
			{Key: "data/7/base-" + uuidB + ".parquet", LastModified: testClock().Add(-time.Hour)},
			{Key: "data/7/_tmp/" + uuidB + ".parquet", LastModified: testClock().Add(-time.Hour)},
		},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: "data/7/gone.parquet"},
	}})
	deleter := &fakeDeleter{}
	r := newTestReconciler(lister, manifests, deleter, &fakeLocker{}, &fakeEnum{ids: []int16{7}})

	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(manifests.saves) != 0 {
		t.Fatalf("report mode must not save manifests, got %d saves", len(manifests.saves))
	}
	if len(deleter.deleted) != 0 {
		t.Fatalf("report mode must not delete objects, got %v", deleter.deleted)
	}
	s := report.Schemas[0]
	if len(s.DeltaOrphans) != 1 || len(s.BaseOrphans) != 1 || len(s.TmpOrphans) != 1 || len(s.Dangling) != 1 {
		t.Fatalf("expected full diff in report, got %+v", s)
	}
}

func TestRun_LockErrorRecorded_OtherSchemasContinue(t *testing.T) {
	lister := &fakeLister{objects: map[string][]ObjectInfo{}}
	locker := &fakeLocker{}
	r := newTestReconciler(lister, newFakeManifests(), &fakeDeleter{}, locker, &fakeEnum{ids: []int16{1, 2}})

	// Fail only schema 1's listing; schema 2 must still reconcile.
	lister.err = errors.New("s3 unavailable")
	failOnce := lister.err
	lister.err = nil
	r.Lister = listerFunc(func(ctx context.Context, prefix string) ([]ObjectInfo, error) {
		if prefix == "data/1/" {
			return nil, failOnce
		}
		return nil, nil
	})

	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.Schemas[0].Err == nil {
		t.Fatal("schema 1 error must be recorded")
	}
	if report.Schemas[1].Err != nil || report.Schemas[1].Skipped {
		t.Fatalf("schema 2 must reconcile normally, got %+v", report.Schemas[1])
	}
	if !report.HasResidualDiscrepancies() {
		t.Fatal("schema error must count as residual discrepancy")
	}
}

type listerFunc func(ctx context.Context, prefix string) ([]ObjectInfo, error)

func (f listerFunc) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	return f(ctx, prefix)
}

func TestReconcileSchema_RecordsInventoryCounts(t *testing.T) {
	deltaListed := "data/7/" + uuidA + ".parquet"
	merged := "data/7/base-" + uuidB + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {
			{Key: deltaListed, LastModified: testClock()},
			{Key: merged, LastModified: testClock()},
			{Key: "data/7/notes.txt", LastModified: testClock()}, // not parquet: never counted
		},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: deltaListed},
	}})
	r := newTestReconciler(lister, manifests, &fakeDeleter{}, &fakeLocker{}, &fakeEnum{ids: []int16{7}})

	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	s := report.Schemas[0]
	if s.ObjectsSeen != 2 {
		t.Fatalf("ObjectsSeen = %d, want 2 (parquet objects only)", s.ObjectsSeen)
	}
	if s.ManifestEntries != 1 {
		t.Fatalf("ManifestEntries = %d, want 1", s.ManifestEntries)
	}
}
