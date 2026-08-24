package reconcile

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifest"
)

func gcReconciler(t *testing.T, lister *fakeLister, manifests *fakeManifests, deleter *fakeDeleter) *Reconciler {
	t.Helper()
	r := newTestReconciler(lister, manifests, deleter, &fakeLocker{}, &fakeEnum{ids: []int16{7}})
	r.Opts = Options{GC: true, GCGrace: 15 * time.Minute}
	return r
}

func TestGC_DeletesBaseAndTmpPastGrace(t *testing.T) {
	old := testClock().Add(-16 * time.Minute)
	baseKey := "data/7/base-" + uuidA + ".parquet"
	tmpKey := "data/7/_tmp/" + uuidB + ".parquet"
	listedDelta := "data/7/" + uuidC + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {
			{Key: listedDelta, LastModified: old},
			{Key: baseKey, Size: 10, LastModified: old},
			{Key: tmpKey, Size: 20, LastModified: old},
		},
	}}
	deleter := &fakeDeleter{}
	// The listed delta keeps the manifest non-empty: since #463 --gc
	// refuses to treat base objects as orphans against a zero-entry
	// manifest (see empty_manifest_guard_test.go).
	r := gcReconciler(t, lister, newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: listedDelta},
	}}), deleter)
	seedGCSighting(t, r, 7, old, baseKey, tmpKey)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{baseKey, tmpKey}, deleter.deleted)
	require.ElementsMatch(t, []string{baseKey, tmpKey}, report.Schemas[0].Deleted)
	require.False(t, report.HasResidualDiscrepancies())
}

func TestGC_GraceBoundaryNotDeleted(t *testing.T) {
	atBoundary := testClock().Add(-15 * time.Minute) // exactly grace old: survives
	within := testClock().Add(-14 * time.Minute)
	baseKey := "data/7/base-" + uuidA + ".parquet"
	tmpKey := "data/7/_tmp/" + uuidB + ".parquet"
	listedDelta := "data/7/" + uuidC + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {
			{Key: listedDelta, LastModified: within},
			{Key: baseKey, LastModified: atBoundary},
			{Key: tmpKey, LastModified: within},
		},
	}}
	deleter := &fakeDeleter{}
	// Non-empty manifest so the #463 guard stays out of this test's way.
	r := gcReconciler(t, lister, newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: listedDelta},
	}}), deleter)
	// Sightings are old enough — the object-age gate alone must hold.
	seedGCSighting(t, r, 7, testClock().Add(-time.Hour), baseKey, tmpKey)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, deleter.deleted)
	require.Empty(t, report.Schemas[0].Deleted)
	require.True(t, report.HasResidualDiscrepancies(), "orphans within grace remain residual")
}

func TestGC_SkipsDeltaOrphans(t *testing.T) {
	old := testClock().Add(-24 * time.Hour)
	deltaKey := "data/7/" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {{Key: deltaKey, LastModified: old}},
	}}
	deleter := &fakeDeleter{}
	r := gcReconciler(t, lister, newFakeManifests(&manifest.Manifest{SchemaID: 7}), deleter)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, deleter.deleted, "delta orphans carry unique data and must never be GCed")
	require.Equal(t, []string{deltaKey}, report.Schemas[0].DeltaOrphans)
}

func TestGC_SkipsObjectPresentInManifest(t *testing.T) {
	old := testClock().Add(-24 * time.Hour)
	baseKey := "data/7/base-" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {{Key: baseKey, LastModified: old}},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "base", Path: baseKey},
	}})
	deleter := &fakeDeleter{}
	r := gcReconciler(t, lister, manifests, deleter)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, deleter.deleted, "manifest-listed objects are live data, never GC candidates")
	require.False(t, report.HasResidualDiscrepancies())
}

func TestGC_DeleteFailureBestEffortContinues(t *testing.T) {
	old := testClock().Add(-16 * time.Minute)
	failKey := "data/7/base-" + uuidA + ".parquet"
	okKey := "data/7/_tmp/" + uuidB + ".parquet"
	listedDelta := "data/7/" + uuidC + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {
			{Key: listedDelta, LastModified: old},
			{Key: failKey, LastModified: old},
			{Key: okKey, LastModified: old},
		},
	}}
	deleter := &fakeDeleter{errFor: map[string]error{failKey: fmt.Errorf("access denied")}}
	// Non-empty manifest so the #463 guard stays out of this test's way.
	r := gcReconciler(t, lister, newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: listedDelta},
	}}), deleter)
	seedGCSighting(t, r, 7, old, failKey, okKey)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{okKey}, deleter.deleted)
	require.Equal(t, []string{okKey}, report.Schemas[0].Deleted)
	require.True(t, report.HasResidualDiscrepancies(), "undeleted orphan stays residual")
}
