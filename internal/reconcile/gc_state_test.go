package reconcile

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifest"
)

// The GC sighting state implements the #203 follow-up contract: an orphan
// is deleted only after it has been OBSERVED unlisted for longer than the
// grace period — LastModified alone cannot express "unlisted duration", so
// an old source file freshly spliced out by the compactor would otherwise
// be deleted inside the in-flight-reader window.

func gcStateReconciler(t *testing.T, lister *fakeLister, deleter *fakeDeleter, states *fakeGCState) *Reconciler {
	t.Helper()
	// One listed delta keeps schema 7's manifest non-empty — since #463
	// --gc refuses base orphans against a zero-entry manifest, and these
	// tests are about sighting-state mechanics, not the guard.
	listedDelta := "data/7/" + uuidC + ".parquet"
	lister.objects["data/7/"] = append(lister.objects["data/7/"],
		ObjectInfo{Key: listedDelta, LastModified: testClock().Add(-24 * time.Hour)})
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: listedDelta},
	}})
	r := newTestReconciler(lister, manifests, deleter, &fakeLocker{}, &fakeEnum{ids: []int16{7}})
	r.GCStates = states
	r.Opts = Options{GC: true, GCGrace: 15 * time.Minute}
	return r
}

func TestGC_FirstSightingRecordsInsteadOfDeleting(t *testing.T) {
	old := testClock().Add(-24 * time.Hour) // ancient object — age alone must NOT suffice
	merged := "data/7/base-" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {{Key: merged, LastModified: old}},
	}}
	deleter := &fakeDeleter{}
	states := newFakeGCState()
	r := gcStateReconciler(t, lister, deleter, states)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, deleter.deleted, "an orphan seen unlisted for the first time must only be recorded")
	require.Empty(t, report.Schemas[0].Deleted)
	require.Equal(t, testClock().UnixMilli(), states.state[7][merged], "first-unlisted sighting must be persisted")
}

func TestGC_DeletesOnlyAfterUnlistedBeyondGrace(t *testing.T) {
	old := testClock().Add(-24 * time.Hour)
	merged := "data/7/base-" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {{Key: merged, LastModified: old}},
	}}
	deleter := &fakeDeleter{}
	states := newFakeGCState()
	states.seed(7, merged, testClock().Add(-16*time.Minute)) // observed unlisted 16m ago > 15m grace
	r := gcStateReconciler(t, lister, deleter, states)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{merged}, deleter.deleted)
	require.Equal(t, []string{merged}, report.Schemas[0].Deleted)
	require.NotContains(t, states.state[7], merged, "deleted key must leave the sighting state")
}

func TestGC_UnlistedWithinGraceNotDeleted(t *testing.T) {
	old := testClock().Add(-24 * time.Hour)
	merged := "data/7/base-" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {{Key: merged, LastModified: old}},
	}}
	deleter := &fakeDeleter{}
	states := newFakeGCState()
	states.seed(7, merged, testClock().Add(-14*time.Minute)) // observed unlisted only 14m
	r := gcStateReconciler(t, lister, deleter, states)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, deleter.deleted, "a freshly spliced-out old source must survive the grace window")
	require.Empty(t, report.Schemas[0].Deleted)
	require.Contains(t, states.state[7], merged, "pending sighting must be retained")
}

func TestGC_RelistedKeyPrunedFromState(t *testing.T) {
	// A key that stops being a candidate (re-listed in the manifest, or
	// gone) must drop out of the sighting state so a later unlisting
	// restarts its grace clock.
	stale := "data/7/base-" + uuidB + ".parquet"
	listedKey := "data/7/base-" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {{Key: listedKey, LastModified: testClock().Add(-time.Hour)}},
	}}
	deleter := &fakeDeleter{}
	states := newFakeGCState()
	states.seed(7, stale, testClock().Add(-time.Hour))
	r := gcStateReconciler(t, lister, deleter, states)
	r.Manifests = newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "base", Path: listedKey},
	}})

	_, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, deleter.deleted)
	require.NotContains(t, states.state[7], stale)
}

func TestGC_StateLoadErrorIsToolFailure(t *testing.T) {
	merged := "data/7/base-" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {{Key: merged, LastModified: testClock().Add(-time.Hour)}},
	}}
	deleter := &fakeDeleter{}
	states := newFakeGCState()
	states.loadErr = context.DeadlineExceeded
	r := gcStateReconciler(t, lister, deleter, states)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, deleter.deleted, "unreadable sighting state must never allow deletion")
	require.Error(t, report.Schemas[0].Err)
}

func TestManifestGCStateStore_RoundTripAndMissing(t *testing.T) {
	store := &ManifestGCStateStore{
		Store:    newMemManifestStore(),
		Resolver: manifest.PathResolver{PathTemplate: "manifest/{{.SchemaID}}.json"},
	}
	ctx := context.Background()

	state, etag, err := store.Load(ctx, 7)
	require.NoError(t, err, "missing state must load as empty")
	require.Empty(t, state)
	require.Empty(t, etag)

	state = map[string]int64{"data/7/base-x.parquet": 1234}
	_, err = store.Save(ctx, 7, state, etag)
	require.NoError(t, err)

	reloaded, etag2, err := store.Load(ctx, 7)
	require.NoError(t, err)
	require.NotEmpty(t, etag2)
	require.Equal(t, state, reloaded)
}
