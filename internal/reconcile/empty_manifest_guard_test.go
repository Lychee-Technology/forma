package reconcile

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifest"
)

// #463: a mis-pointed --manifest-prefix/--manifest-template resolves every
// manifest EMPTY (ResolverManifestStore.Load has LoadOrCreate semantics)
// while --data-prefix still matches, so every base object classifies as an
// orphan. --gc must fail that signature — N live base objects against zero
// manifest entries is a resolution failure, not mass orphaning — rather
// than delete the whole base tier after the grace.

func emptyManifestGuardReconciler(t *testing.T, lister *fakeLister, deleter *fakeDeleter, ids ...int16) *Reconciler {
	t.Helper()
	var ms []*manifest.Manifest
	for _, id := range ids {
		ms = append(ms, &manifest.Manifest{SchemaID: id})
	}
	r := newTestReconciler(lister, newFakeManifests(ms...), deleter, &fakeLocker{}, &fakeEnum{ids: ids})
	r.Opts = Options{GC: true, GCGrace: 15 * time.Minute}
	return r
}

func TestGC_EmptyManifestWithBaseObjects_RefusesInsteadOfDeleting(t *testing.T) {
	old := testClock().Add(-24 * time.Hour)
	initShaped := "data/7/" + uuidA + "_" + uuidB + ".parquet"
	merged := "data/7/base-" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {
			{Key: initShaped, LastModified: old},
			{Key: merged, LastModified: old},
		},
	}}
	deleter := &fakeDeleter{}
	r := emptyManifestGuardReconciler(t, lister, deleter, 7)
	// Sightings AND ages are both past the grace: without the guard this
	// exact run performs the irreversible deletion (the cron's later pass
	// in the issue's reproduction).
	seedGCSighting(t, r, 7, old, initShaped, merged)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Error(t, s.Err, "live base objects against an empty manifest must fail the schema (exit 1)")
	require.Contains(t, s.Err.Error(), "0 manifest entries")
	require.Contains(t, s.Err.Error(), "init=1 merged=1")
	require.Contains(t, s.Err.Error(), "--allow-empty-manifest-schema 7")
	require.Empty(t, deleter.deleted, "the guard must refuse before any deletion")
	require.Empty(t, s.Deleted)
}

func TestGC_EmptyManifestOverride_IsSchemaExplicit(t *testing.T) {
	old := testClock().Add(-24 * time.Hour)
	merged7 := "data/7/base-" + uuidA + ".parquet"
	merged8 := "data/8/base-" + uuidB + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {{Key: merged7, LastModified: old}},
		"data/8/": {{Key: merged8, LastModified: old}},
	}}
	deleter := &fakeDeleter{}
	r := emptyManifestGuardReconciler(t, lister, deleter, 7, 8)
	r.Opts.AllowEmptyManifestSchemas = []int16{7}
	seedGCSighting(t, r, 7, old, merged7)
	seedGCSighting(t, r, 8, old, merged8)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.NoError(t, report.Schemas[0].Err)
	require.Equal(t, []string{merged7}, report.Schemas[0].Deleted, "allowed schema keeps normal GC")
	require.Error(t, report.Schemas[1].Err, "the allowance must not wave through other schemas")
	require.Equal(t, []string{merged7}, deleter.deleted)
}

func TestGC_EmptyManifestTmpOnly_GuardDoesNotFire(t *testing.T) {
	// The hazard is confined to the base tier (delta orphans only become GC
	// candidates under --repair; _tmp is staging garbage) — tmp-only
	// schemas with empty manifests keep their GC.
	old := testClock().Add(-24 * time.Hour)
	tmpKey := "data/7/_tmp/" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {{Key: tmpKey, LastModified: old}},
	}}
	deleter := &fakeDeleter{}
	r := emptyManifestGuardReconciler(t, lister, deleter, 7)
	seedGCSighting(t, r, 7, old, tmpKey)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.NoError(t, report.Schemas[0].Err)
	require.Equal(t, []string{tmpKey}, deleter.deleted)
}

func TestGC_NonEmptyManifest_GuardDoesNotFire(t *testing.T) {
	old := testClock().Add(-24 * time.Hour)
	listedDelta := "data/7/" + uuidC + ".parquet"
	merged := "data/7/base-" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {
			{Key: listedDelta, LastModified: old},
			{Key: merged, LastModified: old},
		},
	}}
	deleter := &fakeDeleter{}
	r := newTestReconciler(lister, newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: listedDelta},
	}}), deleter, &fakeLocker{}, &fakeEnum{ids: []int16{7}})
	r.Opts = Options{GC: true, GCGrace: 15 * time.Minute}
	seedGCSighting(t, r, 7, old, merged)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.NoError(t, report.Schemas[0].Err)
	require.Equal(t, []string{merged}, deleter.deleted, "a resolvable manifest keeps normal leftover GC")
}
