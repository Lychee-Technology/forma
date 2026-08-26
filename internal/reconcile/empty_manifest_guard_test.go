package reconcile

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

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

// #481: a NON-EMPTY manifest that accounts for none of this schema's data —
// a fixed-file --manifest-template, a cross-schema/cross-tier mispointing,
// or entries all in a foreign bucket — reaches the same irreversible outcome
// #463 closed off, because the raw entry count waived the guard. The guard
// must key on the in-prefix count, and the foreign signature is never
// waivable: it indicates a wrong template, not a legitimately empty manifest.

func TestGC_ForeignManifestWithBaseObjects_RefusesInsteadOfDeleting(t *testing.T) {
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
	// Fixed-file template resolved schema 7 to another schema's manifest:
	// non-empty, but every entry lies outside data/7/.
	foreign := &manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: "data/9/" + uuidC + ".parquet"},
		{Tier: "base", Path: "s3://other/data/7/base-" + uuidB + ".parquet"},
	}}
	r := newTestReconciler(lister, newFakeManifests(foreign), deleter, &fakeLocker{}, &fakeEnum{ids: []int16{7}})
	r.Opts = Options{GC: true, GCGrace: 15 * time.Minute}
	seedGCSighting(t, r, 7, old, initShaped, merged)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Error(t, s.Err, "live base objects against a foreign manifest must fail the schema")
	require.Contains(t, s.Err.Error(), "2 manifest entries", "refusal must quote the raw count")
	require.Contains(t, s.Err.Error(), "0 in-prefix", "refusal must quote the in-prefix count")
	require.NotContains(t, s.Err.Error(), "--allow-empty-manifest-schema 7",
		"a foreign manifest must not advertise the empty-manifest waiver")
	require.Empty(t, deleter.deleted)
	require.Empty(t, s.Deleted)
}

func TestGC_ForeignManifest_EmptyWaiverDoesNotApply(t *testing.T) {
	old := testClock().Add(-24 * time.Hour)
	merged := "data/7/base-" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {{Key: merged, LastModified: old}},
	}}
	deleter := &fakeDeleter{}
	foreign := &manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: "data/9/" + uuidC + ".parquet"},
	}}
	r := newTestReconciler(lister, newFakeManifests(foreign), deleter, &fakeLocker{}, &fakeEnum{ids: []int16{7}})
	r.Opts = Options{GC: true, GCGrace: 15 * time.Minute, AllowEmptyManifestSchemas: []int16{7}}
	seedGCSighting(t, r, 7, old, merged)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Error(t, report.Schemas[0].Err,
		"--allow-empty-manifest-schema waives EMPTY manifests only, never a non-empty foreign one")
	require.Empty(t, deleter.deleted)
}

func TestGC_ManifestWithInPrefixEntries_GuardDoesNotFire(t *testing.T) {
	// One in-prefix entry proves resolution reached this schema's manifest;
	// leftover GC proceeds even if other entries are foreign.
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
	m := &manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: listedDelta},
		{Tier: "delta", Path: "s3://other/data/7/" + uuidB + ".parquet"},
	}}
	r := newTestReconciler(lister, newFakeManifests(m), deleter, &fakeLocker{}, &fakeEnum{ids: []int16{7}})
	r.Opts = Options{GC: true, GCGrace: 15 * time.Minute}
	seedGCSighting(t, r, 7, old, merged)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.NoError(t, report.Schemas[0].Err)
	require.Equal(t, []string{merged}, deleter.deleted)
}

func TestGC_FixedFileTemplate_ForeignManifest_FailsInsteadOfDeleting(t *testing.T) {
	// #481 acceptance regression: a fixed-file --manifest-template (no
	// {{.SchemaID}}) resolving to an EXISTING foreign manifest, with
	// --data-prefix still matching the writers', must fail --gc — through
	// the real resolver store, not a fake.
	ctx := context.Background()
	mem := newMemManifestStore()
	resolver := &ResolverManifestStore{
		Store:    mem,
		Resolver: manifest.PathResolver{PathTemplate: "manifest/data.json"},
	}
	m7, etag, err := resolver.Load(ctx, 7)
	require.NoError(t, err)
	m7.Files = append(m7.Files, manifest.FileEntry{Tier: "delta", Path: "data/7/" + uuidC + ".parquet"})
	_, err = resolver.Save(ctx, 7, m7, etag)
	require.NoError(t, err)

	old := testClock().Add(-24 * time.Hour)
	merged9 := "data/9/base-" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/9/": {{Key: merged9, LastModified: old}},
	}}
	deleter := &fakeDeleter{}
	r := &Reconciler{
		Lister:     lister,
		Deleter:    deleter,
		Manifests:  resolver,
		Locker:     &fakeLocker{},
		Schemas:    &fakeEnum{ids: []int16{9}},
		GCStates:   newFakeGCState(),
		Now:        testClock,
		Bucket:     "bkt",
		DataPrefix: "data",
		Logger:     zap.NewNop(),
		Opts:       Options{GC: true, GCGrace: 15 * time.Minute},
	}
	seedGCSighting(t, r, 9, old, merged9)

	report, err := r.Run(ctx)
	require.NoError(t, err)
	require.Error(t, report.Schemas[0].Err, "schema 9 must fail, not delete its base tier")
	require.Empty(t, deleter.deleted)
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
