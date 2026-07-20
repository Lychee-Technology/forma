package reconcile

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifest"
)

func TestGC_SkipsInitShapedBaseOrphans(t *testing.T) {
	old := testClock().Add(-24 * time.Hour)
	initShaped := "data/7/" + uuidA + "_" + uuidB + ".parquet"
	merged := "data/7/base-" + uuidB + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {
			{Key: initShaped, LastModified: old},
			{Key: merged, LastModified: old},
		},
	}}
	deleter := &fakeDeleter{}
	r := newTestReconciler(lister, newFakeManifests(&manifest.Manifest{SchemaID: 7}), deleter, &fakeLocker{}, &fakeEnum{ids: []int16{7}})
	r.Opts = Options{GC: true, GCGrace: 15 * time.Minute}
	seedGCSighting(t, r, 7, old, initShaped, merged)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	// An in-flight cdc-init promotes {min}_{max} base files long before it
	// publishes the manifest and holds no advisory lock — GC must never
	// touch that shape, no matter how old or how long unlisted.
	require.Equal(t, []string{merged}, deleter.deleted)
	require.ElementsMatch(t, []string{initShaped, merged}, report.Schemas[0].BaseOrphans)
	require.True(t, report.HasResidualDiscrepancies(), "undeletable init-shaped orphan stays residual")
}

func TestGC_ZeroGraceRefusesDeletion(t *testing.T) {
	old := testClock().Add(-24 * time.Hour)
	merged := "data/7/base-" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {{Key: merged, LastModified: old}},
	}}
	deleter := &fakeDeleter{}
	r := newTestReconciler(lister, newFakeManifests(&manifest.Manifest{SchemaID: 7}), deleter, &fakeLocker{}, &fakeEnum{ids: []int16{7}})
	r.Opts = Options{GC: true} // zero-value grace: must refuse, not delete everything

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, deleter.deleted)
	require.Empty(t, report.Schemas[0].Deleted)
}

func TestReconcileSchema_LoadsManifestBeforeListing(t *testing.T) {
	var order []string
	lister := listerFunc(func(ctx context.Context, prefix string) ([]ObjectInfo, error) {
		order = append(order, "list")
		return nil, nil
	})
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7})
	manifests.onLoad = func(*fakeManifests) { order = append(order, "load") }

	r := newTestReconciler(nil, manifests, &fakeDeleter{}, &fakeLocker{}, &fakeEnum{ids: []int16{7}})
	r.Lister = lister
	_, err := r.Run(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, order)
	// Loading first means a file created after the manifest snapshot still
	// appears in the listing, so a racing compactor's freshly committed
	// base entry can never be reported dangling.
	require.Equal(t, "load", order[0])
}

func TestDangling_DroppedWhenObjectAppearsOnReprobe(t *testing.T) {
	key := "data/7/base-" + uuidA + ".parquet"
	// Initial listing misses the object (compactor wrote it mid-run), but
	// the per-key re-probe finds it.
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {},
		key:       {{Key: key, LastModified: testClock()}},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "base", Path: key},
	}})
	r := newTestReconciler(lister, manifests, &fakeDeleter{}, &fakeLocker{}, &fakeEnum{ids: []int16{7}})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, report.Schemas[0].Dangling, "live object found on re-probe must not be reported dangling")
	require.False(t, report.HasResidualDiscrepancies())
}

func TestDangling_DroppedWhenEntryRemovedOnReload(t *testing.T) {
	key := "data/7/base-" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{"data/7/": {}}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "base", Path: key},
	}})
	// A concurrent compactor spliced the entry out (and deleted its object)
	// between our load and the dangling confirmation reload.
	manifests.onLoad = func(f *fakeManifests) {
		if f.loads > 1 {
			f.manifests[7] = &manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}}
		}
	}
	r := newTestReconciler(lister, manifests, &fakeDeleter{}, &fakeLocker{}, &fakeEnum{ids: []int16{7}})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, report.Schemas[0].Dangling, "entry already spliced out is not dangling")
}

func TestDangling_ProbeErrorIsToolFailureNotDiscrepancy(t *testing.T) {
	// PR #289 review finding 4: a failed re-probe (or manifest reload)
	// leaves dangling UNCONFIRMED — reporting it as confirmed would map a
	// storage outage to exit 2 "data drift" instead of exit 1.
	key := "data/7/base-" + uuidA + ".parquet"
	probeErr := errors.New("s3 transient outage")
	lister := listerFunc(func(ctx context.Context, prefix string) ([]ObjectInfo, error) {
		if prefix == key {
			return nil, probeErr
		}
		return nil, nil
	})
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "base", Path: key},
	}})
	r := newTestReconciler(nil, manifests, &fakeDeleter{}, &fakeLocker{}, &fakeEnum{ids: []int16{7}})
	r.Lister = lister

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Error(t, report.Schemas[0].Err, "an unconfirmable dangling candidate must surface as a schema failure")
}

func TestDangling_ReloadErrorIsToolFailure(t *testing.T) {
	key := "data/7/base-" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{"data/7/": {}}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "base", Path: key},
	}})
	manifests.onLoad = func(f *fakeManifests) {
		if f.loads > 1 {
			f.loadErr = errors.New("manifest store outage")
		}
	}
	r := newTestReconciler(lister, manifests, &fakeDeleter{}, &fakeLocker{}, &fakeEnum{ids: []int16{7}})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Error(t, report.Schemas[0].Err)
}

func TestDangling_ConfirmedWhenStillListedAndAbsent(t *testing.T) {
	key := "data/7/base-" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{"data/7/": {}}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "base", Path: key},
	}})
	r := newTestReconciler(lister, manifests, &fakeDeleter{}, &fakeLocker{}, &fakeEnum{ids: []int16{7}})

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{key}, report.Schemas[0].Dangling)
}
