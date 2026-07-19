package reconcile

import (
	"context"
	"fmt"
	"sync"

	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/manifest"
)

type fakeLister struct {
	objects map[string][]ObjectInfo // prefix -> listing
	calls   []string
	err     error
}

func (f *fakeLister) ListObjects(_ context.Context, prefix string) ([]ObjectInfo, error) {
	f.calls = append(f.calls, prefix)
	if f.err != nil {
		return nil, f.err
	}
	return f.objects[prefix], nil
}

type savedManifest struct {
	schemaID int16
	m        manifest.Manifest
	etag     string
}

type fakeManifests struct {
	mu             sync.Mutex
	manifests      map[int16]*manifest.Manifest
	etags          map[int16]string
	saves          []savedManifest
	saveErrs       []error // consumed per Save call; nil entry = success
	loadErr        error
	onSaveConflict func(*fakeManifests) // invoked after a non-nil saveErr is consumed
}

func newFakeManifests(ms ...*manifest.Manifest) *fakeManifests {
	f := &fakeManifests{
		manifests: map[int16]*manifest.Manifest{},
		etags:     map[int16]string{},
	}
	for _, m := range ms {
		f.manifests[m.SchemaID] = m
		f.etags[m.SchemaID] = fmt.Sprintf("etag-%d-0", m.SchemaID)
	}
	return f
}

func (f *fakeManifests) Load(_ context.Context, schemaID int16) (*manifest.Manifest, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.loadErr != nil {
		return nil, "", f.loadErr
	}
	m, ok := f.manifests[schemaID]
	if !ok {
		return &manifest.Manifest{SchemaID: schemaID, Files: []manifest.FileEntry{}}, "", nil
	}
	cp := *m
	cp.Files = append([]manifest.FileEntry(nil), m.Files...)
	return &cp, f.etags[schemaID], nil
}

func (f *fakeManifests) Save(_ context.Context, schemaID int16, m *manifest.Manifest, etag string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.saveErrs) > 0 {
		err := f.saveErrs[0]
		f.saveErrs = f.saveErrs[1:]
		if err != nil {
			if f.onSaveConflict != nil {
				f.onSaveConflict(f)
			}
			return "", err
		}
	}
	cp := *m
	cp.Files = append([]manifest.FileEntry(nil), m.Files...)
	f.saves = append(f.saves, savedManifest{schemaID: schemaID, m: cp, etag: etag})
	f.manifests[schemaID] = &cp
	newETag := fmt.Sprintf("etag-%d-%d", schemaID, len(f.saves))
	f.etags[schemaID] = newETag
	return newETag, nil
}

type fakeLocker struct {
	denied   map[int16]bool
	err      error
	locked   []int16
	unlocked []int16
}

func (f *fakeLocker) TryLock(_ context.Context, schemaID int16) (bool, func(), error) {
	if f.err != nil {
		return false, nil, f.err
	}
	if f.denied[schemaID] {
		return false, nil, nil
	}
	f.locked = append(f.locked, schemaID)
	return true, func() { f.unlocked = append(f.unlocked, schemaID) }, nil
}

type fakeDeleter struct {
	deleted []string
	errFor  map[string]error
}

func (f *fakeDeleter) DeleteObject(_ context.Context, key string) error {
	if err := f.errFor[key]; err != nil {
		return err
	}
	f.deleted = append(f.deleted, key)
	return nil
}

type fakeStats struct {
	stats  map[string]compaction.MergeStats // uri -> stats
	errFor map[string]error
	calls  []string
}

func (f *fakeStats) FileStats(_ context.Context, uri string) (compaction.MergeStats, error) {
	f.calls = append(f.calls, uri)
	if err := f.errFor[uri]; err != nil {
		return compaction.MergeStats{}, err
	}
	return f.stats[uri], nil
}

type fakeEnum struct {
	ids []int16
	err error
}

func (f *fakeEnum) SchemaIDs(context.Context) ([]int16, error) {
	return f.ids, f.err
}
