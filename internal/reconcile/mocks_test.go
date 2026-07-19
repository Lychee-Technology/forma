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
	loads          int
	onSaveConflict func(*fakeManifests) // invoked after a non-nil saveErr is consumed
	onLoad         func(*fakeManifests) // invoked before each Load returns
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
	f.loads++
	if f.onLoad != nil {
		f.onLoad(f)
	}
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
	stats  map[string]compaction.MergeStats // key -> stats
	errFor map[string]error
	calls  []string
	// uncovered maps orphan key -> row ids absent from every listed file.
	// Keys not in the map default to one synthetic uncovered row, so
	// append-path tests behave like a genuine #197 orphan.
	uncovered      map[string][]string
	uncoveredCalls [][]string // listedKeys per UncoveredRowIDs call
	uncoveredErr   map[string]error
}

func (f *fakeStats) FileStats(_ context.Context, key string) (compaction.MergeStats, error) {
	f.calls = append(f.calls, key)
	if err := f.errFor[key]; err != nil {
		return compaction.MergeStats{}, err
	}
	return f.stats[key], nil
}

func (f *fakeStats) UncoveredRowIDs(_ context.Context, key string, listedKeys []string) ([]string, error) {
	f.uncoveredCalls = append(f.uncoveredCalls, listedKeys)
	if err := f.uncoveredErr[key]; err != nil {
		return nil, err
	}
	if rows, ok := f.uncovered[key]; ok {
		return rows, nil
	}
	return []string{"synthetic-uncovered-row"}, nil
}

// fakeLiveRows reports which row ids are missing from entity_main. The
// zero value treats every row as live (the safe-append case).
type fakeLiveRows struct {
	missing map[string]bool // row id -> deleted in Postgres
	err     error
	queried [][]string
}

func (f *fakeLiveRows) MissingLiveRows(_ context.Context, _ int16, rowIDs []string) ([]string, error) {
	f.queried = append(f.queried, rowIDs)
	if f.err != nil {
		return nil, f.err
	}
	var missing []string
	for _, id := range rowIDs {
		if f.missing[id] {
			missing = append(missing, id)
		}
	}
	return missing, nil
}

type fakeEnum struct {
	ids []int16
	err error
}

func (f *fakeEnum) SchemaIDs(context.Context) ([]int16, error) {
	return f.ids, f.err
}
