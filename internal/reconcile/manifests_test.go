package reconcile

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/manifest"
)

// memManifestStore is an in-memory manifest.Store: not-found until first
// save, etag bumps per save (mirrors internal/cdc/init_test.go).
type memManifestStore struct {
	data  map[string][]byte
	etags map[string]string
	saves int
}

func newMemManifestStore() *memManifestStore {
	return &memManifestStore{data: map[string][]byte{}, etags: map[string]string{}}
}

func (s *memManifestStore) Load(_ context.Context, path string) ([]byte, string, error) {
	b, ok := s.data[path]
	if !ok {
		return nil, "", fmt.Errorf("mem store %s: %w", path, manifest.ErrObjectNotFound)
	}
	return b, s.etags[path], nil
}

func (s *memManifestStore) Save(_ context.Context, path string, data []byte, _ string) (string, error) {
	s.saves++
	s.data[path] = data
	etag := fmt.Sprintf("v%d", s.saves)
	s.etags[path] = etag
	return etag, nil
}

func TestResolverManifestStore_MissingManifestLoadsEmpty(t *testing.T) {
	store := &ResolverManifestStore{
		Store:    newMemManifestStore(),
		Resolver: manifest.PathResolver{PathTemplate: "manifest/{{.SchemaID}}.json"},
	}
	m, etag, err := store.Load(context.Background(), 7)
	require.NoError(t, err, "a schema without a manifest must reconcile as empty")
	require.Equal(t, int16(7), m.SchemaID)
	require.Empty(t, m.Files)
	require.Empty(t, etag)
}

func TestResolverManifestStore_SchemaIDMismatchFails(t *testing.T) {
	// #481: a fixed-file --manifest-template (no {{.SchemaID}}) resolves
	// every schema to the same manifest. Loading it for the wrong schema
	// must fail the schema as a tool error before any classification.
	mem := newMemManifestStore()
	store := &ResolverManifestStore{
		Store:    mem,
		Resolver: manifest.PathResolver{PathTemplate: "manifest/data.json"},
	}
	ctx := context.Background()

	m, etag, err := store.Load(ctx, 7)
	require.NoError(t, err)
	m.Files = append(m.Files, manifest.FileEntry{Tier: "delta", Path: "data/7/a.parquet"})
	_, err = store.Save(ctx, 7, m, etag)
	require.NoError(t, err)

	_, _, err = store.Load(ctx, 9)
	require.Error(t, err, "schema 9 must not load schema 7's manifest")
	require.Contains(t, err.Error(), "schema 7")
	require.Contains(t, err.Error(), "schema 9")
	// #520: the check is the shared manifest.LoadOrCreateForSchema; the
	// typed error rides under the reconcile wording so callers can match it.
	require.ErrorIs(t, err, forma.ErrManifestSchemaMismatch)
	require.Contains(t, err.Error(), "claims schema 7, not requested schema 9")
	require.Contains(t, err.Error(), "mis-pointed --manifest-prefix/--manifest-template")
}

// #522: a zero stamp proves nothing about ownership, and no Forma writer
// has ever produced one. A zero-stamped manifest that lists entries is
// refused for every schema — before any classification, so the tool never
// diffs a schema's objects against entries it cannot attribute — as the
// schema-mismatch species that names the remedy (set schema_id on the
// object).
func TestResolverManifestStore_UnstampedManifestWithEntriesFails(t *testing.T) {
	mem := newMemManifestStore()
	mem.data["manifest/7.json"] = []byte(`{"files":[{"tier":"delta","path":"data/7/a.parquet"}]}`)
	mem.etags["manifest/7.json"] = "v0"
	store := &ResolverManifestStore{
		Store:    mem,
		Resolver: manifest.PathResolver{PathTemplate: "manifest/{{.SchemaID}}.json"},
	}
	_, _, err := store.Load(context.Background(), 7)
	require.ErrorIs(t, err, forma.ErrManifestUnstamped)
	require.ErrorIs(t, err, forma.ErrManifestSchemaMismatch)
	require.Contains(t, err.Error(), "manifest/7.json")
	require.Contains(t, err.Error(), "1 entries listed under schema_id 0")
	require.Zero(t, mem.saves, "a refused load must not save")
}

// An empty zero-stamped manifest has nothing another schema could own: it
// loads stamped for the requested schema in memory, under the stored etag,
// so the tool's next save persists the stamp.
func TestResolverManifestStore_EmptyUnstampedManifestLoadsStamped(t *testing.T) {
	mem := newMemManifestStore()
	mem.data["manifest/7.json"] = []byte(`{"files":[]}`)
	mem.etags["manifest/7.json"] = "v0"
	store := &ResolverManifestStore{
		Store:    mem,
		Resolver: manifest.PathResolver{PathTemplate: "manifest/{{.SchemaID}}.json"},
	}
	m, etag, err := store.Load(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, int16(7), m.SchemaID)
	require.Equal(t, "v0", etag)
	require.Empty(t, m.Files)
	require.Zero(t, mem.saves)
}

// The end-to-end shape from #522: a --manifest-template that collides only
// at schema IDs the two-probe check never renders, a zero-stamped manifest
// at the shared path listing schema 3's tiers, and a --repair run for
// schema 4 with an orphan to adopt. The schema fails at load, nothing is
// spliced under the wrong identity, and nothing is saved.
func TestReconcile_RepairRefusesUnstampedSharedManifest(t *testing.T) {
	const colliding = "{{if lt .SchemaID 3}}manifest/{{.SchemaID}}.json{{else}}manifest/shared.json{{end}}"
	require.NoError(t, forma.ValidateManifestPathTemplate("manifest-template", colliding),
		"the two-probe check cannot see a collision at schema IDs 3 and 4")

	mem := newMemManifestStore()
	mem.data["manifest/shared.json"] = []byte(`{"files":[{"tier":"base","path":"data/3/base-` + uuidA + `.parquet"}]}`)
	mem.etags["manifest/shared.json"] = "v0"
	before := append([]byte(nil), mem.data["manifest/shared.json"]...)

	orphan := "data/4/" + uuidB + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{"data/4/": {oldObject(orphan)}}}
	r := &Reconciler{
		Lister:     lister,
		Deleter:    &fakeDeleter{},
		Manifests:  &ResolverManifestStore{Store: mem, Resolver: manifest.PathResolver{PathTemplate: colliding}},
		Stats:      &fakeStats{uncovered: map[string][]compaction.UncoveredRow{orphan: {{RowID: uuidB}}}},
		LiveRows:   &fakeLiveRows{},
		Locker:     &fakeLocker{},
		Schemas:    &fakeEnum{ids: []int16{4}},
		GCStates:   newFakeGCState(),
		Now:        testClock,
		Bucket:     "bkt",
		DataPrefix: "data",
		Logger:     zap.NewNop(),
		Opts:       Options{Repair: true, MaxETagRetries: 3},
	}

	report, err := r.Run(context.Background())
	require.NoError(t, err, "a per-schema load failure is reported, not returned")
	require.Len(t, report.Schemas, 1)
	s := report.Schemas[0]
	require.ErrorIs(t, s.Err, forma.ErrManifestUnstamped, "schema 4 must fail at manifest load")
	require.ErrorIs(t, s.Err, forma.ErrManifestSchemaMismatch)
	require.Contains(t, s.Err.Error(), "manifest/shared.json")
	require.Empty(t, s.Repaired, "nothing may be spliced under an unproven identity")
	require.Zero(t, mem.saves, "nothing may be saved")
	require.Equal(t, before, mem.data["manifest/shared.json"], "the shared manifest is byte-identical")
}

func TestResolverManifestStore_SaveRoundTrip(t *testing.T) {
	store := &ResolverManifestStore{
		Store:    newMemManifestStore(),
		Resolver: manifest.PathResolver{PathTemplate: "manifest/{{.SchemaID}}.json"},
	}
	ctx := context.Background()

	m, etag, err := store.Load(ctx, 7)
	require.NoError(t, err)
	m.Files = append(m.Files, manifest.FileEntry{Tier: "delta", Path: "data/7/a.parquet"})
	_, err = store.Save(ctx, 7, m, etag)
	require.NoError(t, err)

	reloaded, etag2, err := store.Load(ctx, 7)
	require.NoError(t, err)
	require.NotEmpty(t, etag2)
	require.Len(t, reloaded.Files, 1)
	require.Equal(t, "data/7/a.parquet", reloaded.Files[0].Path)
	require.Greater(t, reloaded.Version, int64(0), "manifest.Save must bump the version")
}
