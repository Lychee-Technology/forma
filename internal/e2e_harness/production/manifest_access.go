package production

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/lychee-technology/forma/internal/reconcile"
)

// The test files of this package reach the published file inventory only
// through this file, publication_state.go, publication_faults.go and the
// harness sources that already wrap internal/manifest (cdc.go, engine.go,
// init_keep_delta.go); no e2e scenario imports the package (#901). A test
// observes the published files (loadSchemaManifest, TierEntries and the
// entries they answer), compares publications through publication_state.go
// and stages a fault in the inventory through publication_faults.go. It never
// reads the V1 manifest's version, ETag or bytes, and never saves a manifest
// itself: Protocol V2 replaces the mutable V1 manifest with immutable
// generations behind a publication record (docs/publication-protocol-v2.md),
// and #896 then rewrites these bodies instead of every test that asserts
// over the inventory. The ETag load and save below exist for the fault
// helpers alone.

// Manifest is a schema's published file inventory, as the flush, init,
// compaction and reconcile paths leave it.
type Manifest = manifest.Manifest

// ManifestEntry is one parquet object a Manifest lists.
type ManifestEntry = manifest.FileEntry

// TierEntries returns the entries m lists on tier ("base" or "delta").
func TierEntries(m *Manifest, tier string) []ManifestEntry {
	return manifest.FilterByTier(m, tier)
}

// loadSchemaManifest loads and parses the schema's current manifest, failing
// the test when it does not exist.
func loadSchemaManifest(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) *Manifest {
	t.Helper()
	manifests, err := env.loadManifests(ctx)
	if err != nil {
		t.Fatalf("load manifests: %v", err)
	}
	m := manifests[schema.ID]
	if m == nil {
		t.Fatalf("schema %d (%s) has no manifest", schema.ID, schema.Name)
	}
	return m
}

// loadManifestWithETag loads the schema's manifest together with the ETag a
// later saveManifestWithETag must present, for the helpers of
// publication_faults.go that rewrite the published inventory to stage a
// fault (a stripped entry, a lost publish).
func loadManifestWithETag(t *testing.T, ctx context.Context, env *Env, schema SchemaRef) (*Manifest, string) {
	t.Helper()
	store, resolver := env.manifestAccess()
	path, err := resolver.Resolve(schema.ID)
	if err != nil {
		t.Fatalf("resolve manifest path: %v", err)
	}
	m, etag, err := manifest.Load(ctx, store, path)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	return m, etag
}

// saveManifestWithETag publishes m as the schema's manifest under the ETag
// compare-and-set loadManifestWithETag returned.
func saveManifestWithETag(t *testing.T, ctx context.Context, env *Env, schema SchemaRef, m *Manifest, etag string) {
	t.Helper()
	store, resolver := env.manifestAccess()
	path, err := resolver.Resolve(schema.ID)
	if err != nil {
		t.Fatalf("resolve manifest path: %v", err)
	}
	if _, err := manifest.Save(ctx, store, path, m, etag); err != nil {
		t.Fatalf("save manifest: %v", err)
	}
}

// replaceManifestTier republishes the schema's manifest with entries as the
// whole of tier, leaving the other tier's entries as they are.
func replaceManifestTier(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, tier string, entries []ManifestEntry) {
	t.Helper()
	store, resolver := env.manifestAccess()
	path, err := resolver.Resolve(schema.ID)
	if err != nil {
		t.Fatalf("resolve manifest path: %v", err)
	}
	if err := manifest.ReplaceTierFiles(ctx, store, path, schema.ID, tier, entries); err != nil {
		t.Fatalf("replace %s entries: %v", tier, err)
	}
}

// reconcileStores answers the manifest and GC-state stores the
// manifest-reconcile tool wires, over the store the other harness paths use.
func (e *Env) reconcileStores() (*reconcile.ResolverManifestStore, *reconcile.ManifestGCStateStore) {
	store, resolver := e.manifestAccess()
	return &reconcile.ResolverManifestStore{Store: store, Resolver: resolver},
		&reconcile.ManifestGCStateStore{Store: store, Resolver: resolver}
}
