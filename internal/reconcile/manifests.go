package reconcile

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma/internal/manifest"
)

// ResolverManifestStore implements ManifestStore over a manifest.Store and
// PathResolver. Load has LoadOrCreate semantics — a schema whose manifest
// does not exist yet reconciles as empty instead of erroring (the
// compaction ManifestProvider errors on a missing manifest, which is wrong
// for reconciliation: an empty manifest with live objects is exactly the
// all-orphans case the tool must report).
type ResolverManifestStore struct {
	Store    manifest.Store
	Resolver manifest.PathResolver
}

func (s *ResolverManifestStore) Load(ctx context.Context, schemaID int16) (*manifest.Manifest, string, error) {
	path, err := s.Resolver.Resolve(schemaID)
	if err != nil {
		return nil, "", fmt.Errorf("resolve schema %d manifest path: %w", schemaID, err)
	}
	m, etag, err := manifest.LoadOrCreate(ctx, s.Store, path, schemaID)
	if err != nil {
		return nil, "", fmt.Errorf("load manifest %s: %w", path, err)
	}
	return m, etag, nil
}

func (s *ResolverManifestStore) Save(ctx context.Context, schemaID int16, m *manifest.Manifest, etag string) (string, error) {
	path, err := s.Resolver.Resolve(schemaID)
	if err != nil {
		return "", fmt.Errorf("resolve schema %d manifest path: %w", schemaID, err)
	}
	newETag, err := manifest.Save(ctx, s.Store, path, m, etag)
	if err != nil {
		return "", fmt.Errorf("save manifest %s: %w", path, err)
	}
	return newETag, nil
}

var _ ManifestStore = (*ResolverManifestStore)(nil)
