package reconcile

import (
	"context"
	"errors"
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/manifest"
)

// ResolverManifestStore implements ManifestStore over a manifest.Store and
// PathResolver. Load has LoadOrCreate semantics — a schema whose manifest
// does not exist yet reconciles as empty instead of erroring (the
// compaction ManifestProvider errors on a missing manifest, which is wrong
// for reconciliation: an empty manifest with live objects is exactly the
// all-orphans case the tool must report). Load also rejects a manifest
// whose stored SchemaID names a different schema (#481), through the shared
// manifest.LoadOrCreateForSchema check (#520).
type ResolverManifestStore struct {
	Store    manifest.Store
	Resolver manifest.PathResolver
}

func (s *ResolverManifestStore) Load(ctx context.Context, schemaID int16) (*manifest.Manifest, string, error) {
	path, err := s.Resolver.Resolve(schemaID)
	if err != nil {
		return nil, "", fmt.Errorf("resolve schema %d manifest path: %w", schemaID, err)
	}
	// #481: a loaded manifest claiming a different schema is a mis-pointed
	// template (e.g. a fixed-file --manifest-template without
	// {{.SchemaID}}) — fail the schema before any classification rather
	// than diff its data against a foreign manifest. The check is the
	// shared manifest.LoadOrCreateForSchema (#520); the reconcile wording
	// is kept on top because the report quotes it. SchemaID 0 is
	// tolerated: manifests written before the field was stamped unmarshal
	// to 0, and the #481 in-prefix GC guard still covers them.
	m, etag, err := manifest.LoadOrCreateForSchema(ctx, s.Store, path, schemaID)
	var mismatch *forma.ManifestSchemaMismatchError
	if errors.As(err, &mismatch) {
		return nil, "", fmt.Errorf("manifest %s claims schema %d, not requested schema %d — mis-pointed --manifest-prefix/--manifest-template (fixed-file or cross-schema): %w",
			path, mismatch.ManifestSchemaID, schemaID, err)
	}
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
