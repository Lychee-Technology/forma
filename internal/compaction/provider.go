package compaction

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifest"
)

// ManifestProvider adapts manifest.Store + resolver to FileProvider.
type ManifestProvider struct {
	Store    manifest.Store
	Resolver manifest.PathResolver
}

// NewManifestProvider creates a ManifestProvider from a ManifestConfig and Store.
// Use this to wire up the provider from configuration.
func NewManifestProvider(cfg cdc.ManifestConfig, store manifest.Store) *ManifestProvider {
	return &ManifestProvider{
		Store: store,
		Resolver: manifest.PathResolver{
			Prefix:       cfg.Prefix,
			PathTemplate: cfg.PathTemplate,
		},
	}
}

// NewS3ManifestProvider creates a ManifestProvider backed by S3. It accepts
// any manifest.S3Client (Get/Put) so tests can decorate the real client.
func NewS3ManifestProvider(cfg cdc.ManifestConfig, s3Client manifest.S3Client) *ManifestProvider {
	store := &manifest.S3Store{
		Client: s3Client,
		Bucket: cfg.Bucket,
	}
	return NewManifestProvider(cfg, store)
}

// NewFSManifestProvider creates a ManifestProvider backed by local filesystem.
// Useful for testing and local development.
func NewFSManifestProvider(cfg cdc.ManifestConfig, rootFS fs.FS) *ManifestProvider {
	store := &manifest.FSStore{
		Root: rootFS,
	}
	return NewManifestProvider(cfg, store)
}

// LoadManifest resolves the schema's manifest path and loads it. A missing
// manifest is an error here (unlike reconcile's LoadOrCreate semantics): the
// compactor has nothing to compact without one. A loaded manifest stamped for
// another schema is a collided or mis-pointed path template and is refused
// with forma.ManifestSchemaMismatchError before the compactor can swap this
// schema's tiers on it (#520). The zero stamp keeps the read path's
// lenience, see manifest.LoadOrCreateForSchema.
func (p *ManifestProvider) LoadManifest(ctx context.Context, schemaID int16) (*manifest.Manifest, string, error) {
	if p == nil || p.Store == nil {
		return nil, "", fmt.Errorf("manifest provider not configured")
	}
	path, err := p.Resolver.Resolve(schemaID)
	if err != nil {
		return nil, "", err
	}
	m, etag, err := manifest.Load(ctx, p.Store, path)
	if err != nil {
		return nil, "", err
	}
	if m.SchemaID != 0 && m.SchemaID != schemaID {
		return nil, "", &forma.ManifestSchemaMismatchError{
			RequestedSchemaID: schemaID,
			ManifestSchemaID:  m.SchemaID,
			Path:              path,
		}
	}
	return m, etag, nil
}

func (p *ManifestProvider) SaveManifest(ctx context.Context, schemaID int16, m *manifest.Manifest, etag string) (string, error) {
	if p == nil || p.Store == nil {
		return "", fmt.Errorf("manifest provider not configured")
	}
	path, err := p.Resolver.Resolve(schemaID)
	if err != nil {
		return "", err
	}
	return manifest.Save(ctx, p.Store, path, m, etag)
}
