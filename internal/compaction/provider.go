package compaction

import (
	"context"
	"fmt"
	"io/fs"

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
// manifest surfaces as the store's wrapped manifest.ErrObjectNotFound (unlike
// reconcile's LoadOrCreate semantics, no manifest is synthesised for a
// missing object); the compactor classifies that with manifest.IsNotFound and reports Noop, since
// a never-flushed schema has nothing to compact (#524). The loaded manifest
// then passes manifest.VerifySchemaStamp, the one schema-identity rule: a
// manifest stamped for another schema is a collided or mis-pointed path
// template and is refused with forma.ManifestSchemaMismatchError (#520), and
// one listing entries under schema_id 0 cannot prove which schema owns them
// and is refused with forma.ManifestUnstampedError (#522) — either way
// before the compactor can swap this schema's tiers on it.
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
	if err := manifest.VerifySchemaStamp(m, path, schemaID); err != nil {
		return nil, "", err
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
