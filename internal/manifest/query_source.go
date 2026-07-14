package manifest

import (
	"context"
	"fmt"
)

// QuerySource resolves a schema's parquet object set for federated reads
// (#187): Paths returns the manifest-listed objects as s3:// URIs and
// MissingKeys reports listed keys absent from storage, so the read path can
// classify a failed scan as manifest inconsistency by storage state instead
// of driver message text.
//
// Direction contract: manifest ⊆ live objects is required — a listed key
// missing from storage is loud (the read fails and classifies). Extra
// unlisted objects are tolerated and invisible to reads; object-level
// reconciliation is #203's scope. That one-directional contract is what
// makes this safe against the CDC write windows: both the flusher and init
// copy the final object to storage before listing it in the manifest, so a
// listed-but-absent key can only mean loss, never in-flight publication.
type QuerySource struct {
	Store    Store
	Resolver PathResolver
	// Bucket prefixes manifest FileEntry paths (bucket-relative keys) into
	// full s3:// URIs for DuckDB.
	Bucket string
	// Exists probes one bucket-relative key for existence (e.g. HeadObject).
	// Nil disables missing-key classification (MissingKeys reports none).
	Exists func(ctx context.Context, key string) (bool, error)
	// Fallback, when set, supplies the path set for schemas whose manifest
	// is missing or empty — typically the legacy per-schema glob, preserving
	// pre-manifest read behavior for never-flushed schemas.
	Fallback func(schemaID int16) string
}

func (s *QuerySource) load(ctx context.Context, schemaID int16) (*Manifest, error) {
	path, err := s.Resolver.Resolve(schemaID)
	if err != nil {
		return nil, fmt.Errorf("resolve manifest path for schema %d: %w", schemaID, err)
	}
	m, _, err := LoadOrCreate(ctx, s.Store, path, schemaID)
	if err != nil {
		return nil, fmt.Errorf("load manifest for schema %d: %w", schemaID, err)
	}
	return m, nil
}

// Paths returns the schema's manifest-listed parquet objects as s3:// URIs,
// or the Fallback glob when the schema has no manifest entries yet.
func (s *QuerySource) Paths(ctx context.Context, schemaID int16) ([]string, error) {
	m, err := s.load(ctx, schemaID)
	if err != nil {
		return nil, err
	}
	if len(m.Files) == 0 {
		if s.Fallback != nil {
			if glob := s.Fallback(schemaID); glob != "" {
				return []string{glob}, nil
			}
		}
		return nil, nil
	}
	uris := make([]string, 0, len(m.Files))
	for _, f := range m.Files {
		uris = append(uris, fmt.Sprintf("s3://%s/%s", s.Bucket, f.Path))
	}
	return uris, nil
}

// MissingKeys returns the manifest-listed keys that no longer exist in
// storage. Called on the read-error path only.
func (s *QuerySource) MissingKeys(ctx context.Context, schemaID int16) ([]string, error) {
	if s.Exists == nil {
		return nil, nil
	}
	m, err := s.load(ctx, schemaID)
	if err != nil {
		return nil, err
	}
	var missing []string
	for _, f := range m.Files {
		ok, err := s.Exists(ctx, f.Path)
		if err != nil {
			return nil, fmt.Errorf("probe parquet object %s: %w", f.Path, err)
		}
		if !ok {
			missing = append(missing, f.Path)
		}
	}
	return missing, nil
}
