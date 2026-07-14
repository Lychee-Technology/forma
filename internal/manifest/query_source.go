package manifest

import (
	"context"
	"fmt"
	"strings"
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
// or the Fallback glob when the schema has no manifest entries yet. The
// manifest format accepts both bucket-relative keys (what the CDC writers
// produce) and absolute s3:// URIs — absolute entries pass through
// unchanged instead of being double-prefixed (#249 review).
func (s *QuerySource) Paths(ctx context.Context, schemaID int16) ([]string, error) {
	m, err := s.load(ctx, schemaID)
	if err != nil {
		return nil, fmt.Errorf("resolve manifest paths: %w", err)
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
		if strings.HasPrefix(f.Path, "s3://") {
			uris = append(uris, f.Path)
			continue
		}
		uris = append(uris, fmt.Sprintf("s3://%s/%s", s.Bucket, f.Path))
	}
	return uris, nil
}

// MissingIn probes the given scanned URIs and returns the bucket-relative
// keys absent from storage. It deliberately probes the exact set the failed
// scan used rather than reloading the manifest: a concurrent
// flush/compaction could otherwise swap in a newer snapshot that hides the
// lost key or lists an unrelated one. Glob entries and URIs outside this
// source's bucket are skipped — their absence cannot be proven with the
// configured probe, and an unprovable key must not fabricate inconsistency.
func (s *QuerySource) MissingIn(ctx context.Context, scanned []string) ([]string, error) {
	if s.Exists == nil {
		return nil, nil
	}
	prefix := "s3://" + s.Bucket + "/"
	var missing []string
	for _, uri := range scanned {
		if strings.ContainsAny(uri, "*?[") || !strings.HasPrefix(uri, prefix) {
			continue
		}
		key := strings.TrimPrefix(uri, prefix)
		ok, err := s.Exists(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("probe parquet object %s: %w", key, err)
		}
		if !ok {
			missing = append(missing, key)
		}
	}
	return missing, nil
}
