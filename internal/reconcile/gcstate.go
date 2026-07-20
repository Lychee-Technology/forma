package reconcile

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/lychee-technology/forma/internal/manifest"
)

// gcStateDoc is the persisted shape of one schema's GC sighting state.
type gcStateDoc struct {
	// FirstUnlistedMs maps a bucket-relative key to the unix-ms timestamp
	// of the first reconcile run that observed it unlisted.
	FirstUnlistedMs map[string]int64 `json:"first_unlisted_ms"`
}

// ManifestGCStateStore persists GC sighting state next to the schema's
// manifest (at "<manifest path>.gc-state") through the same etag-aware
// store, inheriting its optimistic concurrency incl. the If-None-Match
// create guard. The state object lives under the manifest prefix, so it
// never pollutes the data-prefix listing the reconciler classifies.
type ManifestGCStateStore struct {
	Store    manifest.Store
	Resolver manifest.PathResolver
}

func (s *ManifestGCStateStore) statePath(schemaID int16) (string, error) {
	path, err := s.Resolver.Resolve(schemaID)
	if err != nil {
		return "", fmt.Errorf("resolve schema %d manifest path for gc state: %w", schemaID, err)
	}
	return path + ".gc-state", nil
}

func (s *ManifestGCStateStore) Load(ctx context.Context, schemaID int16) (map[string]int64, string, error) {
	path, err := s.statePath(schemaID)
	if err != nil {
		return nil, "", err
	}
	data, etag, err := s.Store.Load(ctx, path)
	if err != nil {
		if manifest.IsNotFound(err) {
			return map[string]int64{}, "", nil
		}
		return nil, "", fmt.Errorf("load gc state %s: %w", path, err)
	}
	var doc gcStateDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, "", fmt.Errorf("parse gc state %s: %w", path, err)
	}
	if doc.FirstUnlistedMs == nil {
		doc.FirstUnlistedMs = map[string]int64{}
	}
	return doc.FirstUnlistedMs, etag, nil
}

func (s *ManifestGCStateStore) Save(ctx context.Context, schemaID int16, state map[string]int64, etag string) (string, error) {
	path, err := s.statePath(schemaID)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(gcStateDoc{FirstUnlistedMs: state})
	if err != nil {
		return "", fmt.Errorf("encode gc state %s: %w", path, err)
	}
	newETag, err := s.Store.Save(ctx, path, data, etag)
	if err != nil {
		return "", fmt.Errorf("save gc state %s: %w", path, err)
	}
	return newETag, nil
}

var _ GCStateStore = (*ManifestGCStateStore)(nil)
