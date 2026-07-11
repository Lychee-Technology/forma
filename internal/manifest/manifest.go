package manifest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// FileEntry describes a single parquet file tracked by the manifest.
// Tier is typically "base" or "delta".
type FileEntry struct {
	Tier       string `json:"tier"`
	Path       string `json:"path"`
	RowIDMin   string `json:"row_id_min"`
	RowIDMax   string `json:"row_id_max"`
	CreatedMin int64  `json:"created_min"`
	CreatedMax int64  `json:"created_max"`
	SizeBytes  int64  `json:"size_bytes"`
	RowCount   int64  `json:"row_count"`
	Checksum   string `json:"checksum,omitempty"`
}

// Manifest holds per-schema parquet inventory.
type Manifest struct {
	SchemaID    int16       `json:"schema_id"`
	Version     int64       `json:"version"`
	UpdatedAtMs int64       `json:"updated_at_ms"`
	Files       []FileEntry `json:"files"`
}

// Store abstracts load/save operations (could be S3 or local FS).
type Store interface {
	Load(ctx context.Context, path string) (data []byte, etag string, err error)
	Save(ctx context.Context, path string, data []byte, etag string) (newETag string, err error)
}

// Load reads manifest from store.
func Load(ctx context.Context, st Store, path string) (*Manifest, string, error) {
	b, etag, err := st.Load(ctx, path)
	if err != nil {
		return nil, "", err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, "", fmt.Errorf("manifest unmarshal: %w", err)
	}
	return &m, etag, nil
}

// Save writes manifest with updated timestamp and optional optimistic etag.
func Save(ctx context.Context, st Store, path string, m *Manifest, etag string) (string, error) {
	if m == nil {
		return "", fmt.Errorf("manifest nil")
	}
	nextUpdatedAtMs := time.Now().UnixMilli()
	if nextUpdatedAtMs <= m.UpdatedAtMs {
		nextUpdatedAtMs = m.UpdatedAtMs + 1
	}
	m.UpdatedAtMs = nextUpdatedAtMs
	if m.Version == 0 {
		m.Version = 1
	} else {
		m.Version++
	}
	payload, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return "", fmt.Errorf("manifest marshal: %w", err)
	}
	return st.Save(ctx, path, payload, etag)
}

// ListPaths returns paths for the given tier.
func ListPaths(m *Manifest, tier string) []string {
	if m == nil {
		return nil
	}
	tier = strings.ToLower(tier)
	paths := []string{}
	for _, f := range m.Files {
		if strings.ToLower(f.Tier) == tier {
			paths = append(paths, f.Path)
		}
	}
	return paths
}

// FilterBySchema returns files matching the tier (use "" for all tiers).
func FilterByTier(m *Manifest, tier string) []FileEntry {
	if m == nil {
		return nil
	}
	if tier == "" {
		return m.Files
	}
	tier = strings.ToLower(tier)
	out := []FileEntry{}
	for _, f := range m.Files {
		if strings.ToLower(f.Tier) == tier {
			out = append(out, f)
		}
	}
	return out
}

// Parse decodes manifest JSON bytes.
func Parse(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// Decode reads from reader.
func Decode(r io.Reader) (*Manifest, error) {
	var m Manifest
	dec := json.NewDecoder(r)
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	return &m, nil
}

// LoadOrCreate loads an existing manifest or creates a new empty one for the schema.
// Returns the manifest, etag (empty if new), and any error.
func LoadOrCreate(ctx context.Context, st Store, path string, schemaID int16) (*Manifest, string, error) {
	m, etag, err := Load(ctx, st, path)
	if err == nil {
		return m, etag, nil
	}
	// If file doesn't exist, create new manifest
	// Most S3-compatible stores return an error containing "NoSuchKey" or "not found"
	errStr := strings.ToLower(err.Error())
	if strings.Contains(errStr, "nosuchkey") || strings.Contains(errStr, "not found") || strings.Contains(errStr, "does not exist") {
		return &Manifest{
			SchemaID: schemaID,
			Version:  0,
			Files:    []FileEntry{},
		}, "", nil
	}
	return nil, "", err
}

// AppendFile adds a FileEntry to the manifest and saves it.
// Uses optimistic locking via etag to handle concurrent updates.
func AppendFile(ctx context.Context, st Store, path string, schemaID int16, entry FileEntry) error {
	m, etag, err := LoadOrCreate(ctx, st, path, schemaID)
	if err != nil {
		return fmt.Errorf("load manifest: %w", err)
	}
	m.Files = append(m.Files, entry)
	_, err = Save(ctx, st, path, m, etag)
	if err != nil {
		return fmt.Errorf("save manifest: %w", err)
	}
	return nil
}

// AppendFiles adds multiple FileEntry items to the manifest and saves it.
// Uses optimistic locking via etag to handle concurrent updates.
func AppendFiles(ctx context.Context, st Store, path string, schemaID int16, entries []FileEntry) error {
	if len(entries) == 0 {
		return nil
	}
	m, etag, err := LoadOrCreate(ctx, st, path, schemaID)
	if err != nil {
		return fmt.Errorf("load manifest: %w", err)
	}
	m.Files = append(m.Files, entries...)
	_, err = Save(ctx, st, path, m, etag)
	if err != nil {
		return fmt.Errorf("save manifest: %w", err)
	}
	return nil
}

// UpsertFiles adds entries to the manifest, replacing any existing entry
// with the same Path in place instead of appending a duplicate. Init reruns
// re-export the same row ranges to the same deterministic keys, so append
// semantics would double every base entry on each rerun (#176). Uses
// optimistic locking via etag to handle concurrent updates.
func UpsertFiles(ctx context.Context, st Store, path string, schemaID int16, entries []FileEntry) error {
	if len(entries) == 0 {
		return nil
	}
	m, etag, err := LoadOrCreate(ctx, st, path, schemaID)
	if err != nil {
		return fmt.Errorf("load manifest: %w", err)
	}
	index := make(map[string]int, len(m.Files))
	for i, f := range m.Files {
		index[f.Path] = i
	}
	for _, entry := range entries {
		if i, ok := index[entry.Path]; ok {
			m.Files[i] = entry
			continue
		}
		index[entry.Path] = len(m.Files)
		m.Files = append(m.Files, entry)
	}
	if _, err := Save(ctx, st, path, m, etag); err != nil {
		return fmt.Errorf("save manifest: %w", err)
	}
	return nil
}
