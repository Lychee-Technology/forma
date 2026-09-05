package manifest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/lychee-technology/forma"
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
	// Columns records the file's parquet footer schema (column name → DuckDB
	// type), stamped by the writer from a DESCRIBE of the object it just
	// wrote (#256). Readers use it to validate the #189 system-column
	// invariant and build the #255 column union without a footer probe.
	// Nil/absent means the entry predates stamping: readers fall back to
	// probing, so no manifest migration or version bump is needed — field
	// presence is the format version signal.
	Columns map[string]string `json:"columns,omitempty"`
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

// IsNotFound reports whether a Store.Load error means the object does not
// exist. Most S3-compatible stores return an error containing "NoSuchKey"
// or "not found".
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "nosuchkey") || strings.Contains(errStr, "not found") || strings.Contains(errStr, "does not exist")
}

// LoadOrCreate loads an existing manifest or creates a new empty one for the schema.
// Returns the manifest, etag (empty if new), and any error.
func LoadOrCreate(ctx context.Context, st Store, path string, schemaID int16) (*Manifest, string, error) {
	m, etag, err := Load(ctx, st, path)
	if err == nil {
		return m, etag, nil
	}
	if IsNotFound(err) {
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

// SpliceTierFiles replaces every entry of the given tier on the in-memory
// manifest with the provided entries, preserving other tiers in their
// original order. Extracted from ReplaceTierFiles so callers that manage
// their own etag/save cycle — manifest-reconcile's 412-retried init
// promotion (#292) — reuse the exact replacement semantics.
func SpliceTierFiles(m *Manifest, tier string, entries []FileEntry) {
	kept := make([]FileEntry, 0, len(m.Files)+len(entries))
	target := strings.ToLower(tier)
	for _, f := range m.Files {
		if strings.ToLower(f.Tier) != target {
			kept = append(kept, f)
		}
	}
	m.Files = append(kept, entries...)
}

// ReplaceTierFiles replaces every manifest entry of the given tier with the
// provided entries, preserving entries of other tiers in their original
// order, as one load-splice-save under the loaded etag. It is the wholesale
// tier replacement a full re-export needs (#176): stale entries from earlier
// runs and historical duplicates must not survive, and compaction-promoted
// base entries are replaced too — their S3 objects remain for glob-based
// readers, and object-level reconciliation is #203. cdc-init's own publish
// (internal/cdc/init_publish.go) splices base and delta itself so it can
// retry a confirmed 412 and confirm an ambiguous save; this helper remains
// for single-tier callers and loads through LoadOrCreateForSchema, so a
// manifest stamped for another schema is never rewritten under this one.
func ReplaceTierFiles(ctx context.Context, st Store, path string, schemaID int16, tier string, entries []FileEntry) error {
	m, etag, err := LoadOrCreateForSchema(ctx, st, path, schemaID)
	if err != nil {
		return fmt.Errorf("load manifest: %w", err)
	}
	SpliceTierFiles(m, tier, entries)
	if _, err := Save(ctx, st, path, m, etag); err != nil {
		return fmt.Errorf("save manifest: %w", err)
	}
	return nil
}

// LoadOrCreateForSchema is LoadOrCreate plus the schema-identity check every
// per-schema manifest consumer needs: a loaded manifest whose stamped
// SchemaID names a different schema is a collided or mis-pointed path
// template, and it is rejected with forma.ManifestSchemaMismatchError
// instead of being read or written as the requested schema's. A zero stamp
// is treated as unstamped rather than as schema 0: schema IDs are always
// positive, and rejecting zero would break deployments still holding
// manifests written before the field existed. Config validation samples two
// schema IDs, which catches a collapsed template but cannot prove
// injectivity over the whole domain — this is the enforcement.
func LoadOrCreateForSchema(ctx context.Context, st Store, path string, schemaID int16) (*Manifest, string, error) {
	m, etag, err := LoadOrCreate(ctx, st, path, schemaID)
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
