package manifest

import (
	"context"
	"encoding/json"
	"errors"
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

// ErrObjectNotFound is the sentinel every Store.Load returns (wrapped) when the
// object is confirmed absent. Each Store translates its backend's typed
// signal into it (S3: NoSuchKey; FS: fs.ErrNotExist).
var ErrObjectNotFound = errors.New("manifest object not found")

// IsNotFound reports whether a Store.Load error means the object is
// CONFIRMED absent, i.e. the Store wrapped ErrObjectNotFound. It never inspects
// message text: a NoSuchBucket ("The specified bucket does not exist") or
// any other store failure whose wording resembles a missing key used to
// classify as "manifest absent", which turned LoadOrCreate into an empty
// manifest and silently sent federated reads to the glob fallback (#464).
// Those now surface to the caller.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrObjectNotFound)
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
// Uses optimistic locking via etag to handle concurrent updates. It loads
// through LoadOrCreateForSchema, so a manifest stamped for another schema —
// a collided or mis-pointed path template — is refused with
// forma.ManifestSchemaMismatchError instead of receiving this schema's
// entry (#520).
func AppendFile(ctx context.Context, st Store, path string, schemaID int16, entry FileEntry) error {
	m, etag, err := LoadOrCreateForSchema(ctx, st, path, schemaID)
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
// Uses optimistic locking via etag to handle concurrent updates. Like
// AppendFile it loads through LoadOrCreateForSchema, so a manifest stamped
// for another schema is refused with forma.ManifestSchemaMismatchError
// (#520).
func AppendFiles(ctx context.Context, st Store, path string, schemaID int16, entries []FileEntry) error {
	if len(entries) == 0 {
		return nil
	}
	m, etag, err := LoadOrCreateForSchema(ctx, st, path, schemaID)
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
// per-schema manifest consumer needs, see VerifySchemaStamp: a loaded
// manifest whose stamped SchemaID names a different schema is a collided or
// mis-pointed path template and is rejected with
// forma.ManifestSchemaMismatchError; one that lists entries under schema_id
// 0 cannot prove which schema owns them and is rejected with
// forma.ManifestUnstampedError (#522). An empty zero-stamped manifest loads
// stamped for the requested schema in memory, so the caller's next save
// persists the stamp under the loaded etag.
//
// Config validation samples two schema IDs, which catches a collapsed
// template but cannot prove injectivity over the whole domain — this check
// is the enforcement.
func LoadOrCreateForSchema(ctx context.Context, st Store, path string, schemaID int16) (*Manifest, string, error) {
	m, etag, err := LoadOrCreate(ctx, st, path, schemaID)
	if err != nil {
		return nil, "", err
	}
	if err := VerifySchemaStamp(m, path, schemaID); err != nil {
		return nil, "", err
	}
	return m, etag, nil
}

// VerifySchemaStamp is the single schema-identity rule for a loaded manifest
// (#520, #522). A manifest addresses one schema by path convention alone and
// nothing downstream re-checks it — the parquet scan does not filter rows by
// schema and the projection stamps whatever it scans as the requested
// schema — so the stamp inside the object is the only proof of ownership:
//
//   - stamped for schemaID: accepted;
//   - stamped for another schema: forma.ManifestSchemaMismatchError;
//   - stamped 0 with entries: forma.ManifestUnstampedError — no Forma writer
//     has ever produced a zero stamp (the field has been set by LoadOrCreate
//     since this package's first commit), so the object is hand-made and its
//     entries may belong to any schema a colliding template maps here;
//   - stamped 0 and empty: nothing another schema could own, so the manifest
//     is stamped for schemaID in place and accepted; the caller's next save
//     persists the stamp and every later load is checked.
//
// The requested schemaID must itself be positive (#536): schema IDs are
// always positive, so a request for schema 0 is never a schema and would
// otherwise satisfy the equality branch against a zero-stamped manifest and
// admit its entries. A non-positive request is refused before any stamp is
// compared or written, as a plain error — it is an invariant violation by the
// caller (an unguarded enumerator or a hand-inserted registry row), not a
// property of the manifest.
//
// Callers that load without LoadOrCreate semantics (the compactor) apply it
// to the manifest they loaded; LoadOrCreateForSchema applies it for everyone
// else.
func VerifySchemaStamp(m *Manifest, path string, schemaID int16) error {
	if schemaID <= 0 {
		return fmt.Errorf("manifest %s: requested schema id %d is not a schema (schema IDs are positive); refusing to verify the stamp", path, schemaID)
	}
	if m.SchemaID == schemaID {
		return nil
	}
	if m.SchemaID != 0 {
		return &forma.ManifestSchemaMismatchError{
			RequestedSchemaID: schemaID,
			ManifestSchemaID:  m.SchemaID,
			Path:              path,
		}
	}
	if len(m.Files) > 0 {
		return &forma.ManifestUnstampedError{
			RequestedSchemaID: schemaID,
			Path:              path,
			Entries:           len(m.Files),
		}
	}
	m.SchemaID = schemaID
	return nil
}
