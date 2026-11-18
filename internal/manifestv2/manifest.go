// Package manifestv2 produces and fetches the immutable manifest objects of
// the V2 publication protocol (docs/publication-protocol-v2.md, "Immutable
// manifest generations"): the document, its deterministic serialization and
// digest, the file list of each generation kind, the object keys, a
// write-once object store and a verifying process-wide cache.
//
// Nothing outside this package calls it until the writers and the reader move
// onto the publication record together (#896): the guard in
// internal/publication (TestOnlyInitDBDDLReachesThePublicationTables) refuses
// a production import of it from anywhere else.
package manifestv2

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/publication"
)

// Manifest is one generation's manifest document. Every field is required;
// InputGeneration is present on a compact manifest only. A Manifest a Cache
// answers is shared by every caller and must not be modified.
type Manifest struct {
	Format           string           `json:"format"`
	SchemaID         int16            `json:"schema_id"`
	Epoch            int64            `json:"epoch"`
	Generation       int64            `json:"generation"`
	ParentGeneration int64            `json:"parent_generation"`
	Lineage          uuid.UUID        `json:"lineage"`
	Token            uuid.UUID        `json:"token"`
	Kind             publication.Kind `json:"kind"`
	// InputGeneration names the generation whose files a compaction merged.
	// After a rebase over flushes it differs from ParentGeneration.
	InputGeneration int64  `json:"input_generation,omitempty"`
	Files           []File `json:"files"`
}

// File is one parquet object a generation lists.
type File struct {
	Tier      Tier   `json:"tier"`
	Key       string `json:"key"`
	SizeBytes int64  `json:"size_bytes"`
	// SHA256 is the object's content digest, as 64 lowercase hex digits.
	// Readers do not recompute it per scan; verification passes do.
	SHA256   string    `json:"sha256"`
	RowCount int64     `json:"row_count"`
	RowIDMin uuid.UUID `json:"row_id_min"`
	RowIDMax uuid.UUID `json:"row_id_max"`
	// Columns is the file's parquet footer schema, column name to DuckDB
	// type. V2 has no footer-probe fallback, so it is never empty.
	Columns map[string]string `json:"columns"`
}

// Encode validates m and answers its stored bytes and their digest, the
// manifest_sha256 Publish takes: the SHA-256 of exactly those bytes as 64
// lowercase hex digits. The bytes are deterministic: fields in declaration
// order, files sorted by key, columns sorted by name, no insignificant
// whitespace. Encode sorts a copy of the file list; m is left as it is.
func (m Manifest) Encode() ([]byte, string, error) {
	m.Files = sortedFiles(m.Files)
	if err := m.validate(); err != nil {
		return nil, "", fmt.Errorf("encode manifest of schema %d generation %d: %w", m.SchemaID, m.Generation, err)
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return nil, "", fmt.Errorf("encode manifest of schema %d generation %d: %w", m.SchemaID, m.Generation, err)
	}
	return raw, Digest(raw), nil
}

// Digest is the SHA-256 of raw as 64 lowercase hex digits, the form a record's
// manifest_sha256 and a file's sha256 take.
func Digest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// Keys answers the object keys m lists, in its order.
func (m *Manifest) Keys() []string {
	keys := make([]string, len(m.Files))
	for i, f := range m.Files {
		keys[i] = f.Key
	}
	return keys
}

func sortedFiles(files []File) []File {
	out := slices.Clone(files)
	if out == nil {
		out = []File{}
	}
	slices.SortFunc(out, func(a, b File) int { return strings.Compare(a.Key, b.Key) })
	return out
}

// validate refuses a document the protocol does not have. It is what Encode
// checks before it writes and what Verify checks after the digest and the
// identity: a writer cannot store, and a reader never trusts, a manifest
// that names a file it could not scan.
func (m *Manifest) validate() error {
	if err := m.validateHeader(); err != nil {
		return err
	}
	if m.Files == nil {
		return errors.New("files is absent")
	}
	for i, f := range m.Files {
		if i > 0 && m.Files[i-1].Key >= f.Key {
			return fmt.Errorf("files are not sorted by key, or list %q twice", f.Key)
		}
		if err := m.validateFile(f); err != nil {
			return fmt.Errorf("file %d (%s): %w", i, f.Key, err)
		}
	}
	return nil
}

func (m *Manifest) validateHeader() error {
	switch {
	case m.Format != publication.ManifestFormat:
		return fmt.Errorf("format is %q, want %s", m.Format, publication.ManifestFormat)
	case m.SchemaID <= 0:
		return fmt.Errorf("schema_id %d is not positive", m.SchemaID)
	case m.Epoch <= 0 || m.Generation <= 0:
		return fmt.Errorf("epoch %d and generation %d must both be positive", m.Epoch, m.Generation)
	case m.ParentGeneration < 0 || m.ParentGeneration >= m.Generation:
		return fmt.Errorf("parent_generation %d is not an earlier generation than %d, or 0", m.ParentGeneration, m.Generation)
	case m.Lineage == uuid.Nil || m.Token == uuid.Nil:
		return fmt.Errorf("lineage %s and token %s must both be set", m.Lineage, m.Token)
	}
	switch m.Kind {
	case publication.KindFlush, publication.KindInit:
		if m.InputGeneration != 0 {
			return fmt.Errorf("a %s manifest names no input_generation, found %d", m.Kind, m.InputGeneration)
		}
	case publication.KindCompact:
		if m.InputGeneration <= 0 || m.InputGeneration >= m.Generation {
			return fmt.Errorf("a compact manifest names the earlier generation it merged as input_generation, found %d", m.InputGeneration)
		}
	default:
		return fmt.Errorf("kind %q is not %s, %s or %s", m.Kind, publication.KindFlush, publication.KindCompact, publication.KindInit)
	}
	return nil
}

func (m *Manifest) validateFile(f File) error {
	k, err := parseParquetKey(f.Key)
	if err != nil {
		return err
	}
	switch {
	case k.schemaID != m.SchemaID || k.epoch != m.Epoch:
		return fmt.Errorf("key belongs to schema %d epoch %d, the manifest to schema %d epoch %d", k.schemaID, k.epoch, m.SchemaID, m.Epoch)
	case k.tier != f.Tier:
		return fmt.Errorf("tier is %q, the key's %q", f.Tier, k.tier)
	case m.Kind == publication.KindInit && f.Tier != TierBase:
		return fmt.Errorf("an init generation lists the export's %s files only, not a %s file", TierBase, f.Tier)
	case f.SizeBytes <= 0 || f.RowCount <= 0:
		return fmt.Errorf("size_bytes %d and row_count %d must both be positive", f.SizeBytes, f.RowCount)
	case !isDigest(f.SHA256):
		return fmt.Errorf("sha256 %q is not 64 lowercase hex digits", f.SHA256)
	case f.RowIDMin == uuid.Nil || f.RowIDMax == uuid.Nil || strings.Compare(f.RowIDMin.String(), f.RowIDMax.String()) > 0:
		return fmt.Errorf("row_id_min %s and row_id_max %s must both be set, in order", f.RowIDMin, f.RowIDMax)
	case len(f.Columns) == 0:
		return errors.New("columns is empty: V2 has no footer-probe fallback")
	}
	for name, typ := range f.Columns {
		if name == "" || typ == "" {
			return fmt.Errorf("column %q has type %q; a column names both", name, typ)
		}
	}
	return nil
}

// isDigest answers whether s is a SHA-256 as 64 lowercase hex digits.
func isDigest(s string) bool {
	if len(s) != hex.EncodedLen(sha256.Size) {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
