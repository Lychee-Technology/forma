package publication

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// ManifestFormat is the format a V2 manifest names; a manifest that names any
// other is refused.
const ManifestFormat = "forma-manifest/2"

// ErrManifestInvalid reports a manifest object whose stored bytes are not the
// manifest the history committed: their SHA-256 differs from the row's
// manifest_sha256, they do not parse, they name another format, or their
// identity (schema_id, epoch, generation, token) differs from the row's. A
// manifest is immutable and its digest committed with its row, so this is a
// fault, never a state to plan around.
var ErrManifestInvalid = errors.New("manifest fails verification")

// isDigest answers whether s is a SHA-256 as ManifestFiles compares it: 64
// lowercase hex digits.
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

// manifestIdentity is the part of a manifest verification reads: the identity
// it is checked against and the keys it lists. Every other field is the
// reader's and the writers' to check.
type manifestIdentity struct {
	Format     *string    `json:"format"`
	SchemaID   *int16     `json:"schema_id"`
	Epoch      *int64     `json:"epoch"`
	Generation *int64     `json:"generation"`
	Token      *uuid.UUID `json:"token"`
	Files      *[]struct {
		Key string `json:"key"`
	} `json:"files"`
}

// ManifestFiles verifies raw, the stored bytes of generation g's manifest of
// schema schemaID, against g's history row, and answers the object keys it
// lists. It refuses (ErrManifestInvalid) bytes whose SHA-256, as lowercase
// hex, is not g.ManifestSHA256, bytes that are not one JSON object, a format
// other than ManifestFormat, an identity field that is absent or differs from
// the row's, an absent files array, and a file without a key. The digest is
// checked first: a parseable manifest of another generation, or one a bit
// flip left parseable, never reaches the file list.
func ManifestFiles(raw []byte, schemaID int16, g Generation) ([]string, error) {
	fail := func(format string, args ...any) error {
		return fmt.Errorf("manifest %s of schema %d generation %d: %s: %w", g.ManifestKey, schemaID, g.Generation, fmt.Sprintf(format, args...), ErrManifestInvalid)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != g.ManifestSHA256 {
		return nil, fail("stored bytes have SHA-256 %s, the history row %q", got, g.ManifestSHA256)
	}
	var m manifestIdentity
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&m); err != nil {
		return nil, fail("parse: %v", err)
	}
	if dec.More() {
		return nil, fail("trailing data after the manifest object")
	}
	switch {
	case m.Format == nil || *m.Format != ManifestFormat:
		return nil, fail("format is not %s", ManifestFormat)
	case m.SchemaID == nil || *m.SchemaID != schemaID:
		return nil, fail("schema_id is not %d", schemaID)
	case m.Epoch == nil || *m.Epoch != g.Epoch:
		return nil, fail("epoch is not %d", g.Epoch)
	case m.Generation == nil || *m.Generation != g.Generation:
		return nil, fail("generation is not %d", g.Generation)
	case m.Token == nil || *m.Token != g.Token:
		return nil, fail("token is not %s", g.Token)
	case m.Files == nil:
		return nil, fail("files is absent")
	}
	keys := make([]string, 0, len(*m.Files))
	for i, f := range *m.Files {
		if f.Key == "" {
			return nil, fail("file %d has no key", i)
		}
		keys = append(keys, f.Key)
	}
	return keys, nil
}
