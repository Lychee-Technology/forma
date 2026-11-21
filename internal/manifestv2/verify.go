package manifestv2

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/publication"
)

// ErrManifestMissing reports a committed generation whose manifest object the
// store does not hold. Objects are deleted only by garbage collection, and
// never while a generation is retained, so for a generation a record names
// it is loss.
var ErrManifestMissing = errors.New("manifest object missing")

// ConsistencyError is the read-path consistency error for a committed
// generation's manifest: the object is missing (ErrManifestMissing), or its
// bytes are not the manifest PostgreSQL committed (publication.
// ErrManifestInvalid): another digest, another identity, an unknown format or
// a document the protocol does not have. It is an operator-visible fault,
// never a 4xx, and never a state to retry or degrade around: the reader
// fails the read (#900). A failed store call is not one; it stays a plain
// error.
type ConsistencyError struct {
	SchemaID    int16
	Epoch       int64
	Generation  int64
	ManifestKey string
	// Err wraps ErrManifestMissing or publication.ErrManifestInvalid.
	Err error
}

func (e *ConsistencyError) Error() string {
	return fmt.Sprintf("manifest %s of schema %d epoch %d generation %d: %v", e.ManifestKey, e.SchemaID, e.Epoch, e.Generation, e.Err)
}

func (e *ConsistencyError) Unwrap() error { return e.Err }

// errNotPublished reports a record a reader has no manifest to fetch for.
var errNotPublished = errors.New("record names no generation")

// Ref is what PostgreSQL committed about a generation's manifest, and what a
// fetched manifest must match. Lineage and Kind are checked when set: a
// record names the lineage and not the kind, a history row the reverse.
type Ref struct {
	SchemaID         int16
	Epoch            int64
	Generation       int64
	ParentGeneration int64
	Token            uuid.UUID
	Lineage          uuid.UUID
	Kind             publication.Kind
	ManifestKey      string
	ManifestSHA256   string
}

// RecordRef is the reference of a published record's current generation.
func RecordRef(r publication.Record) (Ref, error) {
	if r.State != publication.Published {
		return Ref{}, fmt.Errorf("schema %d at epoch %d is %s: %w", r.SchemaID, r.Epoch, r.State, errNotPublished)
	}
	return Ref{
		SchemaID: r.SchemaID, Epoch: r.Epoch, Generation: r.Generation, ParentGeneration: r.ParentGeneration,
		Token: r.Token, Lineage: r.Lineage, ManifestKey: r.ManifestKey, ManifestSHA256: r.ManifestSHA256,
	}, nil
}

// GenerationRef is the reference of a generation's history row.
func GenerationRef(schemaID int16, g publication.Generation) Ref {
	return Ref{
		SchemaID: schemaID, Epoch: g.Epoch, Generation: g.Generation, ParentGeneration: g.ParentGeneration,
		Token: g.Token, Lineage: g.Lineage, Kind: g.Kind, ManifestKey: g.ManifestKey, ManifestSHA256: g.ManifestSHA256,
	}
}

func (r Ref) fail(err error) error {
	return &ConsistencyError{SchemaID: r.SchemaID, Epoch: r.Epoch, Generation: r.Generation, ManifestKey: r.ManifestKey, Err: err}
}

func (r Ref) invalid(format string, args ...any) error {
	return r.fail(fmt.Errorf("%s: %w", fmt.Sprintf(format, args...), publication.ErrManifestInvalid))
}

// Verify checks raw, the stored bytes of r's manifest, and answers the
// document. It runs publication.ManifestFiles first, the collector's check
// (digest, then format, identity and keys), so the reader and the collector
// never disagree about a manifest. Then it checks the wire form (checkWire:
// exactly the members the protocol names, each once and spelled exactly, none
// null), decodes the whole document, validates it as Encode does
// and checks the rest of r: parent, lineage, kind, and that the manifest key
// and every listed key spell r's identity under one prefix. Every refusal is
// a *ConsistencyError wrapping publication.ErrManifestInvalid.
func Verify(raw []byte, r Ref) (*Manifest, error) {
	keys, err := publication.ManifestFiles(raw, r.SchemaID, publication.Generation{
		Generation: r.Generation, Epoch: r.Epoch, ManifestKey: r.ManifestKey,
		ManifestSHA256: r.ManifestSHA256, Token: r.Token,
	})
	if err != nil {
		return nil, r.fail(err)
	}
	if err := checkWire(raw); err != nil {
		return nil, r.invalid("%v", err)
	}
	var m Manifest
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return nil, r.invalid("decode: %v", err)
	}
	if err := m.validate(); err != nil {
		return nil, r.invalid("%v", err)
	}
	if !slices.Equal(keys, m.Keys()) {
		return nil, r.invalid("the collector's reading lists other keys than the document's")
	}
	if err := r.check(&m, Digest(raw)); err != nil {
		return nil, err
	}
	if err := r.checkKeys(&m); err != nil {
		return nil, err
	}
	return &m, nil
}

// check compares a manifest whose stored bytes have the digest against r.
// Verify runs it after decoding, and a Cache on every hit, so a reference
// that names a cached key with another digest or identity is refused too.
func (r Ref) check(m *Manifest, digest string) error {
	switch {
	case digest != r.ManifestSHA256:
		return r.invalid("stored bytes have SHA-256 %s, PostgreSQL committed %q", digest, r.ManifestSHA256)
	case m.SchemaID != r.SchemaID || m.Epoch != r.Epoch || m.Generation != r.Generation || m.Token != r.Token:
		return r.invalid("manifest is schema %d epoch %d generation %d token %s, PostgreSQL committed schema %d epoch %d generation %d token %s",
			m.SchemaID, m.Epoch, m.Generation, m.Token, r.SchemaID, r.Epoch, r.Generation, r.Token)
	case m.ParentGeneration != r.ParentGeneration:
		return r.invalid("parent_generation is %d, PostgreSQL committed %d", m.ParentGeneration, r.ParentGeneration)
	case r.Lineage != uuid.Nil && m.Lineage != r.Lineage:
		return r.invalid("lineage is %s, PostgreSQL committed %s", m.Lineage, r.Lineage)
	case r.Kind != "" && m.Kind != r.Kind:
		return r.invalid("kind is %s, PostgreSQL committed %s", m.Kind, r.Kind)
	}
	return nil
}

// checkKeys refuses a manifest key that does not spell the manifest's
// identity, and a listed key under another prefix. It depends only on the
// manifest key and the document, so a Cache, whose entries are keyed by the
// one and hold the other, needs it once per fetch, not on every hit.
func (r Ref) checkKeys(m *Manifest) error {
	k, err := parseManifestKey(r.ManifestKey)
	if err != nil {
		return r.invalid("%v", err)
	}
	if k.schemaID != m.SchemaID || k.epoch != m.Epoch || k.generation != m.Generation || k.token != m.Token {
		return r.invalid("manifest key spells schema %d epoch %d generation %d token %s, not the manifest's identity",
			k.schemaID, k.epoch, k.generation, k.token)
	}
	for _, f := range m.Files {
		if p, _ := parseParquetKey(f.Key); p.layout != k.layout {
			return r.invalid("file %s is not under the manifest's prefix %q", f.Key, k.layout.Prefix)
		}
	}
	return nil
}
