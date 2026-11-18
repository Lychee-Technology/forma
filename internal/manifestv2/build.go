package manifestv2

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/publication"
)

// Header is the identity of the generation a writer stages, everything of its
// manifest but the file list.
type Header struct {
	SchemaID         int16
	Epoch            int64
	Generation       int64
	ParentGeneration int64
	Lineage          uuid.UUID
	Token            uuid.UUID
	Kind             publication.Kind
	InputGeneration  int64
}

// Stage answers the header of the generation a writer of kind stages after
// cur, the record it read: the next number of the allocation counter, and as
// parent the record's current generation when it is published and 0 when the
// record has none in this epoch, which is what Publish's compare-and-set
// commits. lineage and token are the caller's: Publish checks the lineage
// rules. A dropped schema stages nothing.
func Stage(cur publication.Record, kind publication.Kind, lineage, token uuid.UUID) (Header, error) {
	if cur.State == publication.Dropped {
		return Header{}, fmt.Errorf("stage a %s of schema %d at epoch %d: %w", kind, cur.SchemaID, cur.Epoch, publication.ErrDropped)
	}
	h := Header{
		SchemaID: cur.SchemaID, Epoch: cur.Epoch, Generation: cur.Generation + 1,
		Lineage: lineage, Token: token, Kind: kind,
	}
	if cur.State == publication.Published {
		h.ParentGeneration = cur.Generation
	}
	return h, nil
}

// Key is the manifest key of the generation h names, under l.
func (h Header) Key(l Layout) string {
	return l.ManifestKey(h.SchemaID, h.Epoch, h.Generation, h.Token)
}

func (h Header) manifest(files []File) Manifest {
	return Manifest{
		Format: publication.ManifestFormat, SchemaID: h.SchemaID, Epoch: h.Epoch,
		Generation: h.Generation, ParentGeneration: h.ParentGeneration,
		Lineage: h.Lineage, Token: h.Token, Kind: h.Kind, InputGeneration: h.InputGeneration,
		Files: sortedFiles(files),
	}
}

// NewFlush builds a flush generation: the parent's files and the new delta
// files (docs/publication-protocol-v2.md, "What a generation replaces").
// parent is the manifest of h's parent generation, nil exactly when h has
// none (the first flush of an unpublished epoch). A rebased flush passes the
// new head as parent, under a fresh token. The parent must be the same
// schema, epoch and lineage, since a flush keeps the record's lineage.
func NewFlush(parent *Manifest, h Header, deltas []File) (Manifest, error) {
	fail := func(format string, args ...any) (Manifest, error) {
		return Manifest{}, fmt.Errorf("build flush generation %d of schema %d: %s", h.Generation, h.SchemaID, fmt.Sprintf(format, args...))
	}
	if h.Kind != publication.KindFlush {
		return fail("header is a %s", h.Kind)
	}
	if len(deltas) == 0 {
		return fail("a flush publishes at least one delta file")
	}
	var files []File
	switch {
	case parent == nil && h.ParentGeneration != 0:
		return fail("parent generation %d is named but its manifest is not given", h.ParentGeneration)
	case parent == nil:
	case parent.SchemaID != h.SchemaID || parent.Epoch != h.Epoch || parent.Generation != h.ParentGeneration:
		return fail("parent manifest is schema %d epoch %d generation %d, the header names schema %d epoch %d generation %d",
			parent.SchemaID, parent.Epoch, parent.Generation, h.SchemaID, h.Epoch, h.ParentGeneration)
	case parent.Lineage != h.Lineage:
		return fail("a flush keeps its parent's lineage %s, not %s", parent.Lineage, h.Lineage)
	default:
		files = append(files, parent.Files...)
	}
	seen := make(map[string]bool, len(files)+len(deltas))
	for _, f := range files {
		seen[f.Key] = true
	}
	for _, d := range deltas {
		if d.Tier != TierDelta {
			return fail("new file %s is %s; a flush adds %s files only", d.Key, d.Tier, TierDelta)
		}
		if seen[d.Key] {
			return fail("new file %s is already listed", d.Key)
		}
		seen[d.Key] = true
		files = append(files, d)
	}
	return finish(h.manifest(files))
}

// NewInit builds an init generation: the export's base files only. Every
// delta and earlier base file leaves the inventory. An export of no rows is
// an empty generation, which says the parquet tiers hold nothing.
func NewInit(h Header, base []File) (Manifest, error) {
	if h.Kind != publication.KindInit {
		return Manifest{}, fmt.Errorf("build init generation %d of schema %d: header is a %s", h.Generation, h.SchemaID, h.Kind)
	}
	return finish(h.manifest(base))
}

func finish(m Manifest) (Manifest, error) {
	if err := m.validate(); err != nil {
		return Manifest{}, fmt.Errorf("build %s generation %d of schema %d: %w", m.Kind, m.Generation, m.SchemaID, err)
	}
	return m, nil
}
