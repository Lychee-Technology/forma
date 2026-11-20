package manifestv2

import (
	"errors"
	"fmt"
	"maps"

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
		Files: ownedFiles(files),
	}
}

// ownedFiles answers files sorted by key, each with its own Columns map, so a
// built manifest shares nothing mutable with its parent, which may be a
// Cache's verified entry, or with the caller's input files.
func ownedFiles(files []File) []File {
	out := sortedFiles(files)
	for i := range out {
		out[i].Columns = maps.Clone(out[i].Columns)
	}
	return out
}

// NewFlush builds a flush generation: the parent's files and the new delta
// files (docs/publication-protocol-v2.md, "What a generation replaces").
// parent is the manifest of h's parent generation, nil exactly when h has
// none (the first flush of an unpublished epoch). A rebased flush passes the
// new head as parent, under a fresh token. The parent must be the same
// schema, epoch and lineage, since a flush keeps the record's lineage. The
// manifest answered owns its files: editing it changes neither parent, which
// a Cache may hold, nor deltas.
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

// NewCompact builds a compact generation: its parent's files, less the
// sources the compaction merged, plus the merge outputs
// (docs/publication-protocol-v2.md, "What a generation replaces"). parent is
// the manifest of h's parent generation: the input generation the
// compaction merged from (h.InputGeneration), or the head it rebased onto
// over flushes only, which appended files and removed none. So every merged
// key must still be listed in parent; one that is not was removed or
// replaced by a writer the compaction may not rebase over, and the build is
// refused (ErrMergedSourceGone). Outputs are base files under fresh keys. A
// merge of nothing but rows it folded away leaves no output, and the
// generation may then be empty. The manifest answered owns its files, as
// NewFlush's does.
func NewCompact(parent *Manifest, h Header, merged []string, outputs []File) (Manifest, error) {
	fail := func(format string, args ...any) (Manifest, error) {
		return Manifest{}, fmt.Errorf("build compact generation %d of schema %d: %s", h.Generation, h.SchemaID, fmt.Sprintf(format, args...))
	}
	switch {
	case h.Kind != publication.KindCompact:
		return fail("header is a %s", h.Kind)
	case parent == nil:
		return fail("a compaction replaces files of its parent generation %d, whose manifest is not given", h.ParentGeneration)
	case parent.SchemaID != h.SchemaID || parent.Epoch != h.Epoch || parent.Generation != h.ParentGeneration:
		return fail("parent manifest is schema %d epoch %d generation %d, the header names schema %d epoch %d generation %d",
			parent.SchemaID, parent.Epoch, parent.Generation, h.SchemaID, h.Epoch, h.ParentGeneration)
	case parent.Lineage != h.Lineage:
		return fail("a compaction keeps its parent's lineage %s, not %s", parent.Lineage, h.Lineage)
	case h.InputGeneration <= 0 || h.InputGeneration > parent.Generation:
		return fail("input generation %d is not the parent %d or an earlier generation", h.InputGeneration, parent.Generation)
	case len(merged) == 0:
		return fail("a compaction merges at least one source file")
	}
	gone := make(map[string]bool, len(merged))
	for _, key := range merged {
		if gone[key] {
			return fail("source %s is named twice", key)
		}
		gone[key] = true
	}
	listed := make(map[string]bool, len(parent.Files)+len(outputs))
	for _, f := range parent.Files {
		listed[f.Key] = true
	}
	// Membership first: only once every merged key is listed is
	// len(parent.Files)-len(merged) a count of the files that stay.
	for key := range gone {
		if !listed[key] {
			return Manifest{}, fmt.Errorf("build compact generation %d of schema %d: source %s is not listed by parent generation %d: %w",
				h.Generation, h.SchemaID, key, parent.Generation, ErrMergedSourceGone)
		}
	}
	files := make([]File, 0, len(parent.Files)-len(merged)+len(outputs))
	for _, f := range parent.Files {
		if !gone[f.Key] {
			files = append(files, f)
		}
	}
	for _, o := range outputs {
		if o.Tier != TierBase {
			return fail("output %s is %s; a compaction writes %s files only", o.Key, o.Tier, TierBase)
		}
		if listed[o.Key] {
			return fail("output %s is already listed", o.Key)
		}
		listed[o.Key] = true
		files = append(files, o)
	}
	return finish(h.manifest(files))
}

// ErrMergedSourceGone reports a compaction whose parent no longer lists a
// file it merged. Only a compaction or a cdc-init removes a file, and a
// compaction never rebases over either (publication.Rebase), so its staging
// is abandoned.
var ErrMergedSourceGone = errors.New("a merged source is no longer listed")

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
