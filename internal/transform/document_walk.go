package transform

import (
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// The write path's one interpreter of a document: walkDocument decides, for
// every position, which attribute and array-index context it addresses and
// whether it writes a record, with #312's last-spelling rule
// (spellingWinners) choosing among the spellings of one attribute. What the
// written row holds is read off its entries, never re-derived: the required
// policy (requireWrittenAttributes), the conversion (flattenToAttributes) and
// an update's stored values (resolveStoredValues) all consume them.

// entryKind classifies a position of the walk.
type entryKind uint8

const (
	// entryClaim is a scalar at a known attribute: it writes exactly one
	// record, or the write fails converting it (populateTypedValue).
	entryClaim entryKind = iota
	// entryMarker is [] at a registered list attribute: the explicit
	// empty-list marker row (array_indices "", both value columns NULL), so
	// the list round-trips as [] instead of degrading to an absent
	// attribute. Under merge-update semantics "tags": [] is the only way to
	// clear a list (#204). [] anywhere else writes nothing.
	entryMarker
	// entryParent is a value at a known path beneath which the walk found no
	// claim: {}, {"total": null}, or a scalar where an object belongs. The
	// caller sent the parent explicitly, so it is present for the required
	// policy even though it writes nothing.
	entryParent
	// entryRefusal is a position the write refuses as caller input.
	entryRefusal
)

// documentEntry is one position of a document the walk visited.
type documentEntry struct {
	kind entryKind
	// name is the dotted attribute path. A refusal carries one only where it
	// refuses an undefined attribute.
	name string
	// tagged is the record the position addresses, without a value, and the
	// key spelling that reached it. Its ArrayIndices is the position's
	// array-index context for every kind but entryRefusal.
	tagged taggedEAVRecord
	// meta and value are the claimed attribute and the value to convert. The
	// refusal of an undefined attribute carries the value found there, which
	// is how an update finds a stored value the write cannot place
	// (resolveStoredValues).
	meta  forma.AttributeMetadata
	value any
	// refusal is the published error of an entryRefusal.
	refusal error
}

// claims reports whether the entry writes a record.
func (e documentEntry) claims() bool {
	return e.kind == entryClaim || e.kind == entryMarker
}

// walkDocument visits data the way the write stores it. It neither converts
// nor stops at a refusal, so its entries describe the whole document: the
// write fails at the first refusal or conversion failure in entry order, as
// it always has, but only after the required policy has been judged on every
// claim (ToAttributes).
//
// The one exception is the nesting cap (checkPayloadDepth): past it there is
// no whole document to describe, a cyclic one has no end, so the walk stops
// and returns the cap's refusal instead of entries (#406).
//
// Keys are visited in sorted order at every map. For any dotted name the
// nested spelling's top-level key is a proper prefix of the literal one, so
// it sorts first and the literal key, the caller's explicit value, is
// visited last and wins (#312).
func walkDocument(schemaID int16, rowID uuid.UUID, data map[string]any, cache forma.SchemaAttributeCache) ([]documentEntry, error) {
	w := &documentWalk{schemaID: schemaID, rowID: rowID, cache: cache}
	w.visitMap(nil, data, nil)
	if w.tooDeep != nil {
		return nil, w.tooDeep
	}
	return w.entries, nil
}

type documentWalk struct {
	schemaID int16
	rowID    uuid.UUID
	cache    forma.SchemaAttributeCache
	entries  []documentEntry
	claims   int
	// tooDeep is the nesting cap's refusal once a container exceeds it; the
	// walk visits nothing after it.
	tooDeep error
}

func (w *documentWalk) visit(path []string, value any, indices []int) {
	switch v := value.(type) {
	case map[string]any:
		w.visitMap(path, v, indices)
	case []any:
		w.visitArray(path, v, indices)
	default:
		w.visitLeaf(path, v, indices)
	}
}

// enter reports whether the walk descends into the container at path,
// stopping it at the nesting cap.
func (w *documentWalk) enter(path []string, indices []int) bool {
	if w.tooDeep != nil {
		return false
	}
	w.tooDeep = checkPayloadDepth(flattenPosition(path, indices))
	return w.tooDeep == nil
}

func (w *documentWalk) visitMap(path []string, v map[string]any, indices []int) {
	if !w.enter(path, indices) {
		return
	}
	claimsBefore := w.claims
	keys := make([]string, 0, len(v))
	for key := range v {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if w.tooDeep != nil {
			return
		}
		value := v[key]
		newPath := append(path, key)
		if value == nil {
			candidateName := strings.Join(newPath, ".")
			if isKnownAttributeOrParent(candidateName, w.cache) {
				w.refuse(forma.InvalidInputf("attribute '%s' cannot be set to null; omit the key to preserve its current value", candidateName))
			}
			continue
		}
		w.visit(newPath, value, indices)
	}
	if w.claims == claimsBefore {
		w.parent(path, indices)
	}
}

func (w *documentWalk) visitArray(path []string, v []any, indices []int) {
	if !w.enter(path, indices) {
		return
	}
	if len(v) == 0 {
		w.emptyList(path)
		return
	}
	for i, item := range v {
		if w.tooDeep != nil {
			return
		}
		if item == nil {
			attrName := strings.Join(path, ".")
			if isKnownAttributeOrParent(attrName, w.cache) {
				w.refuse(forma.InvalidInputf("attribute '%s' cannot be set to null (array index %d); omit the element to preserve its current value", attrName, i))
			}
			continue
		}
		w.visit(path, item, append(indices, i))
	}
}

func (w *documentWalk) visitLeaf(path []string, v any, indices []int) {
	attrName := strings.Join(path, ".")
	meta, ok := w.cache[attrName]
	if !ok {
		// The internal schema id is operator detail: the caller addressed a
		// schema by name and cannot act on the int16 (#362 review, P2).
		w.entries = append(w.entries, documentEntry{
			kind:  entryRefusal,
			name:  attrName,
			value: v,
			refusal: forma.WithOperatorDetail(
				forma.InvalidInputf("attribute '%s' is not defined for this schema", attrName),
				fmt.Errorf("schema %d", w.schemaID)),
		})
		w.parent(path, indices)
		return
	}
	w.claim(documentEntry{
		kind:   entryClaim,
		name:   attrName,
		tagged: w.tag(path, meta.AttributeID, joinIndices(indices)),
		meta:   meta,
		value:  v,
	})
}

// emptyList claims the empty-list marker of a registered list attribute; an
// unregistered name writes nothing, as before.
func (w *documentWalk) emptyList(path []string) {
	attrName := strings.Join(path, ".")
	meta, ok := w.cache[attrName]
	if !ok || meta.ValueType != forma.ValueTypeList {
		return
	}
	w.claim(documentEntry{
		kind:   entryMarker,
		name:   attrName,
		tagged: w.tag(path, meta.AttributeID, ""),
		meta:   meta,
	})
}

// parent records an explicit parent at a known path; the root document and
// an unknown path are no attribute's parent.
func (w *documentWalk) parent(path []string, indices []int) {
	if len(path) == 0 {
		return
	}
	attrName := strings.Join(path, ".")
	if !isKnownAttributeOrParent(attrName, w.cache) {
		return
	}
	w.entries = append(w.entries, documentEntry{
		kind: entryParent,
		name: attrName,
		tagged: taggedEAVRecord{record: model.EAVRecord{
			SchemaID: w.schemaID, RowID: w.rowID, ArrayIndices: joinIndices(indices),
		}},
	})
}

func (w *documentWalk) claim(entry documentEntry) {
	w.entries = append(w.entries, entry)
	w.claims++
}

func (w *documentWalk) refuse(err error) {
	w.entries = append(w.entries, documentEntry{kind: entryRefusal, refusal: err})
}

func (w *documentWalk) tag(path []string, attrID int16, arrayIndices string) taggedEAVRecord {
	return taggedEAVRecord{
		record: model.EAVRecord{
			SchemaID:     w.schemaID,
			RowID:        w.rowID,
			AttrID:       attrID,
			ArrayIndices: arrayIndices,
		},
		spelling: spellingOf(path),
	}
}

// claimWinners applies #312 to the walk's claims.
func claimWinners(entries []documentEntry) spellingWinners {
	winners := make(spellingWinners, len(entries))
	for _, entry := range entries {
		if entry.claims() {
			winners.observe(entry.tagged)
		}
	}
	return winners
}

// requireWrittenAttributes judges every required policy on what the write
// stores: an attribute is present where a winning claim writes it, and a
// parent is present where a winning claim writes beneath it or the caller
// sent it explicitly (entryParent). A losing spelling writes nothing, so it
// makes nothing present, and a caller value whose every claim loses is no
// parent. A missing attribute is caller fault, so the error is published as
// invalid input (#301) naming the alphabetically first one.
//
// Every row this passes also passes the record-side check the read runs
// (checkRequiredAttributes): its leaves are exactly the records written, and
// its parents include theirs.
func requireWrittenAttributes(entries []documentEntry, cache forma.SchemaAttributeCache, relationRoots RelationRoots) error {
	winners := claimWinners(entries)
	leaves := make(map[string]map[string]struct{})
	parents := make(map[string]map[string]struct{})
	for _, entry := range entries {
		switch {
		case entry.claims() && winners.wins(entry.tagged):
			addPresence(leaves, entry.name, entry.tagged.record.ArrayIndices)
		case entry.kind == entryParent:
			addPresence(parents, entry.name, entry.tagged.record.ArrayIndices)
		}
	}
	missing := missingRequiredAttributes(cache, leaves, parents, relationRoots)
	if len(missing) == 0 {
		return nil
	}
	attrName, attrID := firstMissingAttribute(missing)
	return forma.InvalidInputf("missing required attribute '%s' (attrID=%d) in EAV records", attrName, attrID)
}

// flattenToAttributes converts the walk's claims to records in entry order;
// the first refusal or conversion failure fails the write. Every claim is
// converted, losing spellings included, so a value the caller sent is judged
// whichever spelling wins.
func flattenToAttributes(entries []documentEntry) ([]taggedEAVRecord, error) {
	flattened := make([]taggedEAVRecord, 0, len(entries))
	for _, entry := range entries {
		switch entry.kind {
		case entryRefusal:
			return nil, entry.refusal
		case entryMarker:
			flattened = append(flattened, entry.tagged)
		case entryClaim:
			record := entry.tagged.record
			set, err := populateTypedValue(&record, entry.name, entry.value, entry.meta)
			if err != nil {
				return nil, fmt.Errorf("convert value for attribute '%s': %w", entry.name, err)
			}
			if set {
				flattened = append(flattened, taggedEAVRecord{record: record, spelling: entry.tagged.spelling})
			}
		}
	}
	return flattened, nil
}
