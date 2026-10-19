package transform

import (
	"context"
	"fmt"
	"maps"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
)

// MergeUpdate builds the document an update writes (model.
// PersistentRecordTransformer). The stored row is rebuilt as on read, but
// with each EAV record held undecoded (holdRecordValue); merge places, moves
// or drops those values like any other; and resolveStoredValues then decodes
// exactly the ones the written row keeps. Whether a stored value is kept is
// not decided here: it is read off the same walk the write runs over the
// merged document (walkDocument), so the update's path and #312 semantics
// have one source. A row the read refuses to rebuild, such as one holding a
// marker it cannot place (rebuildInferred), is refused before the merge.
func (t *persistentRecordTransformer) MergeUpdate(
	ctx context.Context,
	record *model.PersistentRecord,
	merge func(base map[string]any) (map[string]any, error),
) (map[string]any, error) {
	base, err := t.fromPersistentRecord(ctx, record, holdRecordValue)
	if err != nil {
		return nil, err
	}
	cache, _, err := schemameta.GetSchemaMetadata(t.registry, record.SchemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to load schema %d metadata for row %s: %w", record.SchemaID, record.RowID, err)
	}
	merged, err := merge(base)
	if err != nil {
		return nil, fmt.Errorf("failed to merge the update into row %s: %w", record.RowID, err)
	}
	return resolveStoredValues(record.SchemaID, record.RowID, merged, cache)
}

// resolveStoredValues replaces every stored value the merged document holds
// with its decoded value, or removes it when the write discards it.
//
// A stored value is kept exactly when the write would store it: the walk
// claims it and its spelling wins (updateWinners). A caller's literal
// "contact.total" therefore discards the stored contact.total the base nests
// beside it, and a caller value that replaces a subtree has already dropped
// the stored values beneath it in merge. Kept values are decoded in walk
// order, so of several unreadable kept values the same one fails the update
// on every run; the error is the read path's, plain, as reading the row
// would raise.
//
// A stored value the walk claims but whose spelling loses is discarded only
// when the caller's own spelling of the attribute replaces it
// (storedValue.replacedBy). Otherwise the winning spelling is other stored
// records', which a row rebuilt from records written through different
// spellings of the attribute holds, and the update is refused rather than
// silently deleting the record (respelledError).
//
// A stored value the walk does not claim was rebuilt where the write cannot
// read it back. A row without array ancestry leaves one there when its record
// carries more array indices than its parent takes, as the members of an
// array of objects nested inside another do: it lands beneath its own
// attribute's name, a path the schema does not define (#623). Removing it
// would delete its record in an update that never addressed it, so the update
// is refused instead, in the same walk order, unless the caller's own
// spelling of the attribute replaces the value.
//
// Removing a value must not change what the rest of the row means. A
// container it empties is removed with it, since it held stored values only
// and the stored row is no evidence the caller sent it. An array keeps its
// element positions: an emptied element before a kept one stays as {}, so
// the kept elements keep their indices, and emptied trailing elements are
// cut. The root document is never removed, and a container that was empty
// before resolution, such as a caller's {} or [], is kept.
func resolveStoredValues(schemaID int16, rowID uuid.UUID, doc map[string]any, cache forma.SchemaAttributeCache) (map[string]any, error) {
	entries, err := walkDocument(schemaID, rowID, doc, cache)
	if err != nil {
		return nil, err
	}
	winners := updateWinners(entries)
	stored := storedSpellingsOf(entries)
	kept := make(map[*storedValue]any)
	for _, entry := range entries {
		held, isHeld := entry.value.(*storedValue)
		if !isHeld {
			continue
		}
		claimed := entry.kind == entryClaim
		if claimed && winners.wins(entry.tagged) {
			value, err := held.decode()
			if err != nil {
				return nil, err
			}
			// A stored record decodes to nil only where the read decodes
			// it to nil and leaves it out of the document; so does this.
			if value != nil {
				kept[held] = value
			}
			continue
		}
		spelling := held.nestedSpelling()
		if claimed {
			spelling = entry.tagged.spelling
		}
		switch {
		case held.replacedBy(winners, spelling, stored):
		case claimed:
			return nil, held.respelledError()
		default:
			return nil, held.unplacedError(entry.name)
		}
	}
	resolveMap(doc, kept)
	return doc, nil
}

// updateWinners is #312's rule over an update's merged document, with the
// caller's spellings ahead of the stored row's: per attribute, the last
// spelling the caller sends wins, and the last spelling the walk claims a
// stored value at wins only when the caller sends none.
//
// In a caller's own payload the literal key sorts last and wins, which is
// what lets it replace the nested spelling the merge base holds. A row
// rebuilt by array ancestry can put a stored value under a literal key of
// its own, beside an array (rebuildByAncestry), and a caller's fragment that
// rewrites the array spells the same attribute nested inside it. The last
// spelling there is the stored one, and letting it win would write the row
// back without the caller's value. The stored value loses instead, and is
// discarded as any stored value the caller's spelling replaces
// (storedValue.replacedBy), so the written row holds the caller's spelling
// alone and the write's own rule (dedupeEAVRecords) agrees.
func updateWinners(entries []documentEntry) spellingWinners {
	winners, sent := make(spellingWinners, len(entries)), make(spellingWinners)
	for _, entry := range entries {
		if !entry.claims() {
			continue
		}
		winners.observe(entry.tagged)
		if _, held := entry.value.(*storedValue); !held {
			sent.observe(entry.tagged)
		}
	}
	maps.Copy(winners, sent)
	return winners
}

// resolveMap resolves the stored values beneath m in place and reports
// whether m is left with anything; a map empty to begin with is kept. Only
// containers that held a stored value are written, so a caller's own values,
// which never hold one, are left untouched.
func resolveMap(m map[string]any, kept map[*storedValue]any) bool {
	if len(m) == 0 {
		return true
	}
	for key, child := range m {
		resolved, keep, changed := resolveValue(child, kept)
		switch {
		case !keep:
			delete(m, key)
		case changed:
			m[key] = resolved
		}
	}
	return len(m) > 0
}

// resolveArray resolves the stored values beneath a in place, keeping element
// positions, and reports whether a is left with anything.
func resolveArray(a []any, kept map[*storedValue]any) ([]any, bool, bool) {
	if len(a) == 0 {
		return a, true, false
	}
	changed := false
	last := -1
	for i, item := range a {
		resolved, keep, itemChanged := resolveValue(item, kept)
		if !keep {
			resolved, itemChanged = map[string]any{}, true
		}
		if itemChanged {
			a[i] = resolved
			changed = true
		}
		if keep {
			last = i
		}
	}
	if last < 0 {
		return nil, false, true
	}
	if last < len(a)-1 {
		return a[:last+1], true, true
	}
	return a, true, changed
}

// resolveValue resolves one value: it reports the value to hold in its
// place, whether anything is left of it, and whether the place must be
// rewritten.
func resolveValue(value any, kept map[*storedValue]any) (any, bool, bool) {
	switch v := value.(type) {
	case *storedValue:
		resolved, keep := kept[v]
		return resolved, keep, true
	case map[string]any:
		return v, resolveMap(v, kept), false
	case []any:
		return resolveArray(v, kept)
	default:
		return value, true, false
	}
}
