package transform

import (
	"sort"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemavalidate"
)

// NormalizeDottedKeys rewrites literal dotted keys into their nested paths,
// producing the document handed to the schema validator.
//
// It is for the validator only. The writer keeps receiving the caller's original
// map, so flattenToAttributes and #312's dedupe remain the single authority on
// which record wins: records carry the spelling that produced them, and the last
// spelling replaces the whole logical attribute. Merging spellings into one
// document destroys those tags, so a normalized document cannot re-derive that
// precedence in every case — which is why it must never reach the write path.
// Nothing here can change which records are written.
//
// What it is for: attribute names in this codebase are dotted, so a caller
// reading the metadata may address contact.email either as a nested object or as
// a literal key. Left literal, JSON Schema treats it as an unknown property —
// with no additionalProperties declared it passes validation and its value is
// never examined, making validation trivially bypassable (#314). Expanding it
// first means the value is checked like any other.
//
// Interior paths are expanded too, not just leaf attributes: {"contact.snapshot":
// {"code": …}} hides "code" from validation exactly the way a literal leaf key
// hides its own value. The test is isKnownAttributeOrParent — the same predicate
// flattenToAttributes uses — so a name is expanded when the schema defines it or
// defines something beneath it. An unknown dotted key is left alone; the writer
// still rejects it with "attribute is not defined".
//
// A dotted key is not expanded when the schema declares an array between the
// key's own position and its leaf, because it cannot be: nesting
// requirement.areas.city at the top level puts an object where the schema
// declares an array, and the caller gets a 400 naming a type they never sent.
// Such a value is therefore not validated at all — the documented limit of this
// function.
//
// The discriminator is where the caller writes the key, not which attribute it
// names. The same name is expanded and validated when it is written inside an
// element of the array it lies under: within an element of requirement.areas the
// array is already behind us, so "city" lands in the right place, and a dotted
// "snapshot.code" inside a propertyInterests element nests legally. That is
// ArrayPaths.CrossesBelow asking the question relative to the current node. It
// holds unconditionally on the shipped schemas only because none of them nests
// an array inside an array.
//
// Relation roots need no rule here. RelationIndex.StripComputedFields deletes
// the whole relation subtree from the payload before this runs — the root and
// every dotted descendant such as contactSnapshot.name (#318) — so no name
// beneath a relation root is present to expand. Pinned by
// TestStripLeavesNothingCoveredForTheValidator.
//
// arrays comes from schemavalidate, the only place that knows which paths are
// arrays; the metadata cache records requirement.areas.city and contact.email
// identically. Passing a nil set is safe and means "nothing known to be an
// array".
//
// When both spellings are present the literal wins, matching encoding/json's
// duplicate-key semantics. Keys are applied in sorted order, and for any dotted
// name X.Y the shorter spelling X sorts before X.Y, so the longer, more specific
// one is applied last.
//
// That is not the writer's rule, and this document does not claim to reproduce
// it. The writer's dedupe drops the losing spelling's records for the whole
// logical attribute; here the losing spelling's *siblings* are merged in, which
// is what keeps every persisted value visible to the validator. At an array the
// two rules diverge outright — see mergeSlices for the false rejection that
// follows and why it is accepted.
//
// The input is never mutated: maps and slices are rebuilt rather than shared.
func NormalizeDottedKeys(
	data map[string]any,
	cache forma.SchemaAttributeCache,
	arrays schemavalidate.ArrayPaths,
) map[string]any {
	if data == nil {
		// A nil document is returned unchanged rather than as an empty map, so
		// this function never fabricates a document the caller did not send.
		//
		// It does not change the verdict. Measured against lead_full.json, both
		// are rejected and only the message differs: nil marshals to JSON null and
		// fails the root `"type": "object"`, while {} fails `required`. Every
		// shipped schema declares that root type, so there is no schema here for
		// which one passes and the other does not.
		return nil
	}
	return normalizeMap(data, "", schemaView{cache: cache, arrays: arrays})
}

// schemaView bundles the schema-derived inputs the walk consults. Both are fixed
// for a whole document, so grouping them keeps the recursion's parameters about
// position in the document rather than about configuration.
type schemaView struct {
	cache  forma.SchemaAttributeCache
	arrays schemavalidate.ArrayPaths
}

// normalizeMap rebuilds src with dotted keys expanded, where prefix is the
// dotted attribute-name prefix of src's own position in the document.
func normalizeMap(src map[string]any, prefix string, view schemaView) map[string]any {
	dst := make(map[string]any, len(src))

	keys := make([]string, 0, len(src))
	for key := range src {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		name := joinName(prefix, key)
		value := normalizeValue(src[key], name, view)

		parts := strings.Split(key, ".")
		if shouldExpand(dst, parts, prefix, name, view) {
			setNestedValue(dst, parts, value)
			continue
		}
		mergeValue(dst, key, value)
	}
	return dst
}

// normalizeValue descends into containers and copies everything else through.
// Rebuilding both maps and slices is what keeps the caller's document unmutated:
// merging writes into the result, and the result must share nothing with the map
// the writer will still be handed.
//
// A typed nil container is copied through rather than descended into. A Go
// embedder holding `var contact map[string]any` sends a non-nil any wrapping a
// nil map; recursing produced {} — or [] for a slice — which satisfies
// "type":"object" and everything beneath it, while the writer walked the same
// nil and emitted no attributes. Preserved as nil, the round-trip in Validate
// presents it as null and the schema's own "type" decides, which is the answer
// that matches what gets stored.
func normalizeValue(value any, name string, view schemaView) any {
	switch typed := value.(type) {
	case map[string]any:
		if typed == nil {
			return value
		}
		return normalizeMap(typed, name, view)
	case []any:
		if typed == nil {
			return value
		}
		return normalizeSlice(typed, name, view)
	default:
		return value
	}
}

// normalizeSlice normalizes each element. Array elements keep their parent's
// name because an index is not part of an attribute name — flattenToAttributes
// carries indices separately and recurses into elements with the path unchanged
// — so {"tags":[{"a.b":1}]} must expand the same as {"tags":{"a.b":1}} would.
func normalizeSlice(src []any, prefix string, view schemaView) []any {
	dst := make([]any, len(src))
	for i, item := range src {
		dst[i] = normalizeValue(item, prefix, view)
	}
	return dst
}

func joinName(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + "." + key
}

// shouldExpand gates expansion on three independent questions, each answered by
// one predicate and nothing else:
//
//   - is the key dotted at all, and does the schema know the name;
//   - does the schema declare an array between this node and the key's leaf
//     (arrays.CrossesBelow);
//   - does the payload itself already hold an array above it (arrayOnPath).
//
// Both array questions are asked about the key's own position, not the absolute
// attribute name. Inside an element of a schema array the surrounding array is
// already behind us, and a dotted key there nests legally — asking about the
// absolute name would refuse every dotted key in every array element.
//
// The two array checks overlap but neither subsumes the other. The schema set is
// the only way to know about an array the payload does not happen to contain,
// and the payload check is the only way to catch an array the derivation cannot
// see — one declared behind a $ref, or one the caller sent where the schema says
// object.
func shouldExpand(dst map[string]any, parts []string, prefix, name string, view schemaView) bool {
	return len(parts) > 1 &&
		isKnownAttributeOrParent(name, view.cache) &&
		!view.arrays.CrossesBelow(prefix, name) &&
		!arrayOnPath(dst, parts)
}

// arrayOnPath reports whether an array already sits on the expansion path,
// according to the payload.
//
// The schema's array paths catch the declared cases first, so what is left to
// this check is the arrays the derivation cannot see: one declared behind a $ref,
// whose target the library keeps in an unexported side table, and one the caller
// sent at a path the schema types as an object. In both, expanding would put an
// object where an array is and either produce a rejection for a type the caller
// never sent or erase their array from the document, hiding their own type error.
//
// Only interior segments are checked. An array at the final segment is the
// value the caller wrote there, and validating it is exactly right.
func arrayOnPath(dst map[string]any, parts []string) bool {
	current := dst
	for _, part := range parts[:len(parts)-1] {
		switch existing := current[part].(type) {
		case map[string]any:
			current = existing
		case []any:
			return true
		default:
			return false
		}
	}
	return false
}

// setNestedValue walks parts, creating intermediate maps, and merges the leaf.
// A non-map already on the path is replaced: the dotted spelling is the later
// one, and only the validator's view is at stake.
//
// A typed nil map counts as a non-map here. It is preserved rather than
// materialised (see normalizeValue), so it can genuinely sit on the path — and
// writing into one panics rather than replacing it.
func setNestedValue(dst map[string]any, parts []string, value any) {
	current := dst
	for _, part := range parts[:len(parts)-1] {
		next, ok := current[part].(map[string]any)
		if !ok || next == nil {
			next = make(map[string]any)
			current[part] = next
		}
		current = next
	}
	mergeValue(current, parts[len(parts)-1], value)
}

// mergeValue assigns key, merging recursively where both sides are containers so
// that a subtree already built by an earlier spelling keeps its siblings.
//
// Sibling attributes can diverge at any depth: {"contact":{"snapshot":{"a":1}}}
// plus {"contact.snapshot":{"b":2}} names two different attributes and the
// writer stores both, so both values must stay visible or one is stored without
// ever being validated.
//
// Iteration order over an object's keys is irrelevant to the outcome: each key
// is merged independently, so no key's result depends on another's.
func mergeValue(dst map[string]any, key string, value any) {
	dst[key] = mergeAny(dst[key], value)
}

// mergeAny merges incoming onto existing, which is nil when nothing is there yet.
// Anything that is not a pair of like containers is replaced, which is where the
// later spelling wins.
//
// A typed nil map on the existing side is replaced rather than merged into:
// writing into one panics, and it holds nothing the merge could preserve.
func mergeAny(existing, incoming any) any {
	if target, ok := existing.(map[string]any); ok && target != nil {
		if source, ok := incoming.(map[string]any); ok {
			for key, value := range source {
				target[key] = mergeAny(target[key], value)
			}
			return target
		}
	}
	if target, ok := existing.([]any); ok {
		if source, ok := incoming.([]any); ok {
			return mergeSlices(target, source)
		}
	}
	return incoming
}

// mergeSlices merges two arrays element-wise by index, extending to the longer
// one and replacing any element that is not an object on both sides.
//
// Two spellings meeting at an array can name different attributes, exactly as
// they can at an object: {"requirement":{"areas":[{"note":…}]}} plus a literal
// "requirement.areas":[{"city":…}] names requirement.areas.note and
// requirement.areas.city, and the writer stores both. Replacing would leave one
// of them persisted without the validator ever seeing it, which is the whole
// property this document exists to provide.
//
// Index alignment is what the flattener already does — an array index is part of
// the EAV primary key, so .note at index 0 and .city at index 0 are different
// rows and both survive.
//
// Merging here cannot produce a wrong record: this document reaches the
// validator only, and the writer still receives the caller's original map. It
// can produce a wrong *verdict*, and does.
//
// The writer's dedupe replaces the whole logical attribute — every index — for a
// losing spelling, while this replaces per index. When the nested spelling is the
// longer one its surplus elements survive into the view and never into storage,
// so the view over-approximates: it may validate a value the writer discards. A
// shrinking-list patch over already-invalid stored data is therefore rejected on
// a value that will not exist after the write.
//
// Kept deliberately. Replacing under-approximates instead, leaving a persisted
// value unvalidated — the bypass #314 exists to close — and over-approximating is
// the safer of the two errors. The exact per-leaf-key rule is the right answer in
// the abstract and is not attempted: every previous refinement of this merge
// introduced a new defect. Pinned by
// TestNormalizeArrayMergeOverApproximatesShrinkingList, which carries the full
// reasoning; also in docs/error-handling.md, because it is operator-facing.
func mergeSlices(existing, incoming []any) []any {
	length := max(len(existing), len(incoming))

	merged := make([]any, length)
	for i := range merged {
		switch {
		case i >= len(incoming):
			merged[i] = existing[i]
		case i >= len(existing):
			merged[i] = incoming[i]
		default:
			merged[i] = mergeAny(existing[i], incoming[i])
		}
	}
	return merged
}
