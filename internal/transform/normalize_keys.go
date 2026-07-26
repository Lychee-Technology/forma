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
// arrays comes from schemavalidate, the only place that knows which paths are
// arrays; the metadata cache records requirement.areas.city and contact.email
// identically. Passing a nil set is safe and means "nothing known to be an
// array".
//
// When both spellings are present the literal wins, matching encoding/json's
// duplicate-key semantics and the writer's own last-spelling-wins rule. Keys are
// applied in sorted order, and for any dotted name X.Y the shorter spelling X
// sorts before X.Y, so the longer, more specific one is applied last.
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
	return normalizeMap(data, "", cache, arrays)
}

// normalizeMap rebuilds src with dotted keys expanded, where prefix is the
// dotted attribute-name prefix of src's own position in the document.
func normalizeMap(
	src map[string]any,
	prefix string,
	cache forma.SchemaAttributeCache,
	arrays schemavalidate.ArrayPaths,
) map[string]any {
	dst := make(map[string]any, len(src))

	keys := make([]string, 0, len(src))
	for key := range src {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		name := joinName(prefix, key)
		value := normalizeValue(src[key], name, cache, arrays)

		parts := strings.Split(key, ".")
		if shouldExpand(dst, parts, prefix, name, cache, arrays) {
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
func normalizeValue(value any, name string, cache forma.SchemaAttributeCache, arrays schemavalidate.ArrayPaths) any {
	switch typed := value.(type) {
	case map[string]any:
		return normalizeMap(typed, name, cache, arrays)
	case []any:
		return normalizeSlice(typed, name, cache, arrays)
	default:
		return value
	}
}

// normalizeSlice normalizes each element. Array elements keep their parent's
// name because an index is not part of an attribute name — flattenToAttributes
// carries indices separately and recurses into elements with the path unchanged
// — so {"tags":[{"a.b":1}]} must expand the same as {"tags":{"a.b":1}} would.
func normalizeSlice(src []any, prefix string, cache forma.SchemaAttributeCache, arrays schemavalidate.ArrayPaths) []any {
	dst := make([]any, len(src))
	for i, item := range src {
		dst[i] = normalizeValue(item, prefix, cache, arrays)
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
// The last two overlap but neither subsumes the other. The schema set is the only
// way to know about an array the payload does not happen to contain, and the
// payload check is the only way to catch an array the derivation cannot see —
// one declared behind a $ref, or one the caller sent where the schema says
// object.
func shouldExpand(
	dst map[string]any,
	parts []string,
	prefix string,
	name string,
	cache forma.SchemaAttributeCache,
	arrays schemavalidate.ArrayPaths,
) bool {
	return len(parts) > 1 &&
		isKnownAttributeOrParent(name, cache) &&
		!arrays.CrossesBelow(prefix, name) &&
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
func setNestedValue(dst map[string]any, parts []string, value any) {
	current := dst
	for _, part := range parts[:len(parts)-1] {
		next, ok := current[part].(map[string]any)
		if !ok {
			next = make(map[string]any)
			current[part] = next
		}
		current = next
	}
	mergeValue(current, parts[len(parts)-1], value)
}

// mergeValue assigns key, merging recursively where both sides are objects so
// that a subtree already built by an earlier spelling keeps its siblings.
//
// Sibling attributes can diverge at any depth: {"contact":{"snapshot":{"a":1}}}
// plus {"contact.snapshot":{"b":2}} names two different attributes and the
// writer stores both, so both values must stay visible or one is stored without
// ever being validated. Everything else — two arrays included — is replaced,
// because the later spelling replaces the whole attribute the writer stores.
//
// Iteration order over an object's keys is irrelevant to the outcome: each key
// is merged independently, so no key's result depends on another's.
func mergeValue(dst map[string]any, key string, value any) {
	existing, haveMap := dst[key].(map[string]any)
	incoming, incomingMap := value.(map[string]any)
	if haveMap && incomingMap {
		for k, v := range incoming {
			mergeValue(existing, k, v)
		}
		return
	}
	dst[key] = value
}
