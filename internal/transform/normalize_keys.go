package transform

import (
	"sort"
	"strings"

	"github.com/lychee-technology/forma"
)

// NormalizeDottedKeys rewrites literal dotted keys into their nested paths.
//
// Attribute names in this codebase are dotted, so a caller reading the metadata
// may address contact.email either as a nested object or as a literal key. Left
// literal, JSON Schema treats it as an unknown property: with no
// additionalProperties declared it passes validation and its value is never
// examined, which would make validation trivially bypassable (#314). Expanding
// it first means the value is validated like any other.
//
// Interior paths are expanded too, not just leaf attributes: {"contact.snapshot":
// {"code": …}} hides "code" from validation exactly the way a literal leaf key
// hides its own value. The test is isKnownAttributeOrParent — the same predicate
// flattenToAttributes uses — so a name is expanded when the schema defines it or
// defines something beneath it.
//
// When both spellings are present the literal wins, matching encoding/json's
// duplicate-key semantics and the rule established in #312 — on the update path
// the literal is the caller's explicit value while the nested form was rebuilt
// from storage. Keys are applied in sorted order, and for any dotted name X.Y
// the shorter spelling X sorts before X.Y, so the longer, more specific one is
// applied last. Expanding interior paths is what makes that ordering decisive:
// with every spelling written into one nested subtree, precedence is settled
// here rather than left to where the flattener later re-sorts the result. An
// intermediate spelling left flat would sort after the expanded subtree and win
// the downstream dedupe, inverting precedence for any attribute deeper than two
// segments.
//
// Only names the metadata cache knows are expanded; an unknown dotted key is
// left alone so the existing "attribute is not defined" error still reports it.
// A schema property literally named "a.b" would be ambiguous with the nested
// path a -> b; no shipped schema has one.
//
// The input is never mutated: maps and slices are rebuilt rather than shared.
func NormalizeDottedKeys(data map[string]any, cache forma.SchemaAttributeCache) map[string]any {
	if data == nil {
		// Preserved rather than turned into an empty map: ToAttributes treats a
		// nil document as "no attributes" and returns before required-attribute
		// validation, so materializing a map here would change that path.
		return nil
	}
	return normalizeMap(data, "", cache)
}

// normalizeMap rebuilds src with dotted keys expanded, where prefix is the
// dotted attribute-name prefix of src's own position in the document.
func normalizeMap(src map[string]any, prefix string, cache forma.SchemaAttributeCache) map[string]any {
	dst := make(map[string]any, len(src))

	keys := make([]string, 0, len(src))
	for key := range src {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		name := joinName(prefix, key)
		value := normalizeValue(src[key], name, cache)

		parts := strings.Split(key, ".")
		if len(parts) > 1 && isKnownAttributeOrParent(name, cache) && canSetNestedValue(dst, parts) {
			setNestedValue(dst, parts, value)
			continue
		}
		mergeValue(dst, key, value)
	}
	return dst
}

// normalizeValue descends into containers and copies everything else through.
// Rebuilding both maps and slices is what keeps the caller's document unmutated:
// merging writes into the result, and the result must share nothing with it.
func normalizeValue(value any, name string, cache forma.SchemaAttributeCache) any {
	switch typed := value.(type) {
	case map[string]any:
		return normalizeMap(typed, name, cache)
	case []any:
		return normalizeSlice(typed, name, cache)
	default:
		return value
	}
}

// normalizeSlice normalizes each element. Array elements keep their parent's
// name because an index is not part of an attribute name — flattenToAttributes
// carries indices separately and recurses into elements with the path unchanged
// — so {"tags":[{"a.b":1}]} must expand the same as {"tags":{"a.b":1}} would.
func normalizeSlice(src []any, prefix string, cache forma.SchemaAttributeCache) []any {
	dst := make([]any, len(src))
	for i, item := range src {
		dst[i] = normalizeValue(item, prefix, cache)
	}
	return dst
}

func joinName(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + "." + key
}

// canSetNestedValue reports whether parts can be expanded without overwriting a
// non-map value already sitting on the path.
//
// {"contact":"SCALAR","contact.email":"x"} is rejected today, because "contact"
// is not a defined attribute. Expanding the literal over the scalar would make
// both checks pass and silently drop the caller's "SCALAR": a type error would
// become a 200 with missing data. Leaving the literal unexpanded keeps the
// existing rejection. Checked before any writing, so a conflict discovered
// mid-path cannot leave half-built maps behind.
func canSetNestedValue(dst map[string]any, parts []string) bool {
	current := dst
	for _, part := range parts[:len(parts)-1] {
		existing, present := current[part]
		if !present {
			return true
		}
		next, isMap := existing.(map[string]any)
		if !isMap {
			return false
		}
		current = next
	}
	return true
}

// setNestedValue walks parts, creating intermediate maps, and merges the leaf.
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
// Recursive rather than one level deep because sibling attributes can diverge at
// any depth: {"contact":{"snapshot":{"a":1}}} plus {"contact.snapshot":{"b":2}}
// names two different attributes, and the flattener would write both. Only a
// scalar leaf is overwritten, which is where the later spelling wins.
//
// Iteration order over incoming is irrelevant to the outcome: each key is merged
// independently, so no key's result depends on another's.
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
