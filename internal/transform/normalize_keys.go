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
// When both spellings are present the literal wins, matching encoding/json's
// duplicate-key semantics and the rule established in #312 — on the update path
// the literal is the caller's explicit value while the nested form was rebuilt
// from storage. Keys are walked in sorted order, and for any dotted name X.Y the
// nested key X sorts before the literal X.Y, so the literal is applied last.
//
// Only keys the metadata cache knows as leaf attributes are expanded; an unknown
// dotted key is left alone so the existing "attribute is not defined" error
// still reports it. A schema property literally named "a.b" would be ambiguous
// with the nested path a -> b; no shipped schema has one.
//
// The input is never mutated.
func NormalizeDottedKeys(data map[string]any, cache forma.SchemaAttributeCache) map[string]any {
	return normalizeInto(data, nil, cache)
}

func normalizeInto(src map[string]any, path []string, cache forma.SchemaAttributeCache) map[string]any {
	dst := make(map[string]any, len(src))

	keys := make([]string, 0, len(src))
	for key := range src {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		value := src[key]
		if nested, ok := value.(map[string]any); ok {
			value = normalizeInto(nested, append(path, key), cache)
		}

		parts := strings.Split(key, ".")
		if len(parts) > 1 && isLeafAttribute(append(path, parts...), cache) {
			setNestedValue(dst, parts, value)
			continue
		}
		mergeValue(dst, key, value)
	}
	return dst
}

// isLeafAttribute reports whether the joined path names an attribute the schema
// defines, which is what makes expansion safe rather than a guess.
func isLeafAttribute(path []string, cache forma.SchemaAttributeCache) bool {
	_, ok := cache[strings.Join(path, ".")]
	return ok
}

// setNestedValue walks parts, creating intermediate maps, and assigns the leaf.
// An existing non-map value along the path is replaced, because the dotted
// spelling is the later — and therefore winning — one.
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
	current[parts[len(parts)-1]] = value
}

// mergeValue assigns key, merging into an existing nested map rather than
// replacing it, so a nested object already built by an earlier expansion keeps
// its siblings.
func mergeValue(dst map[string]any, key string, value any) {
	existing, haveMap := dst[key].(map[string]any)
	incoming, incomingMap := value.(map[string]any)
	if haveMap && incomingMap {
		for k, v := range incoming {
			existing[k] = v
		}
		return
	}
	dst[key] = value
}
