package schemavalidate

import (
	"slices"

	"github.com/google/jsonschema-go/jsonschema"
)

// ArrayPaths is the set of dotted attribute paths a schema declares as arrays.
//
// It exists because dotted attribute names are ambiguous about arrays and the
// metadata cache cannot resolve that ambiguity: requirement.areas.city and
// contact.email are recorded identically, and the attribute generator computes
// array membership only to discard it. The schema is the only loaded source of
// truth, and this package already holds every schema resolved, so the set is
// derived here once at construction rather than re-walked per request.
//
// Paths use the attribute-name convention: an array index is not part of a name,
// so array elements keep their parent's path. That is what makes
// requirement.areas the recorded path for items of requirement.areas, matching
// the attribute name requirement.areas.city.
type ArrayPaths map[string]struct{}

// CrossesBelow reports whether name lies beneath an array declared *below*
// prefix, where prefix is the document position the caller is already at. Pass
// "" to ask about the whole name.
//
// The position matters because a dotted key inside an array element is already
// past that array: within an element of propertyInterests, "snapshot.code" nests
// legally, even though the absolute name propertyInterests.snapshot.code does
// cross an array. Only arrays strictly between prefix and the name's own leaf can
// make nesting impossible, so paths at or above prefix are skipped.
//
// Only proper prefixes count at the other end too. A name that *is* an array
// path — a literal "contact.phones" holding the array itself — describes the
// array rather than something under it, and nesting it is both correct and
// necessary for the value to be validated.
func (p ArrayPaths) CrossesBelow(prefix, name string) bool {
	if len(p) == 0 {
		return false
	}
	for i, char := range name {
		if char != '.' || i <= len(prefix) {
			continue
		}
		if _, ok := p[name[:i]]; ok {
			return true
		}
	}
	return false
}

// ArrayPaths returns the array paths declared by the schema registered for
// schemaID, or nil when there is no such schema.
//
// Nil is a safe answer rather than an error: it means "nothing known to be an
// array", which leaves callers with the behaviour they had before this set
// existed. A nil receiver is treated the same way, matching Validate, because
// callers may hold a *Validator that is nil when validation is unconfigured.
func (v *Validator) ArrayPaths(schemaID int16) ArrayPaths {
	if v == nil {
		return nil
	}
	return v.arrays[schemaID]
}

// deriveArrayPaths collects every dotted path the schema declares as an array.
//
// The traversal is name-carrying, which is why it does not reuse walkSubschemas:
// that one finds subschemas by field type and so cannot know the property names
// a path is built from. It descends named properties, array items, and the
// allOf/anyOf/oneOf combinators.
//
// A $ref is not followed. The library keeps resolved targets in an unexported
// side table, so a path-carrying walk cannot reach them; an array declared
// behind a $ref is therefore not recorded, and a dotted key under it falls back
// to payload-shape detection in the normalizer.
func deriveArrayPaths(s *jsonschema.Schema) ArrayPaths {
	paths := make(ArrayPaths)
	collectArrayPaths(s, "", paths)
	if len(paths) == 0 {
		return nil
	}
	return paths
}

func collectArrayPaths(s *jsonschema.Schema, path string, out ArrayPaths) {
	if s == nil {
		return
	}
	if path != "" && declaresArray(s) {
		out[path] = struct{}{}
	}

	for name, child := range s.Properties {
		collectArrayPaths(child, childPath(path, name), out)
	}
	// Items keep the parent's path: an index is not part of an attribute name, so
	// the city of requirement.areas[0] is requirement.areas.city.
	collectArrayPaths(s.Items, path, out)
	for _, child := range slices.Concat(s.AllOf, s.AnyOf, s.OneOf) {
		collectArrayPaths(child, path, out)
	}
}

// declaresArray covers both spellings the library parses "type" into: a single
// value in Type, and a union in Types.
func declaresArray(s *jsonschema.Schema) bool {
	return s.Type == "array" || slices.Contains(s.Types, "array")
}

func childPath(path, name string) string {
	if path == "" {
		return name
	}
	return path + "." + name
}
