package schemavalidate

import (
	"net/url"
	"strconv"
	"strings"
)

// References in the skeleton (shape.go). The skeleton keeps a subschema in
// place under properties, $defs, definitions, allOf and anyOf, and moves
// every other one into its resource's $defs, under movedKey of the JSON
// Pointer it had there. So a pointer into the schema maps to one into the
// skeleton segment by segment: it is kept while it walks kept keywords, and
// restarts at the moved key when it enters a moved subschema. The mapping
// reads the pointer alone, never the document, so a $ref into another file
// maps the same way, onto that file's skeleton (shapeResolveOptions).
//
// What it does not see is an $id between the resource the pointer starts at
// and its target: a subschema with an $id is a resource of its own, which
// holds what is moved beneath it. A pointer that crosses one into a moved
// subschema does not resolve in the skeleton, and the schema keeps a
// partial skeleton (resolveShape).

// movedPrefix starts the $defs key of a moved subschema, so it cannot be
// taken for a definition of the schema's own.
const movedPrefix = "forma-shape"

// movedKey is the $defs key of the subschema moved from ptr.
func movedKey(ptr string) string {
	return movedPrefix + ptr
}

// reference answers a $ref or $dynamicRef of the schema as the skeleton
// holds it: with a JSON Pointer fragment mapped onto the skeleton, or
// dropped for a partial skeleton. A reference that does not parse is kept;
// it fails resolution as it would in the schema.
func (sh shaper) reference(ref string) string {
	if ref == "" || !sh.followRefs {
		return ""
	}
	u, err := url.Parse(ref)
	if err != nil || !strings.HasPrefix(u.Fragment, "/") {
		return ref
	}
	u.Fragment = mapPointer(u.Fragment)
	u.RawFragment = ""
	return u.String()
}

// pointerKeywords classifies the keywords a pointer can walk through: kept
// in place or moved, and followed by a name, an index, or the subschema
// itself.
var (
	keptNamed   = map[string]bool{"properties": true, "$defs": true, "definitions": true}
	keptIndexed = map[string]bool{"allOf": true, "anyOf": true}
	movedNamed  = map[string]bool{"patternProperties": true, "dependentSchemas": true, "dependencies": true}
	movedIndex  = map[string]bool{"oneOf": true, "prefixItems": true}
	movedSingle = map[string]bool{
		"not": true, "if": true, "then": true, "else": true, "contains": true, "additionalProperties": true,
		"propertyNames": true, "unevaluatedItems": true, "unevaluatedProperties": true, "contentSchema": true,
		"additionalItems": true,
	}
)

// mapPointer maps a JSON Pointer into a resource of the schema onto the
// same subschema in the resource's skeleton. Segments stay escaped
// throughout, as the original pointer spells them, which is how the moved
// keys spell them too. A segment it does not know is kept with the rest of
// the pointer, which then fails as it would in the schema.
func mapPointer(ptr string) string {
	segments := strings.Split(ptr, "/")[1:]
	var out []string
	original := ""
	moveTo := func(consumed ...string) {
		original += "/" + strings.Join(consumed, "/")
		out = []string{"$defs", escapePointerSegment(movedKey(original))}
	}
	for i := 0; i < len(segments); {
		keyword := segments[i]
		next, hasNext := "", i+1 < len(segments)
		if hasNext {
			next = segments[i+1]
		}
		switch {
		case (keptNamed[keyword] || keptIndexed[keyword]) && hasNext:
			original += "/" + keyword + "/" + next
			out = append(out, keyword, next)
			i += 2
		case (movedNamed[keyword] || movedIndex[keyword]) && hasNext:
			moveTo(keyword, next)
			i += 2
		case keyword == "items" && hasNext && isIndex(next):
			moveTo(keyword, next)
			i += 2
		case keyword == "items" || movedSingle[keyword]:
			moveTo(keyword)
			i++
		default:
			out = append(out, segments[i:]...)
			i = len(segments)
		}
	}
	return "/" + strings.Join(out, "/")
}

// isIndex reports a pointer segment that indexes an array.
func isIndex(segment string) bool {
	if segment == "" || (len(segment) > 1 && segment[0] == '0') {
		return false
	}
	_, err := strconv.Atoi(segment)
	return err == nil
}

var pointerEscaper = strings.NewReplacer("~", "~0", "/", "~1")

// escapePointerSegment escapes one JSON Pointer segment (RFC 6901).
func escapePointerSegment(s string) string {
	return pointerEscaper.Replace(s)
}

func itoa(i int) string {
	return strconv.Itoa(i)
}
