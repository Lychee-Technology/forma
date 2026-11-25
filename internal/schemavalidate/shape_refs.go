package schemavalidate

import (
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
)

// References in the skeleton (shape.go). The skeleton keeps a subschema in
// place under properties, $defs, definitions, allOf and anyOf, and moves
// every other one into its resource's $defs, under movedKey of the JSON
// Pointer it had there. So a pointer into the schema maps to one into the
// skeleton segment by segment: it is kept while it walks kept keywords, and
// restarts at the moved key when it enters a moved subschema.
//
// A subschema with an $id of its own is a resource, which holds what is
// moved beneath it, under pointers relative to it. A pointer can walk into
// one ("#/$defs/inner/properties/x/oneOf/0" with an $id at inner), so the
// mapping walks the original document beside the pointer: on entering a
// resource, what follows restarts there, and a moved subschema beneath it is
// found in that resource's $defs (#696 review R2-F1). The document is the
// resource the reference names, resolved against its base URI as the
// library resolves it: the document being shaped, a resource with an $id
// inside it, or another file, read through the same loader. A reference
// whose document cannot be read is mapped by the pointer alone, which is
// right unless the pointer crosses an $id; then the skeleton does not
// resolve, and the schema keeps none (resolveShape).

// movedPrefix starts the $defs key of a moved subschema, so it cannot be
// taken for a definition of the schema's own.
const movedPrefix = "forma-shape"

// movedKey is the $defs key of the subschema moved from ptr.
func movedKey(ptr string) string {
	return movedPrefix + ptr
}

// refDocuments finds the original resource a reference names, by its
// absolute URI without fragment. One serves one document's skeleton.
type refDocuments struct {
	resources map[string]*jsonschema.Schema
	load      jsonschema.Loader
	tried     map[string]bool
}

// newRefDocuments indexes the resources of the document root, whose base URI
// is base, and reads other documents through load (nil: none).
func newRefDocuments(root *jsonschema.Schema, base *url.URL, load jsonschema.Loader) *refDocuments {
	d := &refDocuments{resources: make(map[string]*jsonschema.Schema), load: load, tried: make(map[string]bool)}
	d.index(root, base)
	return d
}

// index records s, at base, and every resource beneath it.
func (d *refDocuments) index(s *jsonschema.Schema, base *url.URL) {
	if base == nil {
		return
	}
	d.resources[fragless(base)] = s
	var walk func(s *jsonschema.Schema, base *url.URL)
	walk = func(s *jsonschema.Schema, base *url.URL) {
		for _, child := range subschemas(s) {
			childBase := base
			if startsResource(child) {
				childBase = resourceBase(base, child)
				if childBase == nil {
					continue
				}
				d.resources[fragless(childBase)] = child
			}
			walk(child, childBase)
		}
	}
	walk(s, base)
}

// resource answers the original resource at u, an absolute URI without
// fragment, reading its document when it is another file; nil when none is
// known.
func (d *refDocuments) resource(u *url.URL) *jsonschema.Schema {
	key := fragless(u)
	if s, ok := d.resources[key]; ok || d.load == nil || d.tried[key] {
		return s
	}
	d.tried[key] = true
	if s, err := d.load(u); err == nil && s != nil {
		d.index(s, u)
	}
	return d.resources[key]
}

// resourceBase answers the base URI a resource's $id establishes beneath
// base, or nil when it does not parse.
func resourceBase(base *url.URL, s *jsonschema.Schema) *url.URL {
	id, err := url.Parse(s.ID)
	if err != nil {
		return nil
	}
	if base == nil {
		return id
	}
	return base.ResolveReference(id)
}

func fragless(u *url.URL) string {
	c := *u
	c.Fragment, c.RawFragment = "", ""
	return c.String()
}

// subschemas answers every subschema directly beneath s, whatever keyword
// holds it, found by field type as walkSubschemas finds them.
func subschemas(s *jsonschema.Schema) []*jsonschema.Schema {
	if s == nil {
		return nil
	}
	var out []*jsonschema.Schema
	v := reflect.ValueOf(s).Elem()
	for i := range v.NumField() {
		f := v.Field(i)
		if !f.CanInterface() {
			continue
		}
		switch f.Type() {
		case schemaPtrType:
			if !f.IsNil() {
				out = append(out, f.Interface().(*jsonschema.Schema))
			}
		case schemaSliceType:
			for _, c := range f.Interface().([]*jsonschema.Schema) {
				if c != nil {
					out = append(out, c)
				}
			}
		case schemaMapType:
			for _, c := range f.Interface().(map[string]*jsonschema.Schema) {
				if c != nil {
					out = append(out, c)
				}
			}
		}
	}
	return out
}

// reference answers a $ref or $dynamicRef of the schema as the skeleton
// holds it: with a JSON Pointer fragment mapped onto the skeleton. A
// reference that does not parse is kept; it fails resolution as it would in
// the schema.
func (sh shaper) reference(ref string) string {
	if ref == "" {
		return ""
	}
	u, err := url.Parse(ref)
	if err != nil || !strings.HasPrefix(u.Fragment, "/") {
		return ref
	}
	u.Fragment = mapPointer(sh.target(u), u.Fragment)
	u.RawFragment = ""
	return u.String()
}

// target answers the original resource the reference u names, nil when it
// is not known.
func (sh shaper) target(u *url.URL) *jsonschema.Schema {
	if sh.docs == nil || sh.base == nil {
		return nil
	}
	return sh.docs.resource(sh.base.ResolveReference(u))
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

// mapPointer maps a JSON Pointer into the resource root onto the same
// subschema in the resource's skeleton. root is the original resource, nil
// when it is not known: the pointer alone is then mapped, as if it crossed
// no $id. Segments stay escaped throughout, as the original pointer spells
// them, which is how the moved keys spell them too. A segment it does not
// know is kept with the rest of the pointer, which then fails as it would in
// the schema.
func mapPointer(root *jsonschema.Schema, ptr string) string {
	segments := strings.Split(ptr, "/")[1:]
	var resourceAt, out []string
	original, node := "", root
	for i := 0; i < len(segments); {
		next, hasNext := "", i+1 < len(segments)
		if hasNext {
			next = segments[i+1]
		}
		consumed, moved := pointerStep(segments[i], next, hasNext)
		if consumed == 0 {
			out = append(out, segments[i:]...)
			break
		}
		walked := segments[i : i+consumed]
		original += "/" + strings.Join(walked, "/")
		if moved {
			out = append(append([]string(nil), resourceAt...), "$defs", escapePointerSegment(movedKey(original)))
		} else {
			out = append(out, walked...)
		}
		node = childAt(node, walked)
		if node != nil && startsResource(node) {
			resourceAt, original = append([]string(nil), out...), ""
		}
		i += consumed
	}
	return "/" + strings.Join(out, "/")
}

// pointerStep answers how many segments the keyword at a pointer segment
// takes, 0 for one it does not know, and whether the skeleton moves the
// subschema they reach.
func pointerStep(keyword, next string, hasNext bool) (consumed int, moved bool) {
	switch {
	case (keptNamed[keyword] || keptIndexed[keyword]) && hasNext:
		return 2, false
	case (movedNamed[keyword] || movedIndex[keyword]) && hasNext:
		return 2, true
	case keyword == "items" && hasNext && isIndex(next):
		return 2, true
	case keyword == "items" || movedSingle[keyword]:
		return 1, true
	}
	return 0, false
}

// childAt answers the subschema of s that the escaped pointer segments
// walked (one keyword, and its name or index) reach, nil when s is nil or
// has none there.
func childAt(s *jsonschema.Schema, walked []string) *jsonschema.Schema {
	if s == nil {
		return nil
	}
	keyword := walked[0]
	if len(walked) == 1 {
		return singleSubschema(s, keyword)
	}
	name := unescapePointerSegment(walked[1])
	switch keyword {
	case "properties":
		return s.Properties[name]
	case "$defs":
		return s.Defs[name]
	case "definitions":
		return s.Definitions[name]
	case "patternProperties":
		return s.PatternProperties[name]
	case "dependentSchemas":
		return s.DependentSchemas[name]
	case "dependencies":
		return s.DependencySchemas[name]
	case "allOf":
		return indexed(s.AllOf, name)
	case "anyOf":
		return indexed(s.AnyOf, name)
	case "oneOf":
		return indexed(s.OneOf, name)
	case "prefixItems":
		return indexed(s.PrefixItems, name)
	case "items":
		return indexed(s.ItemsArray, name)
	}
	return nil
}

// singleSubschema answers the subschema s holds under a keyword of one.
func singleSubschema(s *jsonschema.Schema, keyword string) *jsonschema.Schema {
	return map[string]*jsonschema.Schema{
		"items": s.Items, "not": s.Not, "if": s.If, "then": s.Then, "else": s.Else, "contains": s.Contains,
		"additionalProperties": s.AdditionalProperties, "propertyNames": s.PropertyNames,
		"unevaluatedItems": s.UnevaluatedItems, "unevaluatedProperties": s.UnevaluatedProperties,
		"contentSchema": s.ContentSchema, "additionalItems": s.AdditionalItems,
	}[keyword]
}

func indexed(in []*jsonschema.Schema, segment string) *jsonschema.Schema {
	i, err := strconv.Atoi(segment)
	if err != nil || i < 0 || i >= len(in) {
		return nil
	}
	return in[i]
}

// isIndex reports a pointer segment that indexes an array.
func isIndex(segment string) bool {
	if segment == "" || (len(segment) > 1 && segment[0] == '0') {
		return false
	}
	_, err := strconv.Atoi(segment)
	return err == nil
}

var (
	pointerEscaper   = strings.NewReplacer("~", "~0", "/", "~1")
	pointerUnescaper = strings.NewReplacer("~1", "/", "~0", "~")
)

// escapePointerSegment escapes one JSON Pointer segment (RFC 6901).
func escapePointerSegment(s string) string {
	return pointerEscaper.Replace(s)
}

func unescapePointerSegment(s string) string {
	return pointerUnescaper.Replace(s)
}

func itoa(i int) string {
	return strconv.Itoa(i)
}
