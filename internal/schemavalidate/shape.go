package schemavalidate

import (
	"net/url"
	"reflect"
	"slices"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
)

// The container skeleton of a schema (#696). An update must not place an
// object or array where the schema declares a single value, in every
// validation mode, but which positions admit a container is the schema's
// structure to say: a primitive array an element-typed (pre-#204) ledger
// entry still stores, a nullable union, an anyOf of an object and an array
// of objects, items typed through a combinator, a $ref into another file.
// Rather than re-derive those answers from the document, the skeleton is a
// second schema that the library resolves and evaluates like any other. It
// keeps exactly the keywords that decide where a container may sit and
// drops every constraint on a single value, so it refuses a document only
// for its containers.
//
// It is a relaxation: every document the schema admits, the skeleton admits
// too. Dropping a constraint can only admit more, as long as it sits where a
// failure fails the document. Three keywords are not like that: oneOf fails
// when two branches match, which relaxed branches do more often, so it
// becomes an anyOf; not and if succeed when their subschema fails, so not is
// dropped and if/then/else becomes an anyOf of then and else. then and else
// without if are ignored, as the validator ignores them, and a missing
// branch is the empty schema. "type" is widened so that a single value of
// any JSON type is admitted wherever the schema admits a single value of
// some type, or where it declares a container: report-only mode keeps
// admitting a single value of the wrong type, which the write converts or
// refuses as it always has, and a single value where the schema declares an
// object stays the write's own refusal ("attribute is not defined"), at its
// own stage. What is left can refuse an object or an array only, never a
// single value.
//
// A relaxation must not lose what excludes a container, either (#696 review
// R1-F1). enum, const and not are dropped, but each can be all that keeps an
// array or object out: {"enum": [false]} admits no array, and neither does
// false ({"not": {}}) or {"not": {"type": "array"}}. So a schema whose enum
// or const lists no array, or whose not admits every array, keeps "type"
// without "array", and likewise for objects (containerKinds). A not whose
// subschema says more than a type is not judged and excludes nothing, which
// only admits more.
//
// The keywords that only constrain values are dropped with the rest: required
// (the required policy is the write's, #301), the numeric and string bounds,
// format, the item and property counts, contains, additionalProperties and
// patternProperties (an undefined attribute is the write's refusal),
// propertyNames, the unevaluated keywords and the dependent* family.
// Annotations are dropped too.
//
// Arrays are judged by one element (ShapeSchema.AdmitsArrangement), so a
// tuple's positions are merged into one items schema that admits what any
// of them does.
//
// A $ref can point anywhere in a document, a rewritten or dropped keyword
// included ("#/properties/a/oneOf/0"), so every subschema the skeleton does
// not keep in place is moved, as its own skeleton, into the $defs of its
// resource under a key naming its original location, and each reference
// into it is rewritten to land there (shape_refs.go), across a nested $id
// too. Where it applied, a reference to it takes its place. A schema whose
// skeleton still does not resolve keeps none (resolveShape), and its updates
// are judged as without a validator: by the ledger, which refuses an array
// at an attribute that is not a list.

// scalarTypes is every JSON type that is a single value.
var scalarTypes = []string{"boolean", "integer", "null", "number", "string"}

// containerTypes is every JSON type that is a container.
var containerTypes = []string{"array", "object"}

// shaper builds skeletons. base is the base URI of the resource being
// shaped, and docs finds the resources its references name, so that a
// reference is mapped onto the skeleton of its target's resource
// (mapPointer); without them a reference is mapped by its pointer alone.
type shaper struct {
	base *url.URL
	docs *refDocuments
}

// newShaper answers the shaper of the document root, whose base URI is base,
// reading the documents its references name through load (nil: none).
func newShaper(root *jsonschema.Schema, base string, load jsonschema.Loader) shaper {
	u, err := url.Parse(base)
	if err != nil || root == nil {
		return shaper{}
	}
	return shaper{base: u, docs: newRefDocuments(root, u, load)}
}

// document answers the skeleton of a schema document, a new schema; s is
// not modified. The document's root is a resource.
func (sh shaper) document(s *jsonschema.Schema) *jsonschema.Schema {
	if s == nil {
		return nil
	}
	return sh.resource(s)
}

// resource answers the skeleton of a resource root: the document's root or a
// subschema with an $id of its own. Pointers inside it are relative to it,
// so it holds what is moved out of place beneath it.
func (sh shaper) resource(s *jsonschema.Schema) *jsonschema.Schema {
	if startsResource(s) && sh.base != nil {
		sh.base = resourceBase(sh.base, s)
	}
	stash := make(map[string]*jsonschema.Schema)
	out := sh.body(s, "", stash)
	if len(stash) > 0 {
		if out.Defs == nil {
			out.Defs = make(map[string]*jsonschema.Schema, len(stash))
		}
		for key, moved := range stash {
			out.Defs[key] = moved
		}
	}
	return out
}

// node answers the skeleton of s, which sits at ptr in its resource.
func (sh shaper) node(s *jsonschema.Schema, ptr string, stash map[string]*jsonschema.Schema) *jsonschema.Schema {
	if s == nil {
		return nil
	}
	if startsResource(s) {
		return sh.resource(s)
	}
	return sh.body(s, ptr, stash)
}

// startsResource reports whether s's $id starts a resource: an $id that is
// not a draft-07 anchor ("#name").
func startsResource(s *jsonschema.Schema) bool {
	return s.ID != "" && !strings.HasPrefix(s.ID, "#")
}

func (sh shaper) body(s *jsonschema.Schema, ptr string, stash map[string]*jsonschema.Schema) *jsonschema.Schema {
	out := &jsonschema.Schema{
		ID:            s.ID,
		Schema:        s.Schema,
		Ref:           sh.reference(s.Ref),
		Anchor:        s.Anchor,
		DynamicAnchor: s.DynamicAnchor,
		DynamicRef:    sh.reference(s.DynamicRef),
		Defs:          sh.keptMap(s.Defs, ptr+"/$defs", stash),
		Definitions:   sh.keptMap(s.Definitions, ptr+"/definitions", stash),
		Types:         widenTypes(s),

		Items:      sh.items(s, ptr, stash),
		Properties: sh.keptMap(s.Properties, ptr+"/properties", stash),

		AllOf: sh.keptSlice(s.AllOf, ptr+"/allOf", stash),
		AnyOf: sh.keptSlice(s.AnyOf, ptr+"/anyOf", stash),
	}
	if len(s.OneOf) > 0 {
		out.AllOf = append(out.AllOf, &jsonschema.Schema{AnyOf: sh.movedSlice(s.OneOf, ptr+"/oneOf", stash)})
	}
	then := sh.moved(s.Then, ptr+"/then", stash)
	otherwise := sh.moved(s.Else, ptr+"/else", stash)
	if s.If != nil && (then != nil || otherwise != nil) {
		out.AllOf = append(out.AllOf, &jsonschema.Schema{AnyOf: []*jsonschema.Schema{orEmpty(then), orEmpty(otherwise)}})
	}
	sh.moveDropped(s, ptr, stash)
	return out
}

// moveDropped moves every subschema of a keyword the skeleton drops, so a
// reference into one still resolves.
func (sh shaper) moveDropped(s *jsonschema.Schema, ptr string, stash map[string]*jsonschema.Schema) {
	for keyword, sub := range map[string]*jsonschema.Schema{
		"if": s.If, "not": s.Not, "contains": s.Contains, "additionalProperties": s.AdditionalProperties,
		"propertyNames": s.PropertyNames, "unevaluatedItems": s.UnevaluatedItems,
		"unevaluatedProperties": s.UnevaluatedProperties, "contentSchema": s.ContentSchema,
	} {
		sh.moved(sub, ptr+"/"+keyword, stash)
	}
	sh.movedMap(s.PatternProperties, ptr+"/patternProperties", stash)
	sh.movedMap(s.DependentSchemas, ptr+"/dependentSchemas", stash)
	sh.movedMap(s.DependencySchemas, ptr+"/dependencies", stash)
}

// items answers the skeleton of what s admits as any element of an array. A
// tuple (prefixItems, or draft-07's array form of items) becomes one items
// schema admitting what any of its positions admits, so an element is
// judged the same at every index: a single element at index 0 stands for
// one at any index (ShapeSchema.AdmitsArrangement). Without a schema for the
// elements past the tuple, any element is admitted. Every element schema is
// moved, and the skeleton refers to it.
func (sh shaper) items(s *jsonschema.Schema, ptr string, stash map[string]*jsonschema.Schema) *jsonschema.Schema {
	tuple, tuplePtr := s.PrefixItems, ptr+"/prefixItems"
	rest := sh.moved(s.Items, ptr+"/items", stash)
	if len(s.ItemsArray) > 0 {
		tuple, tuplePtr = s.ItemsArray, ptr+"/items"
		rest = sh.moved(s.AdditionalItems, ptr+"/additionalItems", stash)
	} else {
		// Ignored beside a single items schema, as the validator ignores it.
		sh.moved(s.AdditionalItems, ptr+"/additionalItems", stash)
	}
	if len(tuple) == 0 {
		return rest
	}
	positions := sh.movedSlice(tuple, tuplePtr, stash)
	if rest == nil {
		return nil
	}
	return &jsonschema.Schema{AnyOf: append(positions, rest)}
}

// moved moves s's skeleton into stash under the key naming ptr, and answers
// a reference to it, or nil for no schema.
func (sh shaper) moved(s *jsonschema.Schema, ptr string, stash map[string]*jsonschema.Schema) *jsonschema.Schema {
	if s == nil {
		return nil
	}
	key := movedKey(ptr)
	stash[key] = sh.node(s, ptr, stash)
	return &jsonschema.Schema{Ref: "#/$defs/" + escapePointerSegment(key)}
}

func (sh shaper) movedSlice(in []*jsonschema.Schema, ptr string, stash map[string]*jsonschema.Schema) []*jsonschema.Schema {
	out := make([]*jsonschema.Schema, len(in))
	for i, s := range in {
		out[i] = orEmpty(sh.moved(s, ptr+"/"+itoa(i), stash))
	}
	return out
}

func (sh shaper) movedMap(in map[string]*jsonschema.Schema, ptr string, stash map[string]*jsonschema.Schema) {
	for key, s := range in {
		sh.moved(s, ptr+"/"+escapePointerSegment(key), stash)
	}
}

func (sh shaper) keptSlice(in []*jsonschema.Schema, ptr string, stash map[string]*jsonschema.Schema) []*jsonschema.Schema {
	if in == nil {
		return nil
	}
	out := make([]*jsonschema.Schema, len(in))
	for i, s := range in {
		out[i] = sh.node(s, ptr+"/"+itoa(i), stash)
	}
	return out
}

func (sh shaper) keptMap(in map[string]*jsonschema.Schema, ptr string, stash map[string]*jsonschema.Schema) map[string]*jsonschema.Schema {
	if in == nil {
		return nil
	}
	out := make(map[string]*jsonschema.Schema, len(in))
	for key, s := range in {
		out[key] = sh.node(s, ptr+"/"+escapePointerSegment(key), stash)
	}
	return out
}

func orEmpty(s *jsonschema.Schema) *jsonschema.Schema {
	if s == nil {
		return &jsonschema.Schema{}
	}
	return s
}

// widenTypes answers s's "type" with every single-value type added to it,
// and with the containers that enum, const or not exclude taken out of it
// (containerKinds): nil (no constraint) when s constrains neither, and
// otherwise every scalar type beside the container types both admit.
func widenTypes(s *jsonschema.Schema) []string {
	declared := s.Types
	if s.Type != "" {
		declared = []string{s.Type}
	}
	allowed, restricted := containerKinds(s)
	if len(declared) == 0 && !restricted {
		return nil
	}
	types := slices.Clone(scalarTypes)
	for _, t := range containerTypes {
		if (len(declared) == 0 || slices.Contains(declared, t)) && (!restricted || allowed[t]) {
			types = append(types, t)
		}
	}
	slices.Sort(types)
	return types
}

// containerKinds answers which container types s's enum, const and not
// admit, and whether they restrict them at all. An enum or const admits the
// containers among its values. A not admits no container of a type its
// subschema admits whole: the empty schema admits every value, and a
// subschema of a "type" alone admits every value of its types. Any other
// not is not judged.
func containerKinds(s *jsonschema.Schema) (map[string]bool, bool) {
	allowed := map[string]bool{"array": true, "object": true}
	restricted := false
	restrict := func(admits map[string]bool) {
		restricted = true
		for _, t := range containerTypes {
			allowed[t] = allowed[t] && admits[t]
		}
	}
	if s.Const != nil {
		restrict(kindsOfValues([]any{*s.Const}))
	}
	if s.Enum != nil {
		restrict(kindsOfValues(s.Enum))
	}
	if s.Not != nil {
		if whole := admittedWhole(s.Not); whole["array"] || whole["object"] {
			restrict(map[string]bool{"array": !whole["array"], "object": !whole["object"]})
		}
	}
	return allowed, restricted
}

// kindsOfValues answers the container types among values.
func kindsOfValues(values []any) map[string]bool {
	kinds := make(map[string]bool)
	for _, v := range values {
		switch v.(type) {
		case []any:
			kinds["array"] = true
		case map[string]any:
			kinds["object"] = true
		}
	}
	return kinds
}

// admittedWhole answers the container types s admits every value of: all of
// them for the empty schema, the declared ones for a schema of "type"
// alone, and none known otherwise.
func admittedWhole(s *jsonschema.Schema) map[string]bool {
	rest := *s
	declared := rest.Types
	if rest.Type != "" {
		declared = []string{rest.Type}
	}
	rest.Type, rest.Types = "", nil
	if !reflect.DeepEqual(rest, jsonschema.Schema{}) {
		return nil
	}
	whole := make(map[string]bool)
	for _, t := range containerTypes {
		whole[t] = len(declared) == 0 || slices.Contains(declared, t)
	}
	return whole
}
