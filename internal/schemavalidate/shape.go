package schemavalidate

import (
	"reflect"
	"slices"

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
// dropped whole and if/then/else becomes an anyOf of then and else. "type" is widened so that a single value of any
// JSON type is admitted wherever the schema admits a single value of some
// type, or where it declares a container: report-only mode keeps admitting a
// single value of the wrong type, which the write converts or refuses as it
// always has, and a single value where the schema declares an object stays
// the write's own refusal ("attribute is not defined"), at its own stage.
// What is left can refuse an object or an array only, never a single value.
//
// The keywords that only constrain values are dropped with the rest: required
// (the required policy is the write's, #301), enum and const, the numeric
// and string bounds, format, the item and property counts, contains,
// additionalProperties and patternProperties (an undefined attribute is the
// write's refusal), propertyNames, the unevaluated keywords and the
// dependent* family. Annotations are dropped too.
//
// Arrays are judged by one element (ShapeSchema.AdmitsArrangement), so a
// tuple's positions are merged into one items schema that admits what any
// of them does.
//
// A $ref that points into a dropped or rewritten keyword ("#/oneOf/0",
// "#/properties/pair/prefixItems/1") no longer resolves in the skeleton. New
// then keeps no skeleton for the schema, and an update of it takes no array
// at an attribute that is not a list, as with no validator
// (transform.CheckUpdateValues).

// scalarTypes is every JSON type that is a single value.
var scalarTypes = []string{"boolean", "integer", "null", "number", "string"}

// shapeOf answers s's container skeleton, a new schema; s is not modified.
func shapeOf(s *jsonschema.Schema) *jsonschema.Schema {
	if s == nil {
		return nil
	}
	out := &jsonschema.Schema{
		ID:            s.ID,
		Schema:        s.Schema,
		Ref:           s.Ref,
		Anchor:        s.Anchor,
		DynamicAnchor: s.DynamicAnchor,
		DynamicRef:    s.DynamicRef,
		Defs:          shapeMap(s.Defs),
		Definitions:   shapeMap(s.Definitions),
		Types:         widenTypes(s),

		Items:      shapeItems(s),
		Properties: shapeMap(s.Properties),

		AllOf: shapeSlice(s.AllOf),
		AnyOf: shapeSlice(s.AnyOf),
	}
	if len(s.OneOf) > 0 {
		out.AllOf = append(out.AllOf, &jsonschema.Schema{AnyOf: shapeSlice(s.OneOf)})
	}
	if s.Then != nil && s.Else != nil {
		// With only one branch, the other is the empty schema and the anyOf
		// admits everything, so there is nothing to keep.
		out.AllOf = append(out.AllOf, &jsonschema.Schema{AnyOf: []*jsonschema.Schema{shapeOf(s.Then), shapeOf(s.Else)}})
	}
	return out
}

// shapeItems answers the skeleton of what s admits as any element of an
// array. A tuple (prefixItems, or draft-07's array form of items) becomes
// one items schema admitting what any of its positions admits, so an
// element is judged the same at every index: a single element at index 0
// stands for one at any index (ShapeSchema.AdmitsArrangement). Without a
// schema for the elements past the tuple, any element is admitted; false
// there (a closed tuple) adds nothing.
func shapeItems(s *jsonschema.Schema) *jsonschema.Schema {
	tuple, rest := s.PrefixItems, s.Items
	if len(s.ItemsArray) > 0 {
		tuple, rest = s.ItemsArray, s.AdditionalItems
	}
	if len(tuple) == 0 {
		return shapeOf(rest)
	}
	if rest == nil {
		return nil
	}
	positions := shapeSlice(tuple)
	if !isFalseSchema(rest) {
		positions = append(positions, shapeOf(rest))
	}
	return &jsonschema.Schema{AnyOf: positions}
}

// isFalseSchema reports the schema false, which admits nothing: past a
// tuple it closes the tuple, and adds no element shape to merge.
func isFalseSchema(s *jsonschema.Schema) bool {
	return reflect.DeepEqual(*s, jsonschema.Schema{Not: &jsonschema.Schema{}})
}

// widenTypes answers s's "type" with every single-value type added to it:
// nil (no constraint) when s declares none, and otherwise the types s
// declares for a container, if any, beside every scalar type.
func widenTypes(s *jsonschema.Schema) []string {
	declared := s.Types
	if s.Type != "" {
		declared = []string{s.Type}
	}
	if len(declared) == 0 {
		return nil
	}
	types := slices.Clone(scalarTypes)
	for _, t := range declared {
		if (t == "object" || t == "array") && !slices.Contains(types, t) {
			types = append(types, t)
		}
	}
	slices.Sort(types)
	return types
}

func shapeSlice(in []*jsonschema.Schema) []*jsonschema.Schema {
	if in == nil {
		return nil
	}
	out := make([]*jsonschema.Schema, len(in))
	for i, s := range in {
		out[i] = shapeOf(s)
	}
	return out
}

func shapeMap(in map[string]*jsonschema.Schema) map[string]*jsonschema.Schema {
	if in == nil {
		return nil
	}
	out := make(map[string]*jsonschema.Schema, len(in))
	for key, s := range in {
		out[key] = shapeOf(s)
	}
	return out
}
