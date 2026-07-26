package schemavalidate

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
)

// supportedSchemaVersions mirrors jsonschema-go's unexported isValidSchemaVersion
// (validate.go). An empty value is legal: the library treats a missing $schema as
// draft 2020-12.
//
// This list is duplicated rather than imported because the library does not
// export it. TestNewAcceptsSupportedSchemaVersions pins each entry against the
// real library, so a version the library drops support for turns red here.
var supportedSchemaVersions = map[string]bool{
	"": true,
	"http://json-schema.org/draft-07/schema#":      true,
	"https://json-schema.org/draft-07/schema#":     true,
	"https://json-schema.org/draft/2020-12/schema": true,
}

// jsonSchemaTypes is the JSON Schema type vocabulary. Anything outside it makes
// every instance fail the type check, because validation compares the
// instance's detected type against this string.
var jsonSchemaTypes = map[string]bool{
	"null": true, "boolean": true, "object": true,
	"array": true, "number": true, "string": true, "integer": true,
}

// checkSchemaSupported rejects schemas that resolve cleanly but can never
// validate any document.
//
// Both faults below are schema *configuration* problems, not caller mistakes,
// and neither is caught by Resolve. Left to Validate they would be wrapped as
// forma.ErrInvalidInput and answer 400 to every write, blaming a caller error
// that does not exist and hiding the real fault from the operator. Catching
// them at construction converts a permanent 400 into a startup failure, and so
// keeps Validate's blanket ErrInvalidInput wrap honest for the faults it can
// see.
//
// It is not a complete guard. The walk covers the parsed document only, and $ref
// targets live in the library's unexported side table — so a bad "type" behind a
// $ref to a file that is not itself a registered schema still reaches Validate
// and is still mislabelled as caller input.
func checkSchemaSupported(s *jsonschema.Schema) error {
	if err := checkSchemaVersion(s); err != nil {
		return fmt.Errorf("failed to accept the schema's declared version: %w", err)
	}
	if err := walkSubschemas(s, "", checkTypeKeyword); err != nil {
		return fmt.Errorf("failed to accept the schema's type keywords: %w", err)
	}
	return nil
}

// checkSchemaVersion rejects a $schema the library cannot validate against.
// jsonschema-go's detectDraft falls through to draft 2020-12 for anything it
// does not recognise, so an unsupported version survives Resolve and only
// surfaces on the first Validate call.
//
// Only the root is checked, matching Resolved.Validate, which tests
// rs.root.Schema alone. A $schema on a subschema is ignored by the library and
// so cannot cause the failure; rejecting it here would refuse to boot on a
// schema that actually works.
func checkSchemaVersion(s *jsonschema.Schema) error {
	if supportedSchemaVersions[s.Schema] {
		return nil
	}
	return fmt.Errorf(
		"unsupported $schema version %q: supported versions are draft-07 and draft 2020-12",
		s.Schema)
}

// checkTypeKeyword rejects a "type" value outside the JSON Schema vocabulary.
// Unlike $schema this must be checked on every subschema: entity schemas
// declare types on properties and in $defs, not on the root.
//
// path is the subschema's position, which the operator needs to find the typo —
// a schema declares dozens of types and the offending value alone does not say
// which property carries it.
func checkTypeKeyword(path string, s *jsonschema.Schema) error {
	if s.Type != "" && !jsonSchemaTypes[s.Type] {
		return unknownTypeError(path, s.Type)
	}
	for _, t := range s.Types {
		if !jsonSchemaTypes[t] {
			return unknownTypeError(path, t)
		}
	}
	return nil
}

func unknownTypeError(path, got string) error {
	return fmt.Errorf(
		"unknown \"type\" value %q at %s: must be one of null, boolean, object, array, number, string, integer",
		got, describePath(path))
}

// describePath renders a subschema position for an operator. The root has no
// path of its own, and "at " followed by nothing reads as a truncated message.
func describePath(path string) string {
	if path == "" {
		return "the schema root"
	}
	return path
}

var (
	schemaPtrType   = reflect.TypeFor[*jsonschema.Schema]()
	schemaSliceType = reflect.TypeFor[[]*jsonschema.Schema]()
	schemaMapType   = reflect.TypeFor[map[string]*jsonschema.Schema]()
)

// walkSubschemas calls fn for s and every subschema reachable from it, stopping
// at the first error. path is s's own position, "" at the root.
//
// Subschemas are found by field *type* rather than by name, mirroring how
// jsonschema-go's own unexported everyChild walks the tree. That means a
// subschema-bearing keyword added by a future library version is traversed
// automatically instead of being silently skipped by a hardcoded field list.
//
// The tree cannot contain cycles: it comes from json.Unmarshal, and $ref is a
// plain string whose resolved target the library keeps in a side table rather
// than in an exported field.
func walkSubschemas(s *jsonschema.Schema, path string, fn subschemaFunc) error {
	if s == nil {
		return nil
	}
	if err := fn(path, s); err != nil {
		return err
	}

	v := reflect.ValueOf(s).Elem()
	for i := range v.NumField() {
		field := v.Field(i)
		if !field.CanInterface() {
			continue // unexported: not part of the parsed schema tree
		}
		if err := walkField(field, keywordPath(path, v.Type().Field(i)), fn); err != nil {
			return err
		}
	}
	return nil
}

// subschemaFunc is applied to one subschema and its position in the tree.
type subschemaFunc func(path string, s *jsonschema.Schema) error

// walkField descends one struct field if it holds subschemas, where path is the
// field's own position. Map keys are visited in sorted order so that a schema
// with several faults always reports the same one; slice elements carry their
// index, since allOf/anyOf branches are distinguishable only by position.
func walkField(field reflect.Value, path string, fn subschemaFunc) error {
	switch field.Type() {
	case schemaPtrType:
		child, _ := field.Interface().(*jsonschema.Schema)
		return walkSubschemas(child, path, fn)

	case schemaSliceType:
		children, _ := field.Interface().([]*jsonschema.Schema)
		for i, child := range children {
			if err := walkSubschemas(child, fmt.Sprintf("%s/%d", path, i), fn); err != nil {
				return err
			}
		}

	case schemaMapType:
		children, _ := field.Interface().(map[string]*jsonschema.Schema)
		for _, key := range slices.Sorted(maps.Keys(children)) {
			if err := walkSubschemas(children[key], path+"/"+key, fn); err != nil {
				return err
			}
		}
	}
	return nil
}

// keywordPath extends path with the JSON Schema keyword a struct field carries.
//
// The keyword comes from the json tag so the path reads as the schema author
// wrote it. Some fields are tagged "-" because the library marshals them by hand
// — Items is the one that matters, since it is on every array — so those fall
// back to the field name with a lowercased initial, which is the same spelling
// in every case the library currently has.
func keywordPath(path string, field reflect.StructField) string {
	name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
	if name == "" || name == "-" {
		name = strings.ToLower(field.Name[:1]) + field.Name[1:]
	}
	return path + "/" + name
}
