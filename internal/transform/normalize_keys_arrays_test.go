package transform

// Array-path tests: everything here drives the real validator over the shipped
// schema directory, so it moves as one group and keeps normalize_keys_test.go to
// document-shape assertions.

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/stretchr/testify/require"
)

// leadFullRegistry serves the shipped lead_full schema to the validator. The
// schema body is read from disk rather than inlined so this test cannot drift
// away from what production actually validates against.
type leadFullRegistry struct {
	*stubSchemaRegistry
	schema string
}

func (r *leadFullRegistry) GetSchemaByName(name string) (int16, forma.JSONSchema, error) {
	if name != r.schemaName {
		return 0, forma.JSONSchema{}, fmt.Errorf("schema %s not found", name)
	}
	return r.schemaID, forma.JSONSchema{ID: r.schemaID, Name: r.schemaName, Schema: r.schema}, nil
}

// newLeadFullValidator builds the production validator over the shipped schema
// directory. requirement.areas is declared "type": "array" there, with
// requirement.areas.city defined in lead_full_attributes.json — the exact shape
// this test needs, in the form callers really send.
func newLeadFullValidator(t *testing.T) (*schemavalidate.Validator, int16) {
	t.Helper()

	const dir = "../../cmd/server/schemas"
	body, err := os.ReadFile(filepath.Join(dir, "lead_full.json"))
	require.NoError(t, err, "shipped schema must be readable; update the path if it moved")

	registry := &leadFullRegistry{
		stubSchemaRegistry: &stubSchemaRegistry{schemaID: 100, schemaName: "lead_full"},
		schema:             string(body),
	}
	validator, err := schemavalidate.New(registry, dir)
	require.NoError(t, err)
	return validator, 100
}

func areasCache() forma.SchemaAttributeCache {
	return forma.SchemaAttributeCache{
		"requirement.areas.city": {AttributeID: 80, ValueType: forma.ValueTypeText},
		"requirement.areas.note": {AttributeID: 81, ValueType: forma.ValueTypeText},
	}
}

// leadFullPayload returns a document satisfying lead_full.json's required root
// properties, so a validation failure in these tests can only come from the part
// under test. requirement is optional and is supplied by each caller.
func leadFullPayload(requirement map[string]any, extra map[string]any) map[string]any {
	doc := map[string]any{
		"id":          "0199c9e0-0000-7000-8000-000000000000",
		"tenantId":    "t1",
		"ownerUserId": "u1",
		"pipeline":    "buy",
		"stage":       "new",
		"status":      "open",
		"contact":     map[string]any{"name": "Ada"},
		"createdAt":   "2026-07-25T00:00:00Z",
		"updatedAt":   "2026-07-25T00:00:00Z",
	}
	if requirement != nil {
		doc["requirement"] = requirement
	}
	for key, value := range extra {
		doc[key] = value
	}
	return doc
}

// TestNormalizeKeepsArrayOnPathValidatable pins that an array on the expansion
// path stops expansion.
//
// requirement.areas is declared "type": "array". Replacing it with an object to
// place requirement.areas.city would make the validator's document contradict
// the schema, and the caller would get a 400 naming a type they never sent —
// undiagnosable from the response. This is the shape mergeMaps builds on every
// update: a key-literal patch over storage that FromPersistentRecord re-nested.
func TestNormalizeKeepsArrayOnPathValidatable(t *testing.T) {
	in := leadFullPayload(
		map[string]any{"areas": []any{map[string]any{"city": "OLD", "note": "N"}}},
		map[string]any{"requirement.areas.city": "NEW"},
	)

	validator, schemaID := newLeadFullValidator(t)
	out := NormalizeDottedKeys(in, areasCache(), validator.ArrayPaths(schemaID))
	require.Equal(t, in, out, "the array must survive; the literal stays flat")

	require.NoError(t, validator.Validate(schemaID, out),
		"the validator must accept a document that keeps requirement.areas an array")
}

// TestNormalizeLeavesArrayOnPathValueUnvalidated pins the cost of the rule
// above, so the gap is a recorded decision rather than something discovered
// later. The literal stays flat, so it is an unknown property to JSON Schema and
// its value is never examined: requirement.areas.city is declared a string, and
// a number passes. A false 400 on a working payload is worse than an
// unvalidated value, so this is the deliberate side the trade-off falls on.
func TestNormalizeLeavesArrayOnPathValueUnvalidated(t *testing.T) {
	validator, schemaID := newLeadFullValidator(t)
	out := NormalizeDottedKeys(leadFullPayload(
		map[string]any{"areas": []any{map[string]any{"city": "OLD"}}},
		map[string]any{"requirement.areas.city": 12345},
	), areasCache(), validator.ArrayPaths(schemaID))

	require.Equal(t, 12345, out["requirement.areas.city"], "the literal is left unexpanded")

	require.NoError(t, validator.Validate(schemaID, out),
		"a wrongly typed value behind an array on the path is not seen by validation")
}

// TestNormalizeExpandsWhenOnlyTheFinalSegmentIsAnArray pins that arrayOnPath
// checks interior segments only.
//
// An array at the final segment is the value the caller wrote at that path, so
// expansion is what puts it where the schema can see it: contact.phones is
// declared an array of strings, and after expansion a numeric element is
// rejected. Refusing there instead would leave the whole list an unknown
// property and unvalidated — a mutant widening the walk to every segment is
// otherwise invisible to the whole suite.
func TestNormalizeExpandsWhenOnlyTheFinalSegmentIsAnArray(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"contact.phones": {AttributeID: 19, ValueType: forma.ValueTypeText},
	}
	validator, schemaID := newLeadFullValidator(t)
	arrays := validator.ArrayPaths(schemaID)
	require.Contains(t, arrays, "contact.phones", "the fixture must exercise a real array path")

	out := NormalizeDottedKeys(leadFullPayload(nil, map[string]any{
		"contact":        map[string]any{"name": "Ada", "phones": []any{"080-0000-0000"}},
		"contact.phones": []any{"090-1111-2222"},
	}), cache, arrays)

	require.NotContains(t, out, "contact.phones", "the literal must be expanded, not left flat")
	require.Equal(t, []any{"090-1111-2222"}, requireChildMap(t, out, "contact")["phones"])
	require.NoError(t, validator.Validate(schemaID, out))

	// The point of expanding: the value is now inside the schema's reach.
	bad := NormalizeDottedKeys(leadFullPayload(nil, map[string]any{
		"contact":        map[string]any{"name": "Ada", "phones": []any{"080-0000-0000"}},
		"contact.phones": []any{12345},
	}), cache, arrays)
	require.ErrorIs(t, validator.Validate(schemaID, bad), forma.ErrInvalidInput,
		"a wrongly typed element must be caught once the literal is expanded")
}

// TestNormalizeSkipsExpansionUnderSchemaArrayWhenAbsent closes the last
// inconsistency in #314's dotted-key handling.
//
// Before the schema's array paths were available, this payload — the same literal
// with no nested array to detect — was expanded into an object where
// requirement.areas is declared an array, and the caller received
// `has type "object", want "array"`: a 400 naming a type they never sent, for a
// payload the writer accepts. With the array paths derived from the schema, the
// literal is left alone whether or not the array is present, so array-present and
// array-absent are now one behaviour with one documented consequence — the value
// is not validated.
func TestNormalizeSkipsExpansionUnderSchemaArrayWhenAbsent(t *testing.T) {
	validator, schemaID := newLeadFullValidator(t)
	arrays := validator.ArrayPaths(schemaID)
	require.Contains(t, arrays, "requirement.areas")

	in := leadFullPayload(nil, map[string]any{"requirement.areas.city": 12345})
	out := NormalizeDottedKeys(in, areasCache(), arrays)

	require.Equal(t, in, out, "nothing to expand: the schema says this path crosses an array")
	require.NoError(t, validator.Validate(schemaID, out),
		"no false rejection for a type the caller never sent")
}

// TestNormalizeStillExpandsAttributeNotUnderArray is the regression risk of the
// skip above: refusing too broadly would silently stop validating ordinary
// attributes. contact.email is a real shipped attribute under an object, so it
// must still be expanded — and the proof that expansion is what makes validation
// work is that a wrongly typed value is now rejected.
func TestNormalizeStillExpandsAttributeNotUnderArray(t *testing.T) {
	validator, schemaID := newLeadFullValidator(t)
	arrays := validator.ArrayPaths(schemaID)
	cache := forma.SchemaAttributeCache{
		"contact.email": {AttributeID: 8, ValueType: forma.ValueTypeText},
	}

	out := NormalizeDottedKeys(leadFullPayload(nil, map[string]any{
		"contact":       map[string]any{"name": "Ada"},
		"contact.email": "ada@example.com",
	}), cache, arrays)

	require.NotContains(t, out, "contact.email", "an attribute not under an array must expand")
	require.Equal(t, "ada@example.com", requireChildMap(t, out, "contact")["email"])
	require.NoError(t, validator.Validate(schemaID, out))

	bad := NormalizeDottedKeys(leadFullPayload(nil, map[string]any{
		"contact":       map[string]any{"name": "Ada"},
		"contact.email": 12345,
	}), cache, arrays)
	require.ErrorIs(t, validator.Validate(schemaID, bad), forma.ErrInvalidInput,
		"expansion is what puts the value in the schema's reach")
}

// TestNormalizeExpandsDottedKeyInsideArrayElement pins that being inside an
// array element does not disable expansion.
//
// propertyInterests is a schema array, so every attribute name below it crosses
// an array — but a dotted key written *inside* an element is already past that
// array, and nesting it there is legal and is what the write path stores. Asking
// the array paths about the absolute name refuses it anyway, which silently
// stopped validating the five propertyInterests.snapshot.* attributes.
func TestNormalizeExpandsDottedKeyInsideArrayElement(t *testing.T) {
	validator, schemaID := newLeadFullValidator(t)
	arrays := validator.ArrayPaths(schemaID)
	require.Contains(t, arrays, "propertyInterests", "the fixture must sit inside a real array")

	cache := forma.SchemaAttributeCache{
		"propertyInterests.snapshot.code": {AttributeID: 37, ValueType: forma.ValueTypeText},
	}
	payload := func(code any) map[string]any {
		return leadFullPayload(nil, map[string]any{
			"propertyInterests": []any{map[string]any{
				"propertyId":    "P1",
				"status":        "viewed",
				"snapshot.code": code,
			}},
		})
	}

	out := NormalizeDottedKeys(payload("C1"), cache, arrays)

	element := requireFirstElement(t, out, "propertyInterests")
	require.NotContains(t, element, "snapshot.code", "the dotted key must be nested inside the element")
	require.Equal(t, map[string]any{"code": "C1"}, element["snapshot"])
	require.NoError(t, validator.Validate(schemaID, out))

	bad := NormalizeDottedKeys(payload(12345), cache, arrays)
	require.ErrorIs(t, validator.Validate(schemaID, bad), forma.ErrInvalidInput,
		"nesting inside the element is what lets the schema see the value")
}

func requireFirstElement(t *testing.T, doc map[string]any, key string) map[string]any {
	t.Helper()

	items, ok := doc[key].([]any)
	require.Truef(t, ok, "expected %q to be an array", key)
	require.NotEmpty(t, items)
	element, ok := items[0].(map[string]any)
	require.Truef(t, ok, "expected %q[0] to be an object", key)
	return element
}
