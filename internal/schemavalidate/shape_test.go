package schemavalidate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shapeSchemaFields classifies every field of jsonschema.Schema for the
// skeleton (shape.go): kept as is, rewritten, or dropped. A library upgrade
// that adds a field fails TestShapeOfClassifiesEverySchemaField until the
// field is classified, because an unclassified field is dropped, and a
// dropped keyword that can fail a document only where its subschema
// succeeds (as not does) would make the skeleton refuse what the schema
// admits.
var shapeSchemaFields = map[string]string{
	"ID": "kept", "Schema": "kept", "Ref": "kept", "Anchor": "kept", "DynamicAnchor": "kept", "DynamicRef": "kept",
	"Defs": "kept", "Definitions": "kept", "Properties": "kept", "AllOf": "kept", "AnyOf": "kept",
	"PrefixItems": "rewritten", "Items": "rewritten", "ItemsArray": "rewritten", "AdditionalItems": "rewritten",
	"Type": "rewritten", "Types": "rewritten", "OneOf": "rewritten", "If": "rewritten", "Then": "rewritten", "Else": "rewritten",
	"Not": "dropped", "Comment": "dropped", "DependencySchemas": "dropped", "DependencyStrings": "dropped",
	"Vocabulary": "dropped", "Title": "dropped", "Description": "dropped", "Default": "dropped", "Deprecated": "dropped",
	"ReadOnly": "dropped", "WriteOnly": "dropped", "Examples": "dropped", "Enum": "dropped", "Const": "dropped",
	"MultipleOf": "dropped", "Minimum": "dropped", "Maximum": "dropped", "ExclusiveMinimum": "dropped",
	"ExclusiveMaximum": "dropped", "MinLength": "dropped", "MaxLength": "dropped", "Pattern": "dropped",
	"MinItems": "dropped", "MaxItems": "dropped", "UniqueItems": "dropped", "Contains": "dropped",
	"MinContains": "dropped", "MaxContains": "dropped", "UnevaluatedItems": "dropped", "MinProperties": "dropped",
	"MaxProperties": "dropped", "Required": "dropped", "DependentRequired": "dropped", "PatternProperties": "dropped",
	"AdditionalProperties": "dropped", "PropertyNames": "dropped", "UnevaluatedProperties": "dropped",
	"DependentSchemas": "dropped", "ContentEncoding": "dropped", "ContentMediaType": "dropped",
	"ContentSchema": "dropped", "Format": "dropped", "Extra": "dropped", "PropertyOrder": "dropped",
}

func TestShapeOfClassifiesEverySchemaField(t *testing.T) {
	typ := reflect.TypeFor[jsonschema.Schema]()
	for i := range typ.NumField() {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}
		_, ok := shapeSchemaFields[field.Name]
		assert.Truef(t, ok, "jsonschema.Schema.%s is not classified for the container skeleton", field.Name)
	}
}

// shapeFor resolves schemaJSON and its skeleton through New.
func shapeFor(t *testing.T, schemaJSON string) (*jsonschema.Resolved, *ShapeSchema) {
	t.Helper()
	v, err := New(registryWith(t, "s", schemaJSON, 1), t.TempDir())
	require.NoError(t, err)
	shape := v.Shape(1)
	require.NotNil(t, shape, "the skeleton must resolve")
	return v.resolved[1], shape
}

func decodeDoc(t *testing.T, doc string) any {
	t.Helper()
	var m any
	require.NoError(t, json.Unmarshal([]byte(doc), &m))
	return m
}

// leafArrangement is one single value of a document: the dotted path
// segments it sits at and the arrays around each segment's value.
type leafArrangement struct {
	segments []string
	arrays   []int
}

// leafArrangements answers every single value of doc, and a value inside
// every empty array, the way the write's walk meets them.
func leafArrangements(value any, segments []string, arrays []int) []leafArrangement {
	switch v := value.(type) {
	case map[string]any:
		var out []leafArrangement
		for key, child := range v {
			out = append(out, leafArrangements(child, append(slices.Clone(segments), key), append(slices.Clone(arrays), 0))...)
		}
		return out
	case []any:
		inner := slices.Clone(arrays)
		inner[len(inner)-1]++
		if len(v) == 0 {
			return []leafArrangement{{segments, inner}}
		}
		var out []leafArrangement
		for _, item := range v {
			out = append(out, leafArrangements(item, segments, inner)...)
		}
		return out
	}
	return []leafArrangement{{segments, arrays}}
}

// shapeCases pairs schemas with documents and arrangements. Every single
// value of an admitted document, which the schema itself admits, must be
// admitted (the skeleton relaxes the schema); a refused arrangement places a
// value inside arrays the schema's structure excludes there.
var shapeCases = []struct {
	name     string
	schema   string
	admitted []string
	refused  []leafArrangement
}{
	{
		name:     "a single value",
		schema:   `{"type":"object","properties":{"tenant":{"type":"string","enum":["a","b"]}}}`,
		admitted: []string{`{"tenant":"a"}`},
		refused:  []leafArrangement{{[]string{"tenant"}, []int{1}}, {[]string{"tenant"}, []int{2}}},
	},
	{
		name: "an array of objects",
		schema: `{"type":"object","properties":{"items":{"type":"array","minItems":2,"items":{"type":"object",
			"required":["name"],"properties":{"name":{"type":"string"}}}}}}`,
		admitted: []string{`{"items":[{"name":"x"},{"name":"y"}]}`},
		refused: []leafArrangement{
			{[]string{"items", "name"}, []int{2, 0}}, {[]string{"items", "name"}, []int{1, 1}}, {[]string{"items", "name"}, []int{0, 1}},
		},
	},
	{
		name: "an object or one array of objects",
		schema: `{"type":"object","properties":{"office":{"anyOf":[
			{"type":"object","properties":{"city":{"type":"string"}}},
			{"type":"array","items":{"type":"object","properties":{"city":{"type":"string"}}}}]}}}`,
		admitted: []string{`{"office":{"city":"a"}}`, `{"office":[{"city":"a"}]}`},
		refused:  []leafArrangement{{[]string{"office", "city"}, []int{2, 0}}, {[]string{"office", "city"}, []int{0, 1}}},
	},
	{
		name:     "a nullable primitive array beneath a typeless parent",
		schema:   `{"type":"object","properties":{"profile":{"properties":{"tags":{"type":["array","null"],"items":{"type":"string"}},"note":{"type":"string"}}}}}`,
		admitted: []string{`{"profile":{"tags":["a"]}}`, `{"profile":{"tags":[]}}`, `{"profile":{"tags":null}}`},
		refused:  []leafArrangement{{[]string{"profile", "note"}, []int{0, 1}}, {[]string{"profile", "tags"}, []int{0, 2}}},
	},
	{
		name:     "a primitive array with combinator-typed items",
		schema:   `{"type":"object","properties":{"tags":{"type":"array","items":{"anyOf":[{"type":"string"},{"type":"integer"}]}}}}`,
		admitted: []string{`{"tags":["a",1]}`},
		refused:  []leafArrangement{{[]string{"tags"}, []int{2}}},
	},
	{
		name:     "an array or an object",
		schema:   `{"type":"object","properties":{"tags":{"anyOf":[{"type":"array","items":{"type":"string"}},{"type":"object"}]}}}`,
		admitted: []string{`{"tags":["b"]}`},
		refused:  []leafArrangement{{[]string{"tags"}, []int{2}}},
	},
	{
		name:     "a tuple",
		schema:   `{"type":"object","properties":{"pair":{"type":"array","prefixItems":[{"type":"string"},{"type":"array","items":{"type":"string"}}],"items":false}}}`,
		admitted: []string{`{"pair":["a",["b"]]}`},
		refused:  []leafArrangement{{[]string{"pair"}, []int{3}}},
	},
	{
		name: "oneOf branches the relaxation makes overlap",
		schema: `{"type":"object","properties":{"code":{"oneOf":[{"type":"string","maxLength":2},{"type":"string","minLength":3}]},
			"n":{"not":{"const":1}}, "c":{"if":{"type":"string"},"then":{"minLength":9},"else":{"type":"array"}}}}`,
		admitted: []string{`{"code":"abcd","n":2,"c":"abcdefghij"}`, `{"c":["x"]}`},
		refused:  []leafArrangement{{[]string{"code"}, []int{1}}},
	},
	{
		name:     "the dependent and unevaluated keywords",
		schema:   `{"type":"object","dependentRequired":{"a":["b"]},"unevaluatedProperties":false,"properties":{"a":{"type":"string"},"b":{"type":"string"}}}`,
		admitted: []string{`{"a":"x","b":"y"}`},
		refused:  []leafArrangement{{[]string{"a"}, []int{1}}},
	},
}

func TestShapeRelaxesTheSchemaAndRefusesOnlyContainers(t *testing.T) {
	for _, tc := range shapeCases {
		t.Run(tc.name, func(t *testing.T) {
			full, shape := shapeFor(t, tc.schema)
			for _, doc := range tc.admitted {
				require.NoErrorf(t, full.Validate(decodeDoc(t, doc)), "fixture %s must be valid under the schema", doc)
				for _, leaf := range leafArrangements(decodeDoc(t, doc), nil, nil) {
					assert.NoErrorf(t, shape.AdmitsArrangement(leaf.segments, leaf.arrays),
						"the skeleton must admit %v inside %v arrays, which %s places", leaf.segments, leaf.arrays, doc)
				}
			}
			for _, leaf := range tc.refused {
				assert.Errorf(t, shape.AdmitsArrangement(leaf.segments, leaf.arrays), "the skeleton must refuse %v inside %v arrays", leaf.segments, leaf.arrays)
			}
		})
	}
}

// TestShapeAdmitsEverySingleValue pins that the skeleton never refuses a
// single value outside arrays, of any type and wherever it sits: a wrong
// type stays report-only, and a single value where an object belongs stays
// the write's own refusal.
func TestShapeAdmitsEverySingleValue(t *testing.T) {
	_, shape := shapeFor(t, `{"type":"object","properties":{"n":{"type":"integer"},"o":{"type":"object","properties":{"x":{"type":"string"}}},
		"l":{"type":"array","items":{"type":"string"}},"f":false}}`)
	for _, segments := range [][]string{{"n"}, {"o"}, {"l"}, {"f"}, {"o", "x"}, {"undefined", "x"}} {
		assert.NoErrorf(t, shape.AdmitsArrangement(segments, make([]int, len(segments))), "the skeleton must admit a single value at %v", segments)
	}
}

// TestShapeFollowsCrossFileReferences pins that a $ref into a sibling file
// lands in that file's skeleton.
func TestShapeFollowsCrossFileReferences(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "other.json"),
		[]byte(`{"$defs":{"name":{"type":"string","minLength":3},"tags":{"type":"array","items":{"type":"string"}}}}`), 0o600))
	v, err := New(registryWith(t, "s", `{"type":"object","properties":{
		"name":{"$ref":"other.json#/$defs/name"},"tags":{"$ref":"other.json#/$defs/tags"}}}`, 1), dir)
	require.NoError(t, err)
	shape := v.Shape(1)
	require.NotNil(t, shape)
	assert.NoError(t, shape.AdmitsArrangement([]string{"tags"}, []int{1}))
	assert.Error(t, shape.AdmitsArrangement([]string{"name"}, []int{1}))
}

// TestShapeIsAbsentWhereARefPointsIntoARewrittenKeyword pins the fallback:
// the schema still validates, and the validator keeps no skeleton for it.
func TestShapeIsAbsentWhereARefPointsIntoARewrittenKeyword(t *testing.T) {
	v, err := New(registryWith(t, "s", `{"type":"object","properties":{
		"a":{"oneOf":[{"type":"string"},{"type":"integer"}]},"b":{"$ref":"#/properties/a/oneOf/0"}}}`, 1), t.TempDir())
	require.NoError(t, err)
	assert.Nil(t, v.Shape(1))
	require.NoError(t, v.Validate(1, map[string]any{"b": "x"}))
	assert.Nil(t, (*Validator)(nil).Shape(1), "an unconfigured validator has no skeleton")
}

// TestShippedSchemasHaveSkeletons pins that every shipped schema keeps one.
func TestShippedSchemasHaveSkeletons(t *testing.T) {
	dir := shippedSchemaDir(t)
	registry := &stubRegistry{byName: map[string]forma.JSONSchema{}, idsByNm: map[string]int16{}}
	for i, file := range shippedSchemaNames(t, dir) {
		raw, err := os.ReadFile(filepath.Join(dir, file))
		require.NoError(t, err)
		name := file[:len(file)-len(".json")]
		registry.names = append(registry.names, name)
		registry.byName[name] = forma.JSONSchema{Name: name, Schema: string(raw)}
		registry.idsByNm[name] = int16(i + 1)
	}
	v, err := New(registry, dir)
	require.NoError(t, err)
	for name, id := range registry.idsByNm {
		assert.NotNilf(t, v.Shape(id), "shipped schema %s must keep a container skeleton", name)
	}
}

func TestShapeAdmitsIndicesAlongTheAttributePath(t *testing.T) {
	_, shape := shapeFor(t, `{"type":"object","properties":{
		"note":{"type":"string"},
		"tags":{"type":"array","items":{"type":"string"}},
		"areas":{"type":"array","items":{"type":"object","properties":{"city":{"type":"string"}}}},
		"office":{"anyOf":[{"type":"object","properties":{"city":{"type":"string"}}},{"type":"array","items":{"type":"object","properties":{"city":{"type":"string"}}}}]},
		"loose":{"properties":{"x":{"type":"string"}}}}}`)
	for _, tc := range []struct {
		attr     string
		depth    int
		admitted bool
	}{
		{"note", 1, false}, {"tags", 1, true}, {"tags", 2, false},
		{"areas.city", 1, true}, {"areas.city", 2, false},
		{"office.city", 1, true}, {"office.city", 2, false},
		{"loose.x", 1, true}, {"loose.x", 2, true},
	} {
		admitted, decided := shape.AdmitsIndices(tc.attr, tc.depth)
		require.Truef(t, decided, "%s at %d indices", tc.attr, tc.depth)
		assert.Equalf(t, tc.admitted, admitted, "%s at %d indices", tc.attr, tc.depth)
	}
	_, decided := shape.AdmitsIndices("note.a.b.c.d.e.f.g", 12)
	assert.False(t, decided, "past the witness budget the answer is undecided")
}
