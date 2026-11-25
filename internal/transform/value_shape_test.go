package transform

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemavalidate"
)

// shapeRegistry is the one-schema forma.SchemaRegistry schemavalidate.New
// reads.
type shapeRegistry struct{ document string }

func (r shapeRegistry) ListSchemas() []string { return []string{"shaped"} }
func (r shapeRegistry) GetSchemaByName(string) (int16, forma.JSONSchema, error) {
	return 1, forma.JSONSchema{ID: 1, Name: "shaped", Schema: r.document}, nil
}
func (r shapeRegistry) GetSchemaByID(int16) (string, forma.JSONSchema, error) {
	return "", forma.JSONSchema{}, fmt.Errorf("not used")
}
func (r shapeRegistry) GetSchemaAttributeCacheByName(string) (int16, forma.SchemaAttributeCache, error) {
	return 0, nil, fmt.Errorf("not used")
}
func (r shapeRegistry) GetSchemaAttributeCacheByID(int16) (string, forma.SchemaAttributeCache, error) {
	return "", nil, fmt.Errorf("not used")
}

// shapedSchema declares updateIntakeCache's attributes, plus phones, a
// primitive array under an element-typed (pre-#204) ledger entry, and areas,
// an array of objects.
const shapedSchema = `{"type":"object","properties":{
	"name":{"type":"string","enum":["a","b"]},"label":{"type":"string"},
	"tags":{"type":"array","items":{"type":"string"}},
	"items":{"type":"array","items":{"type":"object","properties":{"note":{"type":"string"},"qty":{"type":"integer"}}}},
	"age":{"type":"integer"},
	"phones":{"type":["array","null"],"items":{"anyOf":[{"type":"string"},{"type":"integer"}]}}}}`

func shapedCache() forma.SchemaAttributeCache {
	cache := updateIntakeCache()
	cache["phones"] = forma.AttributeMetadata{AttributeID: 30, ValueType: forma.ValueTypeText}
	return cache
}

func shapedSkeleton(t *testing.T) *schemavalidate.ShapeSchema {
	t.Helper()
	v, err := schemavalidate.New(shapeRegistry{document: shapedSchema}, t.TempDir())
	require.NoError(t, err)
	require.NotNil(t, v.Shape(1))
	return v.Shape(1)
}

// published answers the message err publishes.
func published(t *testing.T, err error) string {
	t.Helper()
	msg, ok := forma.ResolvePublicMessage(err)
	require.Truef(t, ok, "%v publishes no message", err)
	return msg
}

// singleValueRule is the published tail of an object refused where an
// attribute's single values belong.
const singleValueRule = "cannot be stored where a single value belongs (omit the key to keep the current value)"

// TestCheckUpdateValuesRefusesAnObjectAtAnAttribute pins the first rule of
// #696 at intake: an object at an attribute's own position, empty or not,
// directly, inside an array, through a dotted key or inside a list's value,
// is refused before any statement, with or without a skeleton.
func TestCheckUpdateValuesRefusesAnObjectAtAnAttribute(t *testing.T) {
	for name, tc := range map[string]struct {
		doc  map[string]any
		want string
	}{
		"empty object":           {map[string]any{"name": map[string]any{}}, refusedValue("name", 1) + "declared type text takes a single value, got an empty object"},
		"object":                 {map[string]any{"name": map[string]any{"x": "y"}}, refusedValue("name", 1) + "declared type text takes a single value, got an object"},
		"bound":                  {map[string]any{"label": map[string]any{}}, refusedValue("label", 2) + "declared type text takes a single value, got an empty object"},
		"typed nil map":          {map[string]any{"age": map[string]any(nil)}, refusedValue("age", 7) + "declared type integer takes a single value, got an empty object"},
		"inside an array":        {map[string]any{"items": []any{map[string]any{"note": []any{map[string]any{}}}}}, refusedValue("items.note", 5)},
		"member of an array":     {map[string]any{"items": []any{map[string]any{"note": map[string]any{}}}}, refusedValue("items.note", 5)},
		"dotted key":             {map[string]any{"items.note": map[string]any{}}, refusedValue("items.note", 5)},
		"inside a list":          {map[string]any{"tags": []any{"a", map[string]any{}}}, refusedValue("tags", 3) + "declared type list takes an array of single text values, got an empty object"},
		"nested inside a list":   {map[string]any{"tags": []any{[]any{map[string]any{}}}}, refusedValue("tags", 3) + "declared type list takes an array of single text values, got an empty object"},
		"before a later refusal": {map[string]any{"label": map[string]any{}, "ref": "bad"}, refusedValue("label", 2)},
		"after an earlier one":   {map[string]any{"age": 1.5, "label": map[string]any{}}, refusedValue("age", 7)},
	} {
		for shapes, shape := range map[string]*schemavalidate.ShapeSchema{"no skeleton": nil, "skeleton": shapedSkeleton(t)} {
			t.Run(name+"/"+shapes, func(t *testing.T) {
				checked, err := CheckUpdateValues(tc.doc, shapedCache(), shape)
				require.NoError(t, err, "a shape refusal is the Refusal class, raised where a row is written")
				require.ErrorIs(t, checked.Refusal, forma.ErrInvalidInput)
				assert.Contains(t, published(t, checked.Refusal), tc.want)
			})
		}
	}
}

// TestCheckUpdateValuesRefusesAnArrayAtASingleValueWithoutASkeleton pins the
// fallback: with no skeleton nothing declares an array at an attribute that
// is not a list, so every array there is refused, an array of nulls
// included, while a list's arrays are judged as before and a nil list stays
// nil (JSON null), apart from [].
func TestCheckUpdateValuesRefusesAnArrayAtASingleValueWithoutASkeleton(t *testing.T) {
	for name, doc := range map[string]map[string]any{
		"empty":       {"name": []any{}},
		"singleton":   {"name": []any{"a"}},
		"nil slice":   {"age": []any(nil)},
		"in an array": {"items": []any{map[string]any{"note": []any{"n"}}}},
		"elements":    {"phones": []any{"1", "2"}},
		"nulls":       {"name": []any{nil}},
	} {
		t.Run(name, func(t *testing.T) {
			checked, err := CheckUpdateValues(doc, shapedCache(), nil)
			require.NoError(t, err)
			require.ErrorIs(t, checked.Refusal, forma.ErrInvalidInput)
			assert.Contains(t, published(t, checked.Refusal), "takes a single value, got a")
			assert.Contains(t, published(t, checked.Refusal), "inside arrays the schema does not declare there; an object or array, empty or not, "+singleValueRule)
			assert.Contains(t, checked.Refusal.Error(), "no container skeleton")
		})
	}

	checked, err := CheckUpdateValues(map[string]any{"tags": []any(nil), "counts": []any{}}, shapedCache(), nil)
	require.NoError(t, err)
	require.NoError(t, checked.Refusal)
	assert.Nil(t, checked.Values["tags"], "a nil list stays nil, which strict validation refuses as null")
	assert.NotNil(t, checked.Values["counts"], "[] stays the empty-list marker")
}

// TestCheckUpdateValuesJudgesArraysByTheSkeleton pins the second rule: with a
// skeleton, a value inside arrays is refused exactly where the schema's
// structure admits no such arrays around it, there, whatever the spelling.
// A primitive array the schema declares under an element-typed ledger entry
// is written as before; a single value of the wrong type stays report-only.
func TestCheckUpdateValuesJudgesArraysByTheSkeleton(t *testing.T) {
	shape := shapedSkeleton(t)
	for _, doc := range []map[string]any{
		{"phones": []any{"1", 2}},
		{"phones": []any{}},
		{"tags": []any{"a"}, "counts": []any{}},
		{"items": []any{map[string]any{"note": "n", "qty": 1}}},
		{"items.note": "n"},
		{"items": []any{map[string]any{"note": "n"}}, "items.qty": 2},
		{"name": "not-in-the-enum", "age": "12"},
	} {
		checked, err := CheckUpdateValues(doc, shapedCache(), shape)
		require.NoError(t, err)
		require.NoErrorf(t, checked.Refusal, "%v is the schema's shape", doc)
	}

	declares := "inside arrays the schema does not declare there; an object or array, empty or not, " + singleValueRule
	unread := "inside arrays its stored record would not read back from; an object or array, empty or not, " + singleValueRule
	const verdict, twoArrays = "container skeleton verdict", "2 arrays around"
	for name, tc := range map[string]struct {
		doc          map[string]any
		want, detail string
	}{
		"empty array":          {map[string]any{"name": []any{}}, refusedValue("name", 1) + "declared type text takes a single value, got an empty array, " + declares, verdict},
		"nulls":                {map[string]any{"name": []any{nil, nil}}, refusedValue("name", 1) + "declared type text takes a single value, got an array, " + declares, verdict},
		"singleton":            {map[string]any{"label": []any{"a"}}, refusedValue("label", 2) + `declared type text takes a single value, got a value at array indices "0", ` + declares, verdict},
		"typed nil slice":      {map[string]any{"age": []any(nil)}, refusedValue("age", 7) + "declared type integer takes a single value, got an empty array, ", verdict},
		"literal dotted key":   {map[string]any{"items.note": []any{"n"}}, refusedValue("items.note", 5) + `declared type text takes a single value, got a value at array indices "0", `, verdict},
		"member":               {map[string]any{"items": []any{map[string]any{"note": []any{"n"}}}}, refusedValue("items.note", 5) + `declared type text takes a single value, got a value at array indices "0,0", `, verdict},
		"dotted in an element": {map[string]any{"items": []any{map[string]any{"note": "n"}, map[string]any{"qty": []any{}}}}, refusedValue("items.qty", 6) + `declared type smallint takes a single value, got an empty array at array indices "1", `, verdict},
		"extra array":          {map[string]any{"items": []any{[]any{map[string]any{"note": "n"}}}}, refusedValue("items.note", 5) + `declared type text takes a single value, got a value at array indices "0,0", ` + unread, twoArrays},
		"nested in an array":   {map[string]any{"phones": []any{[]any{"1"}}}, refusedValue("phones", 30) + `declared type text takes a single value, got a value at array indices "0,0", ` + unread, twoArrays},
		"nested empty array":   {map[string]any{"phones": []any{[]any{}}}, refusedValue("phones", 30) + `declared type text takes a single value, got an empty array at array indices "0", ` + unread, twoArrays},
	} {
		t.Run(name, func(t *testing.T) {
			checked, err := CheckUpdateValues(tc.doc, shapedCache(), shape)
			require.NoError(t, err)
			require.ErrorIs(t, checked.Refusal, forma.ErrInvalidInput)
			assert.Contains(t, published(t, checked.Refusal), tc.want)
			assert.NotContains(t, published(t, checked.Refusal), "validating", "the skeleton's verdict is operator detail")
			assert.NotContains(t, published(t, checked.Refusal), tc.detail, "the rule's reason is operator detail")
			assert.Contains(t, checked.Refusal.Error(), tc.detail)
		})
	}

	// A list's own nested [] is refused as the walk refuses it, with a skeleton
	// or without, whether the cache declares array ancestors or not.
	for shapes, shape := range map[string]*schemavalidate.ShapeSchema{"no skeleton": nil, "skeleton": shape} {
		checked, err := CheckUpdateValues(map[string]any{"tags": []any{[]any{}}}, shapedCache(), shape)
		require.NoError(t, err)
		require.ErrorIs(t, checked.Refusal, forma.ErrInvalidInput, shapes)
		assert.Contains(t, published(t, checked.Refusal), refusedValue("tags", 3)+`empty list at array indices "0" is nested in more arrays`, shapes)
	}
}

// TestToAttributesRefusesAnObjectAtAnAttribute pins the rule in the write's
// own walk, which every write stores through: a create, and an update's
// merged document whatever route reached it.
func TestToAttributesRefusesAnObjectAtAnAttribute(t *testing.T) {
	tr := NewTransformer(&stubSchemaRegistry{schemaID: 1, schemaName: "shaped", cache: shapedCache()})
	for _, doc := range []map[string]any{
		{"name": map[string]any{}},
		{"label": map[string]any{"x": "y"}},
		{"tags": []any{[]any{map[string]any{}}}},
	} {
		_, err := tr.ToAttributes(context.Background(), 1, uuid.Must(uuid.NewV7()), doc)
		require.ErrorIsf(t, err, forma.ErrInvalidInput, "%v", doc)
		assert.Containsf(t, published(t, err), "an object, empty or not, "+singleValueRule, "%v", doc)
	}
}

// TestMergeUpdateJudgesTheArraysOfThePersistedDocument pins #696 review
// R1-F4 at the persistence boundary: the document an update persists is
// judged by the same array-placement rule as its Updates, whatever reached
// it, with or without a skeleton. Without one the cache's array ancestry
// refuses an extra array around the array of objects; a stored value is
// never judged, so a row the rule would refuse stays updatable.
func TestMergeUpdateJudgesTheArraysOfThePersistedDocument(t *testing.T) {
	ctx := context.Background()
	cache := forma.SchemaAttributeCache{
		"name":       {AttributeName: "name", AttributeID: 1, ValueType: forma.ValueTypeText, ArrayAncestors: []string{}},
		"phones":     {AttributeName: "phones", AttributeID: 30, ValueType: forma.ValueTypeText, ArrayAncestors: []string{}},
		"items.note": {AttributeName: "items.note", AttributeID: 5, ValueType: forma.ValueTypeText, ArrayAncestors: []string{"items"}},
	}
	registry := &stubSchemaRegistry{schemaID: 1, schemaName: "shaped", cache: cache}
	rowID := uuid.Must(uuid.NewV7())
	stored, err := NewPersistentRecordTransformer(registry).ToPersistentRecord(ctx, 1, rowID,
		map[string]any{"name": []any{"legacy"}, "items": []any{map[string]any{"note": "a"}}})
	require.NoError(t, err, "a create's arrays are judged by strict validation, not here")

	for shapes, shape := range map[string]*schemavalidate.ShapeSchema{"no skeleton": nil, "skeleton": shapedSkeleton(t)} {
		tr := NewPersistentRecordTransformer(registry)
		tr.(ShapeAware).SetShapes(func(int16) *schemavalidate.ShapeSchema { return shape })
		for _, updates := range []map[string]any{
			{"items": []any{[]any{map[string]any{"note": "x"}}}},
			{"items": []any{map[string]any{"note": []any{nil}}}},
			{"name": []any{}},
		} {
			_, err := tr.MergeUpdate(ctx, stored, setting(updates))
			require.ErrorIsf(t, err, forma.ErrInvalidInput, "%s: %v", shapes, updates)
			assert.Contains(t, published(t, err), "cannot be stored where a single value belongs", shapes)
		}

		merged, err := tr.MergeUpdate(ctx, stored, setting(map[string]any{"items": []any{map[string]any{"note": "y"}}}))
		require.NoError(t, err, "%s: the stored name inside an array is not the caller's", shapes)
		assert.Equal(t, []any{"legacy"}, merged["name"])

		_, err = tr.MergeUpdate(ctx, stored, setting(map[string]any{"phones": []any{"1", "2"}}))
		if shape == nil {
			require.ErrorIs(t, err, forma.ErrInvalidInput, "without a skeleton the ledger declares a single value")
		} else {
			require.NoError(t, err, "the skeleton admits the primitive array the schema declares")
		}
	}
}
