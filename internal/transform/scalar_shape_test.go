package transform

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// scalarShapeCache declares scalars in EAV and bound to a column, at the top
// level, in an object and in an array of objects, beside a list. With
// ancestry every attribute carries ArrayAncestors, as the file registry
// derives them; without, none does, as for a schema whose document leaves an
// ancestor unresolved.
func scalarShapeCache(ancestry bool) forma.SchemaAttributeCache {
	cache := forma.SchemaAttributeCache{
		"name":  {AttributeID: 1, ValueType: forma.ValueTypeText},
		"label": {AttributeID: 2, ValueType: forma.ValueTypeText, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01}},
		"age":   {AttributeID: 3, ValueType: forma.ValueTypeInteger},
		"ref":   {AttributeID: 4, ValueType: forma.ValueTypeUUID},
		"tags":  {AttributeID: 5, ValueType: forma.ValueTypeList},
		// contact is an object, items an array of objects.
		"contact.email": {AttributeID: 6, ValueType: forma.ValueTypeText},
		"items.sku":     {AttributeID: 7, ValueType: forma.ValueTypeText},
		"items.tags":    {AttributeID: 8, ValueType: forma.ValueTypeList},
	}
	if ancestry {
		for name, meta := range cache {
			meta.ArrayAncestors = []string{}
			if strings.HasPrefix(name, "items.") {
				meta.ArrayAncestors = []string{"items"}
			}
			cache[name] = meta
		}
	}
	return cache
}

// shapeRefused is the published prefix of a scalar-shape refusal.
func shapeRefused(attr string, id int, vt forma.ValueType, kind string) string {
	return refusedValue(attr, id) + "expected a scalar of type " + string(vt) + ", got " + kind
}

// stringerSlice is a Go slice carrying its own text, which the write
// converts as one scalar.
type stringerSlice []string

func (s stringerSlice) String() string { return strings.Join(s, ",") }

func refusedScalarShapes(ancestry bool) []intakeCase {
	cases := []intakeCase{
		{"EAV, {}", map[string]any{"name": map[string]any{}}, shapeRefused("name", 1, "text", "an empty object")},
		{"EAV, []", map[string]any{"name": []any{}}, shapeRefused("name", 1, "text", "an empty array")},
		{"EAV, object", map[string]any{"name": map[string]any{"x": "y"}}, shapeRefused("name", 1, "text", "an object")},
		{"EAV, array", map[string]any{"name": []any{"a@x"}}, shapeRefused("name", 1, "text", "an array")},
		{"bound, {}", map[string]any{"label": map[string]any{}}, shapeRefused("label", 2, "text", "an empty object")},
		{"bound, []", map[string]any{"label": []any{}}, shapeRefused("label", 2, "text", "an empty array")},
		{"bound, array", map[string]any{"label": []any{"a"}}, shapeRefused("label", 2, "text", "an array")},
		{"integer, nested array", map[string]any{"age": []any{[]any{1}}}, shapeRefused("age", 3, "integer", "an array")},
		{"Go slice", map[string]any{"name": []string{"a"}}, shapeRefused("name", 1, "text", "an array")},
		{"Go map", map[string]any{"age": map[string]int{}}, shapeRefused("age", 3, "integer", "an empty object")},
		{"Go slice of UUIDs", map[string]any{"ref": []uuid.UUID{uuid.New()}}, shapeRefused("ref", 4, "uuid", "an array")},
	}
	if !ancestry {
		return cases
	}
	return append(cases,
		intakeCase{"in an object, nested", map[string]any{"contact": map[string]any{"email": map[string]any{}}}, shapeRefused("contact.email", 6, "text", "an empty object")},
		intakeCase{"in an object, dotted", map[string]any{"contact.email": []any{"a"}}, shapeRefused("contact.email", 6, "text", "an array")},
		intakeCase{"in an array of objects", map[string]any{"items": []any{map[string]any{"sku": []any{"a"}}}}, shapeRefused("items.sku", 7, "text", "an array")},
		intakeCase{"in an array of objects, {}", map[string]any{"items": []any{map[string]any{"sku": map[string]any{}}}}, shapeRefused("items.sku", 7, "text", "an empty object")},
	)
}

func uuidBytes() []byte {
	id := uuid.New()
	return id[:]
}

// Positions the rule leaves as they were: a scalar representation the write
// converts, a list, the members of an array of objects, and every position
// the metadata does not place.
func unchangedScalarShapes(ancestry bool) []intakeCase {
	cases := []intakeCase{
		{"[]byte", map[string]any{"name": []byte("a")}, ""},
		{"named byte slice", map[string]any{"name": net.IPv4(10, 0, 0, 1)}, ""},
		{"Stringer slice", map[string]any{"name": stringerSlice{"a", "b"}}, ""},
		{"uuid.UUID", map[string]any{"ref": uuid.New()}, ""},
		{"16-byte UUID", map[string]any{"ref": uuidBytes()}, ""},
		{"list", map[string]any{"tags": []any{"a"}, "items": []any{map[string]any{"tags": []any{}}}}, ""},
		{"empty list", map[string]any{"tags": []any{}}, ""},
		{"array of objects", map[string]any{"items": []any{map[string]any{"sku": "a"}}}, ""},
		{"dotted spelling across a declared array", map[string]any{"items.sku": []any{"a", "b"}}, ""},
		{"array level the schema does not declare", map[string]any{"items": []any{[]any{map[string]any{"sku": []any{"a"}}}}}, ""},
	}
	if ancestry {
		return cases
	}
	return append(cases,
		intakeCase{"unresolved ancestry, nested", map[string]any{"contact": map[string]any{"email": map[string]any{}}}, ""},
		intakeCase{"unresolved ancestry, dotted", map[string]any{"contact.email": []any{"a"}}, ""},
	)
}

// A scalar attribute given an object or an array is refused at intake,
// whatever its binding, with or without derived ancestry, as a Refusal: the
// class a route raises once it knows it writes a row (#924).
func TestCheckUpdateValues_RefusesContainersAtScalars(t *testing.T) {
	for _, ancestry := range []bool{true, false} {
		cache := scalarShapeCache(ancestry)
		for _, tc := range refusedScalarShapes(ancestry) {
			checked, err := CheckUpdateValues(tc.doc, cache)
			require.NoError(t, err, "%s: a shape refusal is no unstorable text", tc.name)
			require.ErrorIs(t, checked.Refusal, forma.ErrInvalidInput, "%s (ancestry %v)", tc.name, ancestry)
			message, published := forma.ResolvePublicMessage(checked.Refusal)
			require.True(t, published, tc.name)
			assert.Contains(t, message, tc.want, tc.name)
			assert.Contains(t, message, "an empty one does not clear it", tc.name)
		}
		for _, tc := range unchangedScalarShapes(ancestry) {
			checked, err := CheckUpdateValues(tc.doc, cache)
			if err == nil && checked.Refusal != nil {
				err = checked.Refusal
			}
			if err != nil {
				assert.NotContains(t, err.Error(), "expected a scalar of type", "%s (ancestry %v)", tc.name, ancestry)
			}
		}
	}
}

// Of a shape refusal and a conversion refusal, intake answers the first in
// the write's sorted key order; unstorable text stays the error even inside
// a refused container.
func TestCheckUpdateValues_ShapeRefusalOrder(t *testing.T) {
	cache := scalarShapeCache(true)
	checked, err := CheckUpdateValues(map[string]any{"age": 1.5, "name": map[string]any{}}, cache)
	require.NoError(t, err)
	assert.Contains(t, checked.Refusal.Error(), "'age'")

	checked, err = CheckUpdateValues(map[string]any{"age": map[string]any{}, "ref": "bad"}, cache)
	require.NoError(t, err)
	assert.Contains(t, checked.Refusal.Error(), shapeRefused("age", 3, "integer", "an empty object"))

	_, err = CheckUpdateValues(map[string]any{"name": []any{"a\x00b"}}, cache)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	assert.Contains(t, err.Error(), nulRule)
}

func scalarShapeRegistry() forma.SchemaRegistry {
	return &stubSchemaRegistry{schemaID: 501, schemaName: "scalar_shape", cache: scalarShapeCache(true)}
}

// MergeUpdate refuses a merged document that gives a scalar an object or an
// array, before any stored value is decoded (#924).
func TestMergeUpdateRefusesContainersAtScalars(t *testing.T) {
	tr := NewPersistentRecordTransformer(scalarShapeRegistry())
	rowID := uuid.Must(uuid.NewV7())
	for _, tc := range refusedScalarShapes(true) {
		record := &model.PersistentRecord{SchemaID: 501, RowID: rowID,
			OtherAttributes: []model.EAVRecord{textRecord(rowID, 1, "", "kept")}}
		_, err := tr.MergeUpdate(context.Background(), record, setting(tc.doc))
		require.ErrorIs(t, err, forma.ErrInvalidInput, tc.name)
		assert.Contains(t, err.Error(), tc.want, tc.name)
	}
}

// A container the stored row holds under a scalar, written before the rule,
// is the row's: an update that does not address it writes it back as
// stored, and one that sends a scalar replaces it.
func TestMergeUpdateKeepsStoredContainersAtScalars(t *testing.T) {
	tr := NewPersistentRecordTransformer(scalarShapeRegistry())
	rowID := uuid.Must(uuid.NewV7())
	legacy := func() *model.PersistentRecord {
		return &model.PersistentRecord{SchemaID: 501, RowID: rowID, OtherAttributes: []model.EAVRecord{
			textRecord(rowID, 1, "0", "a"), textRecord(rowID, 1, "1", "b"),
		}}
	}

	merged, err := tr.MergeUpdate(context.Background(), legacy(), setting(map[string]any{"age": 3}))
	require.NoError(t, err)
	assert.Equal(t, []any{"a", "b"}, merged["name"])

	merged, err = tr.MergeUpdate(context.Background(), legacy(), setting(map[string]any{"name": "fixed"}))
	require.NoError(t, err)
	assert.Equal(t, "fixed", merged["name"])

	_, err = tr.MergeUpdate(context.Background(), legacy(), setting(map[string]any{"name": []any{"c"}}))
	require.ErrorIs(t, err, forma.ErrInvalidInput)
}
