package transform

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemavalidate"
)

func skeletonOf(t *testing.T, document string) *schemavalidate.ShapeSchema {
	t.Helper()
	v, err := schemavalidate.New(shapeRegistry{document: document}, t.TempDir())
	require.NoError(t, err)
	require.NotNil(t, v.Shape(1))
	return v.Shape(1)
}

// TestPlacementJudgeKeepsPathsThatPrintAlikeApart pins #696 review R2-F2:
// the memo of skeleton verdicts names a path unambiguously, so ["a", "b c"]
// and ["a b", "c"] are judged apart, whichever is judged first and
// whichever the schema declares an array.
func TestPlacementJudgeKeepsPathsThatPrintAlikeApart(t *testing.T) {
	array, scalar := `{"type":"array","items":{"type":"string"}}`, `{"type":"string"}`
	of := func(first, second string) string {
		return `{"type":"object","properties":{"a":{"type":"object","properties":{"b c":` + first + `}},
			"a b":{"type":"object","properties":{"c":` + second + `}}}}`
	}
	first, second := []string{"a", "b c"}, []string{"a b", "c"}
	meta := forma.AttributeMetadata{AttributeID: 2, ValueType: forma.ValueTypeText, ArrayAncestors: []string{}}
	inArray := docPosition{indices: []int{0}, arrays: []int{0, 1}}
	for name, tc := range map[string]struct {
		document         string
		declared, scalar []string
	}{
		"first declared":  {of(array, scalar), first, second},
		"second declared": {of(scalar, array), second, first},
	} {
		shape := skeletonOf(t, tc.document)
		for _, order := range [][][]string{{tc.declared, tc.scalar}, {tc.scalar, tc.declared}} {
			judge := newPlacementJudge(nil, shape)
			for _, path := range order {
				err := judge.value(path, meta, inArray, "a value")
				if path[0] == tc.declared[0] {
					assert.NoErrorf(t, err, "%s: %v declares the array", name, path)
				} else {
					assert.ErrorIsf(t, err, forma.ErrInvalidInput, "%s: %v is a single value", name, path)
				}
			}
		}
	}
	assert.NotEqual(t, arrangementKey(first, []int{0, 1}), arrangementKey(second, []int{0, 1}))
}

// nestedIDSchema reaches note, a scalar, and codes, a primitive array under
// an element-typed ledger entry, through pointers that cross inner's $id
// into a oneOf (#696 review R2-F1).
const nestedIDSchema = `{"type":"object","$defs":{"inner":{"$id":"inner.json","properties":{
	"x":{"oneOf":[{"type":"string"},{"type":"integer"}]},
	"y":{"oneOf":[{"type":"array","items":{"type":"string"}},{"type":"integer"}]}}}},"properties":{
	"note":{"$ref":"#/$defs/inner/properties/x/oneOf/0"},
	"codes":{"$ref":"#/$defs/inner/properties/y/oneOf/0"}}}`

// TestMergeUpdateJudgesValuesReferencedAcrossANestedID pins R2-F1 at the
// persistence boundary: the document an update persists takes no array at
// the referenced scalar, and the referenced array's own elements.
func TestMergeUpdateJudgesValuesReferencedAcrossANestedID(t *testing.T) {
	ctx := context.Background()
	cache := forma.SchemaAttributeCache{
		"note":  {AttributeName: "note", AttributeID: 1, ValueType: forma.ValueTypeText, ArrayAncestors: []string{}},
		"codes": {AttributeName: "codes", AttributeID: 2, ValueType: forma.ValueTypeText, ArrayAncestors: []string{}},
	}
	registry := &stubSchemaRegistry{schemaID: 1, schemaName: "shaped", cache: cache}
	stored, err := NewPersistentRecordTransformer(registry).ToPersistentRecord(ctx, 1, uuid.Must(uuid.NewV7()),
		map[string]any{"note": "n", "codes": []any{"c1"}})
	require.NoError(t, err)

	shape := skeletonOf(t, nestedIDSchema)
	tr := NewPersistentRecordTransformer(registry)
	tr.(ShapeAware).SetShapes(func(int16) *schemavalidate.ShapeSchema { return shape })
	for _, updates := range []map[string]any{{"note": []any{"x"}}, {"note": []any{}}, {"codes": []any{[]any{"c"}}}} {
		_, err := tr.MergeUpdate(ctx, stored, setting(updates))
		require.ErrorIsf(t, err, forma.ErrInvalidInput, "%v", updates)
		assert.Contains(t, published(t, err), "cannot be stored where a single value belongs")
	}
	merged, err := tr.MergeUpdate(ctx, stored, setting(map[string]any{"codes": []any{"c2", "c3"}, "note": "m"}))
	require.NoError(t, err)
	assert.Equal(t, []any{"c2", "c3"}, merged["codes"])
	assert.Equal(t, "m", merged["note"])
}
