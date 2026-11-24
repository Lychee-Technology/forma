package transform

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// The shape rule of #696 over an update's merged document and at the
// nesting cap, and its place beside the intake's unstorable-text error.

func shapeTextRecord(rowID uuid.UUID, attrID int16, indices, value string) model.EAVRecord {
	return model.EAVRecord{SchemaID: shapeSchemaID, RowID: rowID, AttrID: attrID, ArrayIndices: indices, ValueText: &value}
}

// A row of a schema whose registry derived no array ancestry rebuilds the
// members of an array of objects by inference, and reads one as an array of
// its own beside an object member (rebuildInferred): items becomes an object
// whose name holds an array. Those records carry no more indices than the
// document declares arrays of objects around them, so an update that keeps
// them writes each back where it was, and is not refused; one more index
// than that is an array stored under the scalar, which is.
func TestScalarShapeStoredMembersWithoutAncestryAreWrittenBack(t *testing.T) {
	ctx := context.Background()
	tr := NewPersistentRecordTransformer(newShapeRegistry(false))
	rowID := uuid.New()
	stored := func(records ...model.EAVRecord) *model.PersistentRecord {
		return &model.PersistentRecord{SchemaID: shapeSchemaID, RowID: rowID, OtherAttributes: records}
	}

	row := stored(shapeTextRecord(rowID, 15, "0", "w"), shapeTextRecord(rowID, 5, "0", "a"), shapeTextRecord(rowID, 5, "1", "b"))
	read, err := tr.FromPersistentRecord(ctx, row)
	require.NoError(t, err)
	require.Equal(t, []any{"a", "b"}, read["items"].(map[string]any)["name"], "the fixture must read items.name as an array")
	merged, err := tr.MergeUpdate(ctx, row, setting(map[string]any{"count": 1}))
	require.NoError(t, err)
	written, err := tr.ToPersistentRecord(ctx, shapeSchemaID, rowID, merged)
	require.NoError(t, err)
	assert.ElementsMatch(t, append(storedKeys(row.OtherAttributes), "3[]=1"), writtenKeys(written))

	// The caller's own array there is refused all the same.
	merged, err = tr.MergeUpdate(ctx, row, func(base map[string]any) (map[string]any, error) {
		base["items"].(map[string]any)["name"] = []any{"c"}
		return base, nil
	})
	require.NoError(t, err)
	_, err = tr.ToPersistentRecord(ctx, shapeSchemaID, rowID, merged)
	requireShapeRefusal(t, err, shapeRefusal("items.name", 5, "an array of 1 element"))

	_, err = tr.MergeUpdate(ctx, stored(shapeTextRecord(rowID, 5, "0,0", "a")), setting(map[string]any{"count": 1}))
	require.Error(t, err)
	require.NotErrorIs(t, err, forma.ErrInvalidInput, "the stored row is at fault")
	assert.Contains(t, err.Error(), "attrID=5 arrayIndices=0,0")

	_, err = tr.MergeUpdate(ctx, stored(shapeTextRecord(rowID, 8, "0,0", "1")), setting(map[string]any{"count": 1}))
	require.Error(t, err, "a pre-#204 primitive array's element stored as an array")
	require.NotErrorIs(t, err, forma.ErrInvalidInput)
	assert.Contains(t, err.Error(), "attrID=8 arrayIndices=0,0")
}

// A cyclic merge result at a scalar attribute is refused at the nesting cap,
// as one at an object parent is (TestMergeUpdateRefusesCyclicMergeResult),
// rather than searched for stored values without end (#696 review).
func TestScalarShapeCyclicMergeResultAtAScalar(t *testing.T) {
	ctx := context.Background()
	registry := newShapeRegistry(true)
	tr := NewPersistentRecordTransformer(registry)
	rowID := uuid.New()
	for name, cyclic := range map[string]func() any{
		"map":   func() any { m := map[string]any{}; m["self"] = m; return m },
		"slice": func() any { s := []any{nil}; s[0] = s; return s },
	} {
		_, err := tr.MergeUpdate(ctx, storedShapeRow(rowID), func(base map[string]any) (map[string]any, error) {
			base["note"] = cyclic()
			return base, nil
		})
		requireTooDeep(t, err, "note")

		_, err = tr.ToPersistentRecord(ctx, shapeSchemaID, rowID, map[string]any{"note": cyclic()})
		requireTooDeep(t, err, "note")

		checked, err := CheckUpdateValues(map[string]any{"note": cyclic()}, registry.cache, SchemaShapesOf(registry, shapeSchemaID))
		require.NoError(t, err, "%s: the merged write refuses the cap at its own stage", name)
		assert.NoError(t, checked.Refusal, name)
	}
}

// A container the shape rule refuses that is itself past the nesting cap is
// refused as too deep, not as the shape.
func TestScalarShapeRefusedContainerPastTheCap(t *testing.T) {
	registry := newShapeRegistry(true)
	var items any = []any{map[string]any{"name": map[string]any{}}}
	for range maxPayloadNestingDepth - 2 {
		items = []any{items}
	}
	// items is at depth 1, the innermost array at 999, its object at 1000
	// and name's object at 1001.
	doc := map[string]any{"items": items}
	_, err := NewPersistentRecordTransformer(registry).ToPersistentRecord(context.Background(), shapeSchemaID, uuid.New(), doc)
	requireTooDeep(t, err, "items")
	checked, err := CheckUpdateValues(doc, registry.cache, SchemaShapesOf(registry, shapeSchemaID))
	require.NoError(t, err)
	assert.NoError(t, checked.Refusal)
}

// Text PostgreSQL cannot hold stays the intake's error inside a container
// the shape rule refuses, as it was before the rule refused the container
// whole, so it answers whatever the row; a container that holds none is the
// Refusal, which waits for a row to write (#696 review).
func TestScalarShapeKeepsTheUnstorableTextError(t *testing.T) {
	registry := newShapeRegistry(true)
	shapes := SchemaShapesOf(registry, shapeSchemaID)

	for _, doc := range []map[string]any{
		{"note": []any{"a\x00b"}},
		{"note": []any{[]any{"a\x00b"}}},
		{"count": 1.5, "note": []any{"a\x00b"}},
	} {
		_, err := CheckUpdateValues(doc, registry.cache, shapes)
		require.ErrorIs(t, err, forma.ErrInvalidInput, doc)
		assert.Contains(t, err.Error(), refusedValue("note", 2)+nulRule, doc)
	}

	checked, err := CheckUpdateValues(map[string]any{"note": []any{"a"}, "tenant": "ok"}, registry.cache, shapes)
	require.NoError(t, err)
	requireShapeRefusal(t, checked.Refusal, shapeRefusal("note", 2, "an array of 1 element"))
	assert.Equal(t, []any{"a"}, checked.Values["note"])
}
