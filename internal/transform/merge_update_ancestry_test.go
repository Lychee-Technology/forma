package transform

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

// With array ancestry declared, an update that addresses none of a row's
// records writes every one of them back (#623), over every row
// TestFromAttributesByAncestryWritesBackEveryRecord rebuilds: none is
// refused, including the arrays nested inside an array of objects that a row
// without ancestry refuses (TestMergeUpdateWritesBackEveryStoredRecordOrRefuses).
func TestMergeUpdateByAncestryWritesBackEveryStoredRecord(t *testing.T) {
	tr := NewPersistentRecordTransformer(newAncestryRegistry())
	ctx := context.Background()
	for _, row := range ancestryRows() {
		rowID := uuid.Must(uuid.NewV7())
		stored := storedOrderRow(rowID, row...)
		merged, err := tr.MergeUpdate(ctx, stored, setting(map[string]any{"tags": []any{"unrelated"}}))
		require.NoError(t, err, "row %+v", row)

		written, err := tr.ToPersistentRecord(ctx, 619, rowID, merged)
		require.NoError(t, err, "row %+v merged as %v", row, merged)
		require.ElementsMatch(t, append(storedKeys(stored.OtherAttributes), "11[0]=unrelated"),
			storedKeys(written.OtherAttributes), "row %+v merged as %v", row, merged)
	}
}

// The documents a row without ancestry refuses to update
// (TestMergeUpdateRefusesToDropUnplacedStoredValue,
// TestMergeUpdateRefusesToDropLegacyArrayInsideObjectArray) update with it,
// and read back as written. So do the documents that store a member of the
// inner array with fewer indices than its ancestors: one element holding
// that array as an object, which an update's report-only validation lets
// through, and literal dotted keys crossing it beside its elements.
func TestMergeUpdateByAncestryKeepsNestedArrays(t *testing.T) {
	items := func(elements ...any) map[string]any {
		return map[string]any{"order": map[string]any{"items": elements}}
	}
	for name, doc := range map[string]map[string]any{
		"array of objects inside an array of objects":        lotsDocument("c"),
		"legacy text-typed array inside an array of objects": items(map[string]any{"sku": []any{"a", "b"}}),
		"inner array held as an object in one element": items(
			map[string]any{"lots": []any{map[string]any{"code": "c"}}},
			map[string]any{"lots": map[string]any{"code": "x"}},
		),
		"literal keys crossing the inner array": items(
			map[string]any{"lots": []any{map[string]any{"code": "c"}}, "lots.n": "y"},
			map[string]any{"lots.n": "z"},
		),
	} {
		t.Run(name, func(t *testing.T) {
			tr := NewPersistentRecordTransformer(newAncestryRegistry())
			ctx := context.Background()
			rowID := uuid.Must(uuid.NewV7())
			stored, err := tr.ToPersistentRecord(ctx, 619, rowID, doc)
			require.NoError(t, err)
			read, err := tr.FromPersistentRecord(ctx, stored)
			require.NoError(t, err)
			require.Equal(t, doc, read)

			merged, err := tr.MergeUpdate(ctx, stored, setting(map[string]any{"order.note": "n"}))
			require.NoError(t, err)
			written, err := tr.ToPersistentRecord(ctx, 619, rowID, merged)
			require.NoError(t, err)
			require.ElementsMatch(t, append(storedKeys(stored.OtherAttributes), "10[]=n"), storedKeys(written.OtherAttributes))
		})
	}
}

// A stored value the rebuild places under a literal key is replaced by the
// caller's own spelling of its attribute like any other (#312): the caller's
// key wins, and the stored value is discarded rather than refused.
func TestMergeUpdateReplacesLiteralPlacedStoredValue(t *testing.T) {
	tr := NewPersistentRecordTransformer(newAncestryRegistry())
	ctx := context.Background()
	rowID := uuid.Must(uuid.NewV7())
	stored := storedOrderRow(rowID, rebuildAttr(9, "", "a"), rebuildAttr(8, "0", int64(1)))

	merged, err := tr.MergeUpdate(ctx, stored, setting(map[string]any{"order.items.sku": "z"}))
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"order":           map[string]any{"items": []any{map[string]any{"qty": int64(1)}}},
		"order.items.sku": "z",
	}, merged)
	written, err := tr.ToPersistentRecord(ctx, 619, rowID, merged)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"8[0]=1", "9[]=z"}, storedKeys(written.OtherAttributes))
}

// A row whose records of one attribute were written through different
// spellings, nested in one element and as a literal dotted key beside the
// array in another, rebuilds them under both. One write cannot store it,
// since a write keeps one spelling per attribute (#312), so an update that
// keeps both would delete the records of the losing one. It is refused
// instead, blaming the stored row, unless the caller's own spelling replaces
// the attribute.
func TestMergeUpdateRefusesRespelledStoredValue(t *testing.T) {
	tr := NewPersistentRecordTransformer(newAncestryRegistry())
	ctx := context.Background()
	rowID := uuid.Must(uuid.NewV7())
	stored := storedOrderRow(rowID, rebuildAttr(18, "0,0", "c"), rebuildAttr(18, "1", "x"), rebuildAttr(20, "1,0", "y"))

	read, err := tr.FromPersistentRecord(ctx, stored)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"order": map[string]any{"items": []any{
		map[string]any{"lots": []any{map[string]any{"code": "c"}}},
		map[string]any{"lots": []any{map[string]any{"n": "y"}}, "lots.code": "x"},
	}}}, read)

	_, err = tr.MergeUpdate(ctx, stored, setting(map[string]any{"tags": []any{"unrelated"}}))
	require.EqualError(t, err, "record schema=619 row="+rowID.String()+" attrID=18 arrayIndices=0,0: "+
		"stored text value of attribute 'order.items.lots.code' rebuilds under a different key spelling than the attribute's other stored records, "+
		"and a write keeps one spelling per attribute, so this update cannot write it back; "+
		"an update must replace the attribute or a container holding it")
	require.NotErrorIs(t, err, forma.ErrInvalidInput)

	merged, err := tr.MergeUpdate(ctx, stored, setting(map[string]any{"order.items.lots.code": "d"}))
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"order": map[string]any{"items": []any{
			map[string]any{}, map[string]any{"lots": []any{map[string]any{"n": "y"}}},
		}},
		"order.items.lots.code": "d",
	}, merged)
}

// An empty-list marker written before #623 carried no indices wherever its
// list was. Beside the elements of the array of objects around the list it
// names none of them: the read leaves it out, and an update writes the row
// without it.
func TestMergeUpdateDropsUnindexedMarkerBesideElements(t *testing.T) {
	tr := NewPersistentRecordTransformer(newAncestryRegistry())
	ctx := context.Background()
	rowID := uuid.Must(uuid.NewV7())
	stored := storedOrderRow(rowID, rebuildAttr(19, "", nil), rebuildAttr(9, "0", "a"))

	merged, err := tr.MergeUpdate(ctx, stored, setting(map[string]any{"tags": []any{"unrelated"}}))
	require.NoError(t, err)
	written, err := tr.ToPersistentRecord(ctx, 619, rowID, merged)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"9[0]=a", "11[0]=unrelated"}, storedKeys(written.OtherAttributes))
}

// The empty-list marker of a list inside an array of objects carries its
// element's indices when the attribute declares its array ancestors, and no
// indices otherwise, the form a row read without them writes back unchanged.
func TestEmptyListMarkerCarriesItsElementUnderAncestry(t *testing.T) {
	doc := map[string]any{"order": map[string]any{"items": []any{
		map[string]any{"sku": "a"}, map[string]any{"tags": []any{}},
	}}}
	for name, tc := range map[string]struct {
		registry forma.SchemaRegistry
		want     []string
	}{
		"array ancestry": {registry: newAncestryRegistry(), want: []string{"9[0]=a", "19[1]=<none>"}},
		"inferred":       {registry: newRegistryWithoutAncestry(), want: []string{"9[0]=a", "19[]=<none>"}},
	} {
		t.Run(name, func(t *testing.T) {
			stored, err := NewPersistentRecordTransformer(tc.registry).
				ToPersistentRecord(context.Background(), 619, uuid.Must(uuid.NewV7()), doc)
			require.NoError(t, err)
			require.ElementsMatch(t, tc.want, storedKeys(stored.OtherAttributes))
		})
	}
}

// A row written while its schema declared array ancestry holds the indexed
// marker of a list inside an array of objects. A registry that no longer
// declares that ancestry, as one does once the schema document reaches an
// attribute through another file's $ref, reads the row by inference and
// without the marker. An update of the row is then refused rather than
// deleting the marker's record, whatever the update sends: the merge base
// does not hold the marker, so nothing tells whether the update replaces
// it. The refusal follows the row's reading, so a row read by inference
// because one of its attributes alone declares no ancestry is refused too.
func TestMergeUpdateRefusesMarkerReadWithoutAncestry(t *testing.T) {
	ctx := context.Background()
	rowID := uuid.Must(uuid.NewV7())
	stored, err := NewPersistentRecordTransformer(newAncestryRegistry()).ToPersistentRecord(ctx, 619, rowID, map[string]any{
		"order": map[string]any{"note": "n", "items": []any{map[string]any{"sku": "a"}, map[string]any{"tags": []any{}}}},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"10[]=n", "9[0]=a", "19[1]=<none>"}, storedKeys(stored.OtherAttributes))

	noteUndeclared := newAncestryRegistry()
	note := noteUndeclared.cache["order.note"]
	note.ArrayAncestors = nil
	noteUndeclared.cache["order.note"] = note
	for name, registry := range map[string]forma.SchemaRegistry{
		"no attribute declares it": newRegistryWithoutAncestry(),
		"one attribute does not":   noteUndeclared,
	} {
		t.Run(name, func(t *testing.T) {
			tr := NewPersistentRecordTransformer(registry)
			read, err := tr.FromPersistentRecord(ctx, stored)
			require.NoError(t, err)
			require.Equal(t, map[string]any{"order": map[string]any{
				"note": "n", "items": []any{map[string]any{"sku": "a"}},
			}}, read)

			for _, update := range []map[string]any{
				{"tags": []any{"unrelated"}},
				{"order": map[string]any{"items": []any{}}},
			} {
				_, err := tr.MergeUpdate(ctx, stored, setting(update))
				require.EqualError(t, err, "record schema=619 row="+rowID.String()+" attrID=19 arrayIndices=1: "+
					"stored empty-list marker of list attribute 'order.items.tags' carries array indices, "+
					"which only a rebuild by array ancestry places, and the registry does not declare the ancestry of every attribute in the row, "+
					"so this update cannot write it back; the registry must declare the schema's array ancestry, "+
					"as the file registry does when the schema document resolves every attribute's ancestors",
					"update %v", update)
				require.NotErrorIs(t, err, forma.ErrInvalidInput)
			}
		})
	}
}

// An empty list beside a required sibling inside an array of objects is
// accepted: its marker is present in the sibling's element, so the required
// policy judges the sibling there. A marker with no indices made the parent
// present at the top level, where the sibling is never stored, and refused
// the write.
func TestEmptyListBesideRequiredSiblingInObjectArray(t *testing.T) {
	registry := newAncestryRegistry()
	sku := registry.cache["order.items.sku"]
	sku.RequiredPolicy = forma.RequiredPolicyIfParentPresent
	registry.cache["order.items.sku"] = sku
	tr := NewPersistentRecordTransformer(registry)
	ctx := context.Background()
	doc := map[string]any{"order": map[string]any{"items": []any{
		map[string]any{"sku": "a", "tags": []any{}},
	}}}

	stored, err := tr.ToPersistentRecord(ctx, 619, uuid.Must(uuid.NewV7()), doc)
	require.NoError(t, err)
	read, err := tr.FromPersistentRecord(ctx, stored)
	require.NoError(t, err)
	require.Equal(t, doc, read)
}
