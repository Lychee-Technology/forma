package transform

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
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

// An empty-list marker with no indices beside the elements of the array of
// objects around its list is one written through a literal dotted key that
// crosses the array, or one written before #623, when a marker carried none
// wherever its list was. Both read under the literal key, and an update
// writes the marker back rather than deleting its record.
func TestMergeUpdateKeepsUnindexedMarkerBesideElements(t *testing.T) {
	ctx := context.Background()
	tr := NewPersistentRecordTransformer(newAncestryRegistry())
	for name, doc := range map[string]map[string]any{
		"list inside the array": {"order": map[string]any{
			"items": []any{map[string]any{"sku": "a"}}, "items.tags": []any{},
		}},
		"list inside the inner array": {"order": map[string]any{
			"items": []any{map[string]any{"sku": "a"}}, "items.lots.tags": []any{},
		}},
	} {
		t.Run(name, func(t *testing.T) {
			rowID := uuid.Must(uuid.NewV7())
			stored, err := tr.ToPersistentRecord(ctx, 619, rowID, doc)
			require.NoError(t, err)
			read, err := tr.FromPersistentRecord(ctx, stored)
			require.NoError(t, err)
			require.Equal(t, doc, read)

			merged, err := tr.MergeUpdate(ctx, stored, setting(map[string]any{"tags": []any{"unrelated"}}))
			require.NoError(t, err)
			written, err := tr.ToPersistentRecord(ctx, 619, rowID, merged)
			require.NoError(t, err)
			require.ElementsMatch(t, append(storedKeys(stored.OtherAttributes), "11[0]=unrelated"), storedKeys(written.OtherAttributes))
		})
	}
}

// A row read by array ancestry can hold the empty-list marker of a list and
// a value of the same list under two key spellings, nested and under the
// literal key: a scalar stored before the attribute became a list (#372)
// beside a marker. Whichever spelling loses, the update is refused rather
// than deleting the losing record, which the merge sees because it holds a
// marker like any other stored record.
func TestMergeUpdateRefusesRespelledListRecords(t *testing.T) {
	ctx := context.Background()
	tr := NewPersistentRecordTransformer(newAncestryRegistry())
	for name, tc := range map[string]struct {
		attrs []model.EntityAttribute
		read  map[string]any
		want  string
	}{
		"value nested, marker literal": {
			attrs: []model.EntityAttribute{rebuildAttr(19, "0", "x"), rebuildAttr(19, "", nil)},
			read: map[string]any{"order": map[string]any{
				"items": []any{map[string]any{"tags": "x"}}, "items.tags": []any{},
			}},
			want: "attrID=19 arrayIndices=0: stored text value of attribute 'order.items.tags'",
		},
		"marker nested, value literal": {
			attrs: []model.EntityAttribute{rebuildAttr(22, "0,1", nil), rebuildAttr(22, "0", "x")},
			read: map[string]any{"order": map[string]any{"items": []any{map[string]any{
				"lots": []any{map[string]any{}, map[string]any{"tags": []any{}}}, "lots.tags": "x",
			}}}},
			want: "attrID=22 arrayIndices=0,1: stored empty-list marker of list attribute 'order.items.lots.tags'",
		},
	} {
		t.Run(name, func(t *testing.T) {
			rowID := uuid.Must(uuid.NewV7())
			stored := storedOrderRow(rowID, tc.attrs...)
			read, err := tr.FromPersistentRecord(ctx, stored)
			require.NoError(t, err)
			require.Equal(t, tc.read, read)

			_, err = tr.MergeUpdate(ctx, stored, setting(map[string]any{"tags": []any{"unrelated"}}))
			require.EqualError(t, err, "record schema=619 row="+rowID.String()+" "+tc.want+
				" rebuilds under a different key spelling than the attribute's other stored records, "+
				"and a write keeps one spelling per attribute, so this update cannot write it back; "+
				"an update must replace the attribute or a container holding it")
			require.NotErrorIs(t, err, forma.ErrInvalidInput)
		})
	}
}

// A row read by array ancestry places a record with fewer indices than its
// attribute has array ancestors under the literal key beside the array. A
// caller's fragment that rewrites the array spells the same attribute nested
// in it, and the caller's spelling wins: the update writes the caller's
// value, and discards the stored one rather than keeping it over the
// caller's, as a literal key the caller sent itself would be kept.
func TestMergeUpdateCallersNestedSpellingReplacesLiteralPlacedStoredValue(t *testing.T) {
	ctx := context.Background()
	tr := NewPersistentRecordTransformer(newAncestryRegistry())
	for name, tc := range map[string]struct {
		attrs []model.EntityAttribute
		items []any
		want  []string
	}{
		"value": {
			attrs: []model.EntityAttribute{rebuildAttr(9, "", "a"), rebuildAttr(8, "0", int64(1))},
			items: []any{map[string]any{"qty": int64(1), "sku": "z"}},
			want:  []string{"8[0]=1", "9[0]=z"},
		},
		"empty-list marker": {
			attrs: []model.EntityAttribute{rebuildAttr(19, "", nil), rebuildAttr(9, "0", "a")},
			items: []any{map[string]any{"sku": "a", "tags": []any{}}},
			want:  []string{"9[0]=a", "19[0]=<none>"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			rowID := uuid.Must(uuid.NewV7())
			stored := storedOrderRow(rowID, tc.attrs...)
			merged, err := tr.MergeUpdate(ctx, stored, func(base map[string]any) (map[string]any, error) {
				base["order"].(map[string]any)["items"] = tc.items
				return base, nil
			})
			require.NoError(t, err)
			require.Equal(t, map[string]any{"order": map[string]any{"items": tc.items}}, merged)
			written, err := tr.ToPersistentRecord(ctx, 619, rowID, merged)
			require.NoError(t, err)
			require.ElementsMatch(t, tc.want, storedKeys(written.OtherAttributes))
		})
	}
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

// Under array ancestry a marker carries one index per array of objects
// around its list, and the read takes any further index for one of the
// list's own elements, where a row with no value is no element. An [] nested
// in more arrays than that, inside the list's own value or inside an array
// where the schema declares an object, is refused as caller input, so the
// write never stores a record the read leaves out and an unrelated update
// deletes. Without ancestry a marker carries no indices, so the write stores
// the marker of the list the [] is nested in, which the read keeps, as
// before #623.
func TestNestedEmptyListIsRefusedUnderAncestry(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		doc      map[string]any
		refusal  string
		inferred []string
	}{
		"inside a list's value": {
			doc:      map[string]any{"tags": []any{[]any{}}},
			refusal:  `invalid value for attribute 'tags' (attrID=11): empty list at array indices "0" is nested in more arrays than the attribute's arrays of objects (none); a list's elements cannot be arrays`,
			inferred: []string{"11[]=<none>"},
		},
		"inside a list's value inside an array of objects": {
			doc:      map[string]any{"order": map[string]any{"items": []any{map[string]any{"tags": []any{[]any{}}}}}},
			refusal:  `invalid value for attribute 'order.items.tags' (attrID=19): empty list at array indices "0,0" is nested in more arrays than the attribute's arrays of objects (order.items); a list's elements cannot be arrays`,
			inferred: []string{"19[]=<none>"},
		},
		"inside an array where the schema declares an object": {
			doc:      map[string]any{"order": []any{map[string]any{"labels": []any{}}}},
			refusal:  `invalid value for attribute 'order.labels' (attrID=16): empty list at array indices "0" is nested in more arrays than the attribute's arrays of objects (none); a list's elements cannot be arrays`,
			inferred: []string{"16[]=<none>"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewPersistentRecordTransformer(newAncestryRegistry()).ToPersistentRecord(ctx, 619, uuid.Must(uuid.NewV7()), tc.doc)
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			msg, published := forma.ResolvePublicMessage(err)
			require.True(t, published, "must publish, not redact: %v", err)
			require.Equal(t, tc.refusal, msg)

			tr := NewPersistentRecordTransformer(newRegistryWithoutAncestry())
			rowID := uuid.Must(uuid.NewV7())
			stored, err := tr.ToPersistentRecord(ctx, 619, rowID, tc.doc)
			require.NoError(t, err)
			require.ElementsMatch(t, tc.inferred, storedKeys(stored.OtherAttributes))
			read, err := tr.FromPersistentRecord(ctx, stored)
			require.NoError(t, err)
			rewritten, err := tr.ToPersistentRecord(ctx, 619, rowID, read)
			require.NoError(t, err)
			require.ElementsMatch(t, tc.inferred, storedKeys(rewritten.OtherAttributes), "read as %v", read)
		})
	}
}

// A row written while its schema declared array ancestry holds the indexed
// marker of a list inside an array of objects. A registry that no longer
// declares that ancestry, as one does once the schema document reaches an
// attribute through another file's $ref, reads the row by inference, which
// cannot place the marker. The read fails rather than answer without the
// list, and so does every update of the row, whatever it sends, rather than
// delete the marker's record: nothing tells whether the update replaces it.
// The failure follows the row's reading, so a row read by inference because
// one of its attributes alone declares no ancestry fails too.
func TestRowWithMarkerReadWithoutAncestryFails(t *testing.T) {
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
	want := "failed to convert attributes of row " + rowID.String() + " to JSON: " +
		"empty-list marker of list attribute 'order.items.tags' carries array indices 1, " +
		"which only a rebuild by array ancestry places, and the registry does not declare the ancestry of every attribute in the row; " +
		"the registry must declare the schema's array ancestry, " +
		"as the file registry does when the schema document resolves every attribute's ancestors"
	for name, registry := range map[string]forma.SchemaRegistry{
		"no attribute declares it": newRegistryWithoutAncestry(),
		"one attribute does not":   noteUndeclared,
	} {
		t.Run(name, func(t *testing.T) {
			tr := NewPersistentRecordTransformer(registry)
			_, err := tr.FromPersistentRecord(ctx, stored)
			require.EqualError(t, err, want)
			require.NotErrorIs(t, err, forma.ErrInvalidInput)

			for _, update := range []map[string]any{
				{"tags": []any{"unrelated"}},
				{"order": map[string]any{"items": []any{}}},
			} {
				_, err := tr.MergeUpdate(ctx, stored, setting(update))
				require.EqualError(t, err, want, "update %v", update)
			}
		})
	}
}

// Without array ancestry the rebuild cannot tell how many array ancestors a
// list has, but it has no more than its name has proper prefixes. A list's
// row with no value and more indices than that is no marker but a row with
// no value at an element's index, which the read leaves out as it always
// has, and as a reading by ancestry does.
func TestFromAttributesWithoutAncestryLeavesOutValuelessListElement(t *testing.T) {
	for name, registry := range map[string]forma.SchemaRegistry{
		"array ancestry": newAncestryRegistry(),
		"inferred":       newRegistryWithoutAncestry(),
	} {
		t.Run(name, func(t *testing.T) {
			got, err := NewTransformer(registry).FromAttributes(context.Background(), []model.EntityAttribute{
				rebuildAttr(11, "0", "t0"), rebuildAttr(11, "1", nil), rebuildAttr(1, "0,1", nil),
			})
			require.NoError(t, err)
			require.Equal(t, map[string]any{"tags": []any{"t0"}}, got)
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
