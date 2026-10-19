package transform

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// newGapRegistry declares an ordinary array of objects, a scalar beside it,
// and two arrays of objects holding an object: the second's object has a
// required_if_parent_present member. With ancestry, the arrays of objects are
// declared as the file registry derives them (#623); without, the rebuild
// reads the two holding an object per-field.
func newGapRegistry(ancestry bool) forma.SchemaRegistry {
	cache := forma.SchemaAttributeCache{
		"items.sku":    {AttributeName: "items.sku", AttributeID: 1, ValueType: forma.ValueTypeText},
		"items.qty":    {AttributeName: "items.qty", AttributeID: 2, ValueType: forma.ValueTypeText},
		"stage":        {AttributeName: "stage", AttributeID: 3, ValueType: forma.ValueTypeText},
		"pi.pid":       {AttributeName: "pi.pid", AttributeID: 4, ValueType: forma.ValueTypeText},
		"pi.snap.name": {AttributeName: "pi.snap.name", AttributeID: 5, ValueType: forma.ValueTypeText},
		"pi.notes":     {AttributeName: "pi.notes", AttributeID: 6, ValueType: forma.ValueTypeText},
		"qa.pid":       {AttributeName: "qa.pid", AttributeID: 7, ValueType: forma.ValueTypeText},
		"qa.snap.code": {
			AttributeName: "qa.snap.code", AttributeID: 8, ValueType: forma.ValueTypeText,
			RequiredPolicy: forma.RequiredPolicyIfParentPresent,
		},
	}
	if ancestry {
		declareArrayAncestors(cache, "items", "pi", "qa")
	}
	return &stubSchemaRegistry{schemaID: 626, schemaName: "gap", cache: cache}
}

// recordOrders returns every ordering of records.
func recordOrders(records []model.EAVRecord) [][]model.EAVRecord {
	if len(records) <= 1 {
		return [][]model.EAVRecord{append([]model.EAVRecord(nil), records...)}
	}
	var out [][]model.EAVRecord
	for i := range records {
		rest := append(append([]model.EAVRecord(nil), records[:i]...), records[i+1:]...)
		for _, tail := range recordOrders(rest) {
			out = append(out, append([]model.EAVRecord{records[i]}, tail...))
		}
	}
	return out
}

// An element of an array of objects that stores no record reads back as {},
// in every record order, and an update that does not address the array
// writes back exactly the stored records plus its own (#626). Read as null,
// the element made every such update refuse a null element the caller never
// sent. The array's length is not stored, so an empty element after the last
// one that stores a record does not come back.
func TestObjectArrayGapReadsAsEmptyObject(t *testing.T) {
	cases := []struct {
		name   string
		doc    map[string]any
		stored []string
		read   map[string]any
		// inferred is the read without array ancestry, when it differs.
		inferred map[string]any
	}{
		{
			name:   "empty element before a storing one",
			doc:    map[string]any{"items": []any{map[string]any{}, map[string]any{"sku": "a"}}},
			stored: []string{"1[1]=a"},
			read:   map[string]any{"items": []any{map[string]any{}, map[string]any{"sku": "a"}}},
		},
		{
			name: "several gaps around storing elements",
			doc: map[string]any{"items": []any{
				map[string]any{}, map[string]any{"qty": "1"}, map[string]any{}, map[string]any{}, map[string]any{"sku": "a", "qty": "2"},
			}},
			stored: []string{"2[1]=1", "1[4]=a", "2[4]=2"},
			read: map[string]any{"items": []any{
				map[string]any{}, map[string]any{"qty": "1"}, map[string]any{}, map[string]any{}, map[string]any{"sku": "a", "qty": "2"},
			}},
		},
		{
			name:   "trailing empty element is not stored",
			doc:    map[string]any{"items": []any{map[string]any{"sku": "a"}, map[string]any{}}},
			stored: []string{"1[0]=a"},
			read:   map[string]any{"items": []any{map[string]any{"sku": "a"}}},
		},
		{
			// Without ancestry the array of objects reads per-field (#623),
			// and its object member's own array takes the same padding.
			name: "object member missing from an earlier element",
			doc: map[string]any{"pi": []any{
				map[string]any{"pid": "p0"}, map[string]any{"pid": "p1", "snap": map[string]any{"name": "B"}},
			}},
			stored: []string{"4[0]=p0", "4[1]=p1", "5[1]=B"},
			read: map[string]any{"pi": []any{
				map[string]any{"pid": "p0"}, map[string]any{"pid": "p1", "snap": map[string]any{"name": "B"}},
			}},
			inferred: map[string]any{"pi": map[string]any{
				"pid": []any{"p0", "p1"}, "snap": []any{map[string]any{}, map[string]any{"name": "B"}},
			}},
		},
	}
	ctx := context.Background()
	for _, ancestry := range []bool{true, false} {
		tr := NewPersistentRecordTransformer(newGapRegistry(ancestry))
		for _, tc := range cases {
			want := tc.read
			if !ancestry && tc.inferred != nil {
				want = tc.inferred
			}
			t.Run(fmt.Sprintf("%s/ancestry=%t", tc.name, ancestry), func(t *testing.T) {
				rowID := uuid.Must(uuid.NewV7())
				stored, err := tr.ToPersistentRecord(ctx, 626, rowID, tc.doc)
				require.NoError(t, err)
				require.ElementsMatch(t, tc.stored, storedKeys(stored.OtherAttributes))

				for _, order := range recordOrders(stored.OtherAttributes) {
					row := &model.PersistentRecord{SchemaID: 626, RowID: rowID, OtherAttributes: order}
					read, err := tr.FromPersistentRecord(ctx, row)
					require.NoError(t, err, "order %v", storedKeys(order))
					require.Equal(t, want, read, "order %v", storedKeys(order))

					merged, err := tr.MergeUpdate(ctx, row, setting(map[string]any{"stage": "x"}))
					require.NoError(t, err, "order %v", storedKeys(order))
					written, err := tr.ToPersistentRecord(ctx, 626, rowID, merged)
					require.NoError(t, err, "order %v merged as %v", storedKeys(order), merged)
					require.ElementsMatch(t, append(append([]string(nil), tc.stored...), "3[]=x"),
						storedKeys(written.OtherAttributes), "order %v merged as %v", storedKeys(order), merged)
				}
			})
		}
	}
}

// A member some elements of an array of objects lack is absent from them
// when the schema declares array ancestry, so an update that keeps the array
// writes it back as stored (#623). Without ancestry the array reads
// per-field and two such gaps still refuse that update, as caller input,
// though the write that stored the row accepted it. A scalar member some
// element lacks reads null in its own array, which is primitive and keeps nil
// padding. An object member's gap reads {}, which asserts the object present
// at the per-field level, so its required_if_parent_present member is
// missing there.
func TestMemberGapsInObjectArrayOnUpdate(t *testing.T) {
	cases := []struct {
		name     string
		doc      map[string]any
		inferred string
	}{
		{
			name: "scalar member",
			doc: map[string]any{"pi": []any{
				map[string]any{"pid": "p0", "snap": map[string]any{"name": "A"}}, map[string]any{"pid": "p1", "notes": "n"},
			}},
			inferred: "attribute 'pi.notes' cannot be set to null (array index 0)",
		},
		{
			name: "object member with a required member",
			doc: map[string]any{"qa": []any{
				map[string]any{"pid": "p0"}, map[string]any{"pid": "p1", "snap": map[string]any{"code": "c"}},
			}},
			inferred: "missing required attribute 'qa.snap.code'",
		},
	}
	ctx := context.Background()
	for _, ancestry := range []bool{true, false} {
		tr := NewPersistentRecordTransformer(newGapRegistry(ancestry))
		for _, tc := range cases {
			t.Run(fmt.Sprintf("%s/ancestry=%t", tc.name, ancestry), func(t *testing.T) {
				rowID := uuid.Must(uuid.NewV7())
				stored, err := tr.ToPersistentRecord(ctx, 626, rowID, tc.doc)
				require.NoError(t, err)

				merged, err := tr.MergeUpdate(ctx, stored, setting(map[string]any{"stage": "x"}))
				require.NoError(t, err)
				written, err := tr.ToPersistentRecord(ctx, 626, rowID, merged)
				if !ancestry {
					require.ErrorIs(t, err, forma.ErrInvalidInput)
					require.ErrorContains(t, err, tc.inferred, "merged as %v", merged)
					return
				}
				require.NoError(t, err, "merged as %v", merged)
				require.ElementsMatch(t, append(storedKeys(stored.OtherAttributes), "3[]=x"), storedKeys(written.OtherAttributes))
				read, err := tr.FromPersistentRecord(ctx, written)
				require.NoError(t, err)
				want := map[string]any{"stage": "x"}
				for key, value := range tc.doc {
					want[key] = value
				}
				require.Equal(t, want, read)
			})
		}
	}
}
