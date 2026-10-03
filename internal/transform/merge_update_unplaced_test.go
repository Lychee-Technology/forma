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

// storedOrderRow stores attrs as one row's eav_data records of the rebuild
// schema: text in value_text, a bigint as its value_numeric image.
func storedOrderRow(rowID uuid.UUID, attrs ...model.EntityAttribute) *model.PersistentRecord {
	record := &model.PersistentRecord{SchemaID: 619, RowID: rowID}
	for _, attr := range attrs {
		stored := model.EAVRecord{SchemaID: 619, RowID: rowID, AttrID: attr.AttrID, ArrayIndices: attr.ArrayIndices}
		switch v := attr.Value.(type) {
		case string:
			stored.ValueText = &v
		case int64:
			image := float64(v)
			stored.ValueNumeric = &image
		}
		record.OtherAttributes = append(record.OtherAttributes, stored)
	}
	return record
}

// storedKeys names each record by attribute, array indices and value.
func storedKeys(records []model.EAVRecord) []string {
	keys := make([]string, 0, len(records))
	for _, record := range records {
		value := "<none>"
		switch {
		case record.ValueText != nil:
			value = *record.ValueText
		case record.ValueNumeric != nil:
			value = fmt.Sprint(*record.ValueNumeric)
		}
		keys = append(keys, fmt.Sprintf("%d[%s]=%s", record.AttrID, record.ArrayIndices, value))
	}
	return keys
}

// lotsDocument is a document whose array of objects holds an array of
// objects: the writer stores its one value as order.items.lots.code at
// indices "0,0", which the rebuild cannot shape (#623).
func lotsDocument(code string) map[string]any {
	return map[string]any{"order": map[string]any{
		"items": []any{map[string]any{"lots": []any{map[string]any{"code": code}}}},
	}}
}

// An update must not delete a record it never addressed: the row it writes
// holds every record the row stored, or the update is refused (#619 review).
// The stored row reaches the write through resolveStoredValues, so this runs
// the update's own pipeline, MergeUpdate then ToPersistentRecord, over every
// row TestFromAttributesReadsSharedPathsOneWay rebuilds.
//
// Only the array nested inside the array of objects rebuilds where the write
// cannot read it back, so exactly the rows holding it are refused, as stored
// state and not as caller input, and every other row is written back whole.
func TestMergeUpdateWritesBackEveryStoredRecordOrRefuses(t *testing.T) {
	tr := NewPersistentRecordTransformer(newRebuildRegistry())
	ctx := context.Background()
	holdsLots := func(row []model.EntityAttribute) bool {
		for _, attr := range row {
			if attr.AttrID == 18 {
				return true
			}
		}
		return false
	}

	for _, row := range orderRows() {
		rowID := uuid.Must(uuid.NewV7())
		stored := storedOrderRow(rowID, row...)
		merged, err := tr.MergeUpdate(ctx, stored, setting(map[string]any{"tags": []any{"unrelated"}}))
		if holdsLots(row) {
			require.ErrorContains(t, err,
				"stored text value of attribute 'order.items.lots.code' rebuilds at 'order.items.lots.code.code'", "row %+v", row)
			require.NotErrorIs(t, err, forma.ErrInvalidInput, "row %+v", row)
			continue
		}
		require.NoError(t, err, "row %+v", row)

		written, err := tr.ToPersistentRecord(ctx, 619, rowID, merged)
		require.NoError(t, err, "row %+v merged as %v", row, merged)
		require.ElementsMatch(t, append(storedKeys(stored.OtherAttributes), "11[0]=unrelated"),
			storedKeys(written.OtherAttributes), "row %+v merged as %v", row, merged)
	}
}

// The row the writer itself stores for an array nested inside an array of
// objects rebuilds beneath the attribute's own name (#623). Removing the
// stored value there let an unrelated update succeed and delete the record
// (#619 review); the update is refused instead, naming the record, the
// attribute and where it was rebuilt, and blaming the stored row, not the
// caller.
func TestMergeUpdateRefusesToDropUnplacedStoredValue(t *testing.T) {
	tr := NewPersistentRecordTransformer(newRebuildRegistry())
	ctx := context.Background()
	rowID := uuid.Must(uuid.NewV7())
	stored, err := tr.ToPersistentRecord(ctx, 619, rowID, lotsDocument("c"))
	require.NoError(t, err)
	require.Equal(t, []string{"18[0,0]=c"}, storedKeys(stored.OtherAttributes))

	for name, merge := range map[string]func(map[string]any) map[string]any{
		"unrelated attribute":  setting(map[string]any{"tags": []any{"unrelated"}}),
		"no change":            keepAll,
		"sibling of the array": setting(map[string]any{"order.note": "n"}),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := tr.MergeUpdate(ctx, stored, merge)
			require.EqualError(t, err, fmt.Sprintf("record schema=619 row=%s attrID=18 arrayIndices=0,0: "+
				"stored text value of attribute 'order.items.lots.code' rebuilds at 'order.items.lots.code.code', "+
				"which the schema does not define, so this update cannot write it back; "+
				"an update must replace the attribute or a container holding it", rowID))
			require.NotErrorIs(t, err, forma.ErrInvalidInput)
		})
	}
}

// A legacy text-typed array (pre-#204 metadata) inside an array of objects is
// stored the same way, at two array indices under an attribute that is not a
// list, and an update that keeps it is refused the same way.
func TestMergeUpdateRefusesToDropLegacyArrayInsideObjectArray(t *testing.T) {
	tr := NewPersistentRecordTransformer(newRebuildRegistry())
	ctx := context.Background()
	stored, err := tr.ToPersistentRecord(ctx, 619, uuid.Must(uuid.NewV7()), map[string]any{"order": map[string]any{
		"items": []any{map[string]any{"sku": []any{"a", "b"}}},
	}})
	require.NoError(t, err)
	require.Equal(t, []string{"9[0,0]=a", "9[0,1]=b"}, storedKeys(stored.OtherAttributes))

	_, err = tr.MergeUpdate(ctx, stored, setting(map[string]any{"tags": []any{"unrelated"}}))
	require.ErrorContains(t, err,
		"attrID=9 arrayIndices=0,0: stored text value of attribute 'order.items.sku' rebuilds at 'order.items.sku.sku'")
	require.NotErrorIs(t, err, forma.ErrInvalidInput)
}

// The refusal blocks only the updates that keep the stored value. One that
// replaces it writes the replacement, whether it replaces a container holding
// the value or spells the attribute as a literal key, which wins over the
// stored spelling as it does for a value the write can place (#312).
func TestMergeUpdateReplacesUnplacedStoredValue(t *testing.T) {
	cases := []struct {
		name   string
		update map[string]any
		want   []string
	}{
		{name: "container holding the value", update: lotsDocument("d"), want: []string{"18[0,0]=d"}},
		{name: "literal spelling", update: map[string]any{"order.items.lots.code": "d"}, want: []string{"18[]=d"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := NewPersistentRecordTransformer(newRebuildRegistry())
			ctx := context.Background()
			rowID := uuid.Must(uuid.NewV7())
			stored, err := tr.ToPersistentRecord(ctx, 619, rowID, lotsDocument("c"))
			require.NoError(t, err)

			merged, err := tr.MergeUpdate(ctx, stored, setting(tc.update))
			require.NoError(t, err)
			require.Equal(t, tc.update, merged)
			written, err := tr.ToPersistentRecord(ctx, 619, rowID, merged)
			require.NoError(t, err)
			require.Equal(t, tc.want, storedKeys(written.OtherAttributes))
		})
	}
}

// A stored record of the same attribute that the write can place is no
// replacement: writing it back claims the attribute under the stored spelling,
// and the record beside it that the write cannot place would still be deleted.
func TestMergeUpdateRefusesUnplacedValueBesidePlacedOne(t *testing.T) {
	rowID := uuid.Must(uuid.NewV7())
	stored := storedOrderRow(rowID, rebuildAttr(18, "0", "x"), rebuildAttr(18, "1,0", "c"))

	_, err := NewPersistentRecordTransformer(newRebuildRegistry()).
		MergeUpdate(context.Background(), stored, setting(map[string]any{"tags": []any{"unrelated"}}))
	require.ErrorContains(t, err, "attrID=18 arrayIndices=1,0: stored text value of attribute 'order.items.lots.code'")
	require.NotErrorIs(t, err, forma.ErrInvalidInput)
}
