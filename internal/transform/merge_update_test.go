package transform

import (
	"context"
	"math"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// newMergeUpdateRegistry declares a required bigint at each float64 image
// destination (#590), eav_data scalar, eav_data list item and double_*
// column, beside a nested object and an array of objects.
func newMergeUpdateRegistry() forma.SchemaRegistry {
	return &stubSchemaRegistry{schemaID: 501, schemaName: "merge_update", cache: forma.SchemaAttributeCache{
		"name":   {AttributeName: "name", AttributeID: 1, ValueType: forma.ValueTypeText},
		"total":  {AttributeName: "total", AttributeID: 2, ValueType: forma.ValueTypeBigInt, Required: true},
		"totals": {AttributeName: "totals", AttributeID: 3, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeBigInt, Required: true},
		"ratio": {AttributeName: "ratio", AttributeID: 4, ValueType: forma.ValueTypeBigInt, Required: true,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnDouble01}},
		"contact.total":    {AttributeName: "contact.total", AttributeID: 5, ValueType: forma.ValueTypeBigInt},
		"contact.name":     {AttributeName: "contact.name", AttributeID: 6, ValueType: forma.ValueTypeText},
		"order.items.name": {AttributeName: "order.items.name", AttributeID: 7, ValueType: forma.ValueTypeText},
		"order.items.qty":  {AttributeName: "order.items.qty", AttributeID: 8, ValueType: forma.ValueTypeBigInt},
	}}
}

var (
	fractionImage  = 1000.5
	pastInt64Image = math.Ldexp(1, 63)
)

// legacyBigintRecord stores an image the read refuses at every required
// destination: a fraction, a whole number past int64, and both in a list.
func legacyBigintRecord(rowID uuid.UUID) *model.PersistentRecord {
	name, whole := "kept", 7.0
	return &model.PersistentRecord{
		SchemaID:     501,
		RowID:        rowID,
		Float64Items: map[string]float64{string(forma.MainColumnDouble01): pastInt64Image},
		OtherAttributes: []model.EAVRecord{
			{SchemaID: 501, RowID: rowID, AttrID: 1, ValueText: &name},
			{SchemaID: 501, RowID: rowID, AttrID: 2, ValueNumeric: &fractionImage},
			{SchemaID: 501, RowID: rowID, AttrID: 3, ArrayIndices: "0", ValueNumeric: &whole},
			{SchemaID: 501, RowID: rowID, AttrID: 3, ArrayIndices: "1", ValueNumeric: &pastInt64Image},
		},
	}
}

// healthyRequiredRecord stores readable required values and then extra.
func healthyRequiredRecord(rowID uuid.UUID, extra ...model.EAVRecord) *model.PersistentRecord {
	one := 1.0
	record := &model.PersistentRecord{
		SchemaID:     501,
		RowID:        rowID,
		Float64Items: map[string]float64{string(forma.MainColumnDouble01): one},
		OtherAttributes: []model.EAVRecord{
			{SchemaID: 501, RowID: rowID, AttrID: 2, ValueNumeric: &one},
			{SchemaID: 501, RowID: rowID, AttrID: 3, ArrayIndices: "0", ValueNumeric: &one},
		},
	}
	record.OtherAttributes = append(record.OtherAttributes, extra...)
	return record
}

func textRecord(rowID uuid.UUID, attrID int16, indices, text string) model.EAVRecord {
	return model.EAVRecord{SchemaID: 501, RowID: rowID, AttrID: attrID, ArrayIndices: indices, ValueText: &text}
}

func numericRecord(rowID uuid.UUID, attrID int16, indices string, image float64) model.EAVRecord {
	return model.EAVRecord{SchemaID: 501, RowID: rowID, AttrID: attrID, ArrayIndices: indices, ValueNumeric: &image}
}

func setting(values map[string]any) func(map[string]any) map[string]any {
	return func(base map[string]any) map[string]any {
		for key, value := range values {
			base[key] = value
		}
		return base
	}
}

func keepAll(base map[string]any) map[string]any { return base }

// The update that rewrites a stored image the read refuses must not be
// blocked by decoding it (#590 review): a stored value the merge replaces is
// discarded undecoded, and the required policy still holds because the
// written row carries the replacement.
func TestMergeUpdateNeverDecodesDiscardedValues(t *testing.T) {
	tr := NewPersistentRecordTransformer(newMergeUpdateRegistry())

	got, err := tr.MergeUpdate(context.Background(), legacyBigintRecord(uuid.Must(uuid.NewV7())), setting(map[string]any{
		"total": int64(1), "totals": []any{int64(2)}, "ratio": int64(3),
	}))
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"name": "kept", "total": int64(1), "totals": []any{int64(2)}, "ratio": int64(3),
	}, got)
}

// Every stored value the written row keeps is decoded as the read decodes it,
// so a refused image the update does not rewrite still fails the merge with
// the read path's own plain error, not a 4xx.
func TestMergeUpdateDecodesKeptValues(t *testing.T) {
	rowID := uuid.Must(uuid.NewV7())
	tr := NewPersistentRecordTransformer(newMergeUpdateRegistry())
	replacements := map[string]any{"total": int64(1), "totals": []any{int64(2)}, "ratio": int64(3)}

	for kept, want := range map[string]string{
		"total":  "stored bigint image 1000.5 is not a whole number",
		"totals": "stored bigint image 9223372036854775808 is outside the bigint range",
		"ratio":  "stored bigint image 9223372036854775808 is outside the bigint range",
	} {
		t.Run(kept, func(t *testing.T) {
			others := make(map[string]any)
			for name, value := range replacements {
				if name != kept {
					others[name] = value
				}
			}
			_, err := tr.MergeUpdate(context.Background(), legacyBigintRecord(rowID), setting(others))
			require.ErrorContains(t, err, want)
			require.ErrorContains(t, err, "'"+kept+"'")
			require.ErrorContains(t, err, rowID.String())
			require.NotErrorIs(t, err, forma.ErrInvalidInput)
		})
	}
}

// A merge that keeps every stored value fails exactly as reading the row
// does, and of several unreadable kept values the same one fails on every
// run: they are decoded in walk order, not in map order.
func TestMergeUpdateFailsLikeTheRead(t *testing.T) {
	rowID := uuid.Must(uuid.NewV7())
	tr := NewPersistentRecordTransformer(newMergeUpdateRegistry())
	_, readErr := tr.FromPersistentRecord(context.Background(), legacyBigintRecord(rowID))
	require.Error(t, readErr)

	for range 20 {
		_, err := tr.MergeUpdate(context.Background(), legacyBigintRecord(rowID), keepAll)
		require.Error(t, err)
		require.True(t, strings.HasSuffix(readErr.Error(), err.Error()),
			"merge error %q is not the read's %q", err, readErr)
	}
}

// #312 on the merge base: a caller's literal "contact.total" is the spelling
// the write keeps, so the stored contact.total the base nests beside it is
// discarded undecoded. Its sibling is kept, and a container left holding
// nothing is removed with the value.
func TestMergeUpdateLiteralKeyDiscardsNestedStoredValue(t *testing.T) {
	rowID := uuid.Must(uuid.NewV7())
	tr := NewPersistentRecordTransformer(newMergeUpdateRegistry())
	repair := setting(map[string]any{"contact.total": int64(42)})

	got, err := tr.MergeUpdate(context.Background(), healthyRequiredRecord(rowID,
		numericRecord(rowID, 5, "", pastInt64Image), textRecord(rowID, 6, "", "kept")), repair)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"name": "kept"}, got["contact"])
	require.Equal(t, int64(42), got["contact.total"])

	got, err = tr.MergeUpdate(context.Background(), healthyRequiredRecord(rowID,
		numericRecord(rowID, 5, "", pastInt64Image)), repair)
	require.NoError(t, err)
	require.NotContains(t, got, "contact", "a container emptied by the discard is removed")
	require.Equal(t, int64(42), got["contact.total"])

	_, err = tr.MergeUpdate(context.Background(), healthyRequiredRecord(rowID,
		numericRecord(rowID, 5, "", pastInt64Image)), setting(map[string]any{"contact.name": "x"}))
	require.ErrorContains(t, err, "'contact.total'", "a sibling update keeps, and so decodes, the stored value")
	require.NotErrorIs(t, err, forma.ErrInvalidInput)
}

// Discarding an element's stored values keeps the other elements' indices:
// an emptied element before a kept one stays as {}, and emptied trailing
// elements are cut.
func TestMergeUpdateKeepsArrayPositions(t *testing.T) {
	rowID := uuid.Must(uuid.NewV7())
	tr := NewPersistentRecordTransformer(newMergeUpdateRegistry())

	got, err := tr.MergeUpdate(context.Background(), healthyRequiredRecord(rowID,
		numericRecord(rowID, 8, "0", pastInt64Image), textRecord(rowID, 7, "1", "n1")),
		setting(map[string]any{"order.items": []any{map[string]any{"qty": int64(5)}}}))
	require.NoError(t, err)
	require.Equal(t, map[string]any{"items": []any{map[string]any{}, map[string]any{"name": "n1"}}}, got["order"])

	got, err = tr.MergeUpdate(context.Background(), healthyRequiredRecord(rowID,
		textRecord(rowID, 7, "0", "n0"), numericRecord(rowID, 8, "1", pastInt64Image)),
		setting(map[string]any{"order.items": []any{map[string]any{}, map[string]any{"qty": int64(5)}}}))
	require.NoError(t, err)
	require.Equal(t, map[string]any{"items": []any{map[string]any{"name": "n0"}}}, got["order"])
}

// Presence of the stored row comes from storage, not from the update: a row
// that never stored a required attribute is a required-policy drift the
// read refuses, plain, whatever the update writes.
func TestMergeUpdateKeepsRequiredPolicyOnStoredRow(t *testing.T) {
	rowID := uuid.Must(uuid.NewV7())
	record := legacyBigintRecord(rowID)
	record.OtherAttributes = record.OtherAttributes[:2] // no totals stored
	tr := NewPersistentRecordTransformer(newMergeUpdateRegistry())

	_, err := tr.MergeUpdate(context.Background(), record, setting(map[string]any{
		"total": int64(1), "totals": []any{int64(2)}, "ratio": int64(3),
	}))
	require.ErrorContains(t, err, "missing required attribute 'totals'")
	require.NotErrorIs(t, err, forma.ErrInvalidInput)
}

// The walk that decides which stored values are kept stops at the nesting
// cap, so a cyclic merge result is refused instead of walked forever (#406).
func TestMergeUpdateRefusesCyclicMergeResult(t *testing.T) {
	tr := NewPersistentRecordTransformer(newMergeUpdateRegistry())

	_, err := tr.MergeUpdate(context.Background(), legacyBigintRecord(uuid.Must(uuid.NewV7())),
		func(base map[string]any) map[string]any {
			cycle := map[string]any{}
			cycle["self"] = []any{cycle, cycle}
			base["contact"] = cycle
			return base
		})
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.ErrorContains(t, err, `payload nesting exceeds 1000 levels beneath attribute "contact"`)
}
