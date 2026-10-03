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

// newRebuildRegistry declares nested lists on both sides of a scalar
// sibling's attribute id, a top-level list, a legacy text-typed nested array
// (pre-#204 attributes file) whose sibling sorts above it, and an order
// holding one of each kind of member: a scalar, a list, a legacy text-typed
// array, an array of objects, and the object and the array of objects nested
// inside that array, which the rebuild cannot shape (#619, #623).
func newRebuildRegistry() forma.SchemaRegistry {
	return &stubSchemaRegistry{schemaID: 619, schemaName: "rebuild", cache: forma.SchemaAttributeCache{
		"contact.phones":  {AttributeName: "contact.phones", AttributeID: 1, ValueType: forma.ValueTypeList},
		"contact.wechat":  {AttributeName: "contact.wechat", AttributeID: 2, ValueType: forma.ValueTypeText},
		"profile.nick":    {AttributeName: "profile.nick", AttributeID: 3, ValueType: forma.ValueTypeText},
		"profile.emails":  {AttributeName: "profile.emails", AttributeID: 4, ValueType: forma.ValueTypeList},
		"box.codes":       {AttributeName: "box.codes", AttributeID: 5, ValueType: forma.ValueTypeList},
		"notes.lines":     {AttributeName: "notes.lines", AttributeID: 6, ValueType: forma.ValueTypeList},
		"notes.title":     {AttributeName: "notes.title", AttributeID: 7, ValueType: forma.ValueTypeText},
		"order.items.qty": {AttributeName: "order.items.qty", AttributeID: 8, ValueType: forma.ValueTypeBigInt},
		"order.items.sku": {AttributeName: "order.items.sku", AttributeID: 9, ValueType: forma.ValueTypeText},
		"order.note":      {AttributeName: "order.note", AttributeID: 10, ValueType: forma.ValueTypeText},
		"tags":            {AttributeName: "tags", AttributeID: 11, ValueType: forma.ValueTypeList},
		"legacy.phones":   {AttributeName: "legacy.phones", AttributeID: 12, ValueType: forma.ValueTypeText},
		"legacy.name":     {AttributeName: "legacy.name", AttributeID: 13, ValueType: forma.ValueTypeText},
		"aliases":         {AttributeName: "aliases", AttributeID: 14, ValueType: forma.ValueTypeList},
		"order.tags":      {AttributeName: "order.tags", AttributeID: 15, ValueType: forma.ValueTypeText},
		"order.labels":    {AttributeName: "order.labels", AttributeID: 16, ValueType: forma.ValueTypeList},
		"order.items.dims.w": {
			AttributeName: "order.items.dims.w", AttributeID: 17, ValueType: forma.ValueTypeText,
		},
		"order.items.lots.code": {
			AttributeName: "order.items.lots.code", AttributeID: 18, ValueType: forma.ValueTypeText,
		},
	}}
}

func rebuildAttr(attrID int16, indices string, value any) model.EntityAttribute {
	return model.EntityAttribute{SchemaID: 619, AttrID: attrID, ArrayIndices: indices, Value: value}
}

// permutations returns every ordering of attrs.
func permutations(attrs []model.EntityAttribute) [][]model.EntityAttribute {
	if len(attrs) <= 1 {
		return [][]model.EntityAttribute{append([]model.EntityAttribute(nil), attrs...)}
	}
	var out [][]model.EntityAttribute
	for i := range attrs {
		rest := make([]model.EntityAttribute, 0, len(attrs)-1)
		rest = append(rest, attrs[:i]...)
		rest = append(rest, attrs[i+1:]...)
		for _, tail := range permutations(rest) {
			out = append(out, append([]model.EntityAttribute{attrs[i]}, tail...))
		}
	}
	return out
}

// FromAttributes rebuilds one document from a row's records whatever order
// they arrive in (#619): the shape of each record is decided by the schema
// and the row, never by the records placed before it.
func TestFromAttributesIsRecordOrderIndependent(t *testing.T) {
	cases := []struct {
		name  string
		attrs []model.EntityAttribute
		want  map[string]any
	}{
		{
			name:  "nested list whose id sorts below its sibling",
			attrs: []model.EntityAttribute{rebuildAttr(1, "0", "p0"), rebuildAttr(1, "1", "p1"), rebuildAttr(2, "", "w")},
			want:  map[string]any{"contact": map[string]any{"phones": []any{"p0", "p1"}, "wechat": "w"}},
		},
		{
			name:  "nested list whose id sorts above its sibling",
			attrs: []model.EntityAttribute{rebuildAttr(3, "", "k"), rebuildAttr(4, "0", "e0"), rebuildAttr(4, "1", "e1")},
			want:  map[string]any{"profile": map[string]any{"emails": []any{"e0", "e1"}, "nick": "k"}},
		},
		{
			name:  "nested list alone",
			attrs: []model.EntityAttribute{rebuildAttr(5, "0", "c0"), rebuildAttr(5, "1", "c1")},
			want:  map[string]any{"box": map[string]any{"codes": []any{"c0", "c1"}}},
		},
		{
			name:  "nested empty-list marker beside a sibling",
			attrs: []model.EntityAttribute{rebuildAttr(6, "", nil), rebuildAttr(7, "", "t")},
			want:  map[string]any{"notes": map[string]any{"lines": []any{}, "title": "t"}},
		},
		{
			name:  "nested stale empty-list marker beside its elements and a sibling",
			attrs: []model.EntityAttribute{rebuildAttr(6, "", nil), rebuildAttr(6, "0", "l0"), rebuildAttr(7, "", "t")},
			want:  map[string]any{"notes": map[string]any{"lines": []any{"l0"}, "title": "t"}},
		},
		{
			name: "array of objects beside an object sibling",
			attrs: []model.EntityAttribute{
				rebuildAttr(8, "0", int64(1)), rebuildAttr(9, "0", "a"),
				rebuildAttr(8, "1", int64(2)), rebuildAttr(9, "1", "b"), rebuildAttr(10, "", "n"),
			},
			want: map[string]any{"order": map[string]any{
				"items": []any{map[string]any{"qty": int64(1), "sku": "a"}, map[string]any{"qty": int64(2), "sku": "b"}},
				"note":  "n",
			}},
		},
		{
			name:  "top-level list",
			attrs: []model.EntityAttribute{rebuildAttr(11, "0", "t0"), rebuildAttr(11, "1", "t1")},
			want:  map[string]any{"tags": []any{"t0", "t1"}},
		},
		{
			name:  "legacy text-typed nested array beside a sibling",
			attrs: []model.EntityAttribute{rebuildAttr(12, "0", "111"), rebuildAttr(12, "1", "222"), rebuildAttr(13, "", "L")},
			want:  map[string]any{"legacy": map[string]any{"name": "L", "phones": []any{"111", "222"}}},
		},
		{
			name:  "legacy text-typed nested array beside only an array of objects",
			attrs: []model.EntityAttribute{rebuildAttr(8, "0", int64(1)), rebuildAttr(15, "0", "x"), rebuildAttr(15, "1", "y")},
			want: map[string]any{"order": map[string]any{
				"items": []any{map[string]any{"qty": int64(1)}}, "tags": []any{"x", "y"},
			}},
		},
		{
			name:  "nested list beside only an array of objects",
			attrs: []model.EntityAttribute{rebuildAttr(8, "0", int64(1)), rebuildAttr(16, "0", "x"), rebuildAttr(16, "1", "y")},
			want: map[string]any{"order": map[string]any{
				"items": []any{map[string]any{"qty": int64(1)}}, "labels": []any{"x", "y"},
			}},
		},
		{
			name:  "legacy text-typed nested array alone reads as an array of objects",
			attrs: []model.EntityAttribute{rebuildAttr(12, "0", "111"), rebuildAttr(12, "1", "222")},
			want:  map[string]any{"legacy": []any{map[string]any{"phones": "111"}, map[string]any{"phones": "222"}}},
		},
		{
			name:  "legacy scalar row under list metadata alone (#372)",
			attrs: []model.EntityAttribute{rebuildAttr(14, "", "old")},
			want:  map[string]any{"aliases": "old"},
		},
		{
			name:  "legacy scalar row under list metadata beside its own elements (#372)",
			attrs: []model.EntityAttribute{rebuildAttr(14, "", "old"), rebuildAttr(14, "0", "a0"), rebuildAttr(14, "1", "a1")},
			want:  map[string]any{"aliases": []any{"a0", "a1"}},
		},
	}

	tr := NewTransformer(newRebuildRegistry())
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, order := range permutations(tc.attrs) {
				got, err := tr.FromAttributes(context.Background(), order)
				require.NoError(t, err)
				require.Equal(t, tc.want, got, "record order %+v", order)
			}
		})
	}
}

// orderRows returns every combination of one record of each kind an order can
// hold, so no pairing of siblings is left to a hand-picked case. The members
// nested inside the array of objects are the ones the rebuild cannot shape
// (#623).
func orderRows() [][]model.EntityAttribute {
	pool := []model.EntityAttribute{
		rebuildAttr(10, "", "n"),      // order.note, a scalar
		rebuildAttr(16, "0", "l"),     // order.labels, a list
		rebuildAttr(15, "0", "t"),     // order.tags, a legacy text-typed array
		rebuildAttr(8, "0", int64(1)), // order.items.qty, an array-of-objects member
		rebuildAttr(9, "0", "a"),      // order.items.sku, its sibling
		rebuildAttr(17, "0", "w"),     // order.items.dims.w, an object inside the array
		rebuildAttr(18, "0,0", "c"),   // order.items.lots.code, an array inside the array
	}
	rows := make([][]model.EntityAttribute, 0, 1<<len(pool)-1)
	for mask := 1; mask < 1<<len(pool); mask++ {
		var row []model.EntityAttribute
		for i, attr := range pool {
			if mask&(1<<i) != 0 {
				row = append(row, attr)
			}
		}
		rows = append(rows, row)
	}
	return rows
}

// Whatever a row holds, its records read every path they share the same way:
// the rebuild is one document in every order, and that document handed
// straight back to the write either is refused or stores exactly the row's
// records (#619). Two records that read one path differently, an object for
// one and an array of objects for the other, break both: the later placement
// replaces the earlier, and the document omits a record.
//
// This is a property of the rebuild alone. An update does not hand the
// document straight back: it writes the merge base through
// resolveStoredValues, which is where a document the write refuses must not
// become a deleted record. TestMergeUpdateWritesBackEveryStoredRecordOrRefuses
// holds the same rows to that. The shape of the members nested inside the
// array of objects (#623) is asserted by neither.
func TestFromAttributesReadsSharedPathsOneWay(t *testing.T) {
	tr := NewTransformer(newRebuildRegistry())
	ctx := context.Background()
	recordKeys := func(attrs []model.EntityAttribute) []string {
		keys := make([]string, 0, len(attrs))
		for _, attr := range attrs {
			keys = append(keys, fmt.Sprintf("%d[%s]=%v", attr.AttrID, attr.ArrayIndices, attr.Value))
		}
		return keys
	}

	for _, row := range orderRows() {
		doc, err := tr.FromAttributes(ctx, row)
		require.NoError(t, err)
		for _, order := range permutations(row) {
			got, err := tr.FromAttributes(ctx, order)
			require.NoError(t, err)
			require.Equal(t, doc, got, "record order %+v", order)
		}

		rewritten, err := tr.ToAttributes(ctx, 619, uuid.Must(uuid.NewV7()), doc)
		if err != nil {
			require.ErrorIs(t, err, forma.ErrInvalidInput, "row %+v", row)
			continue
		}
		require.ElementsMatch(t, recordKeys(row), recordKeys(rewritten), "row %+v rebuilt as %v", row, doc)
	}
}

// An update's merge base is rebuilt like a read, so a nested list whose id
// sorts below its sibling's must survive an update that does not touch it;
// otherwise the written row omits the list and replaceEAVAttributes deletes
// it (#619).
func TestMergeUpdateKeepsNestedListBesideSibling(t *testing.T) {
	rowID := uuid.Must(uuid.NewV7())
	p0, p1, w := "p0", "p1", "w"
	record := &model.PersistentRecord{SchemaID: 619, RowID: rowID, OtherAttributes: []model.EAVRecord{
		{SchemaID: 619, RowID: rowID, AttrID: 1, ArrayIndices: "0", ValueText: &p0},
		{SchemaID: 619, RowID: rowID, AttrID: 1, ArrayIndices: "1", ValueText: &p1},
		{SchemaID: 619, RowID: rowID, AttrID: 2, ValueText: &w},
	}}

	got, err := NewPersistentRecordTransformer(newRebuildRegistry()).
		MergeUpdate(context.Background(), record, setting(map[string]any{"tags": []any{"t0"}}))
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"contact": map[string]any{"phones": []any{"p0", "p1"}, "wechat": "w"},
		"tags":    []any{"t0"},
	}, got)
}

// The create and update responses echo the written record, whose EAV
// records follow the write walk's sorted keys: "phones" before "wechat"
// (#619).
func TestPersistentRecordEchoKeepsNestedListBesideSibling(t *testing.T) {
	doc := map[string]any{"contact": map[string]any{"phones": []any{"p0", "p1"}, "wechat": "w"}}
	tr := NewPersistentRecordTransformer(newRebuildRegistry())

	record, err := tr.ToPersistentRecord(context.Background(), 619, uuid.Must(uuid.NewV7()), doc)
	require.NoError(t, err)
	got, err := tr.FromPersistentRecord(context.Background(), record)
	require.NoError(t, err)
	require.Equal(t, doc, got)
}
