package transform

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// newRebuildRegistry declares nested lists on both sides of a scalar
// sibling's attribute id, an array of objects beside an object sibling, a
// top-level list, and a legacy text-typed nested array (pre-#204 attributes
// file) whose sibling sorts above it (#619).
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
