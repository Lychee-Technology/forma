package transform

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// newListStubRegistry builds a registry whose schema carries two list
// attributes: text items (default) and integer items.
func newListStubRegistry() forma.SchemaRegistry {
	return &stubSchemaRegistry{
		schemaID:   100,
		schemaName: "test",
		cache: forma.SchemaAttributeCache{
			"tags": {AttributeName: "tags", AttributeID: 18, ValueType: forma.ValueTypeList},
			"nums": {AttributeName: "nums", AttributeID: 19, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeInteger},
		},
	}
}

func TestAttributeConverterFromEAVRecords_ListElementsTypedByItemsType(t *testing.T) {
	registry := newListStubRegistry()
	converter := NewAttributeConverter(registry)
	rowID := uuid.New()

	text := func(s string) *string { return &s }
	num := func(f float64) *float64 { return &f }
	records := []model.EAVRecord{
		{SchemaID: 100, RowID: rowID, AttrID: 18, ArrayIndices: "0", ValueText: text("alpha")},
		{SchemaID: 100, RowID: rowID, AttrID: 18, ArrayIndices: "1", ValueText: text("beta")},
		{SchemaID: 100, RowID: rowID, AttrID: 19, ArrayIndices: "0", ValueNumeric: num(7)},
	}

	attrs, err := converter.FromEAVRecords(records)
	if err != nil {
		t.Fatalf("FromEAVRecords: %v", err)
	}
	if len(attrs) != 3 {
		t.Fatalf("got %d attributes, want 3", len(attrs))
	}
	for _, attr := range attrs {
		switch {
		case attr.AttrID == 18:
			if attr.ValueType != forma.ValueTypeText {
				t.Errorf("tags element ValueType = %q, want text (items type)", attr.ValueType)
			}
			if _, ok := attr.Value.(string); !ok {
				t.Errorf("tags element value = %T(%v), want string", attr.Value, attr.Value)
			}
		case attr.AttrID == 19:
			if attr.ValueType != forma.ValueTypeInteger {
				t.Errorf("nums element ValueType = %q, want integer (items type)", attr.ValueType)
			}
			if _, ok := attr.Value.(int32); !ok {
				t.Errorf("nums element value = %T(%v), want int32", attr.Value, attr.Value)
			}
		}
	}
}

// TestPersistentRecord_EmptyListRoundTrip pins the #204 empty-list contract:
// {"tags": []} persists a single marker EAV row (array_indices "", both value
// columns NULL) and reads back as an explicit empty array — distinguishable
// from an absent attribute. Under merge-update semantics "tags": [] is the
// only way to clear a list, so it must not be lossy.
func TestPersistentRecord_EmptyListRoundTrip(t *testing.T) {
	ctx := t.Context()
	registry := newListStubRegistry()
	prt := NewPersistentRecordTransformer(registry)
	rowID := uuid.Must(uuid.NewV7())

	record, err := prt.ToPersistentRecord(ctx, 100, rowID, map[string]any{
		"tags": []any{},
		"nums": []any{1, 2},
	})
	if err != nil {
		t.Fatalf("ToPersistentRecord: %v", err)
	}
	if len(record.OtherAttributes) != 3 {
		t.Fatalf("got %d EAV records, want 3 (1 tags marker + 2 nums): %+v", len(record.OtherAttributes), record.OtherAttributes)
	}
	var marker *model.EAVRecord
	for i := range record.OtherAttributes {
		r := &record.OtherAttributes[i]
		if r.AttrID == 18 {
			marker = r
		}
	}
	if marker == nil {
		t.Fatal("no marker EAV record for empty tags list")
	}
	if marker.ArrayIndices != "" || marker.ValueText != nil || marker.ValueNumeric != nil {
		t.Errorf("marker = %+v, want array_indices '' and both value columns nil", marker)
	}

	rebuilt, err := prt.FromPersistentRecord(ctx, record)
	if err != nil {
		t.Fatalf("FromPersistentRecord: %v", err)
	}
	tags, ok := rebuilt["tags"].([]any)
	if !ok {
		t.Fatalf("rebuilt tags = %#v (%T), want explicit empty []any", rebuilt["tags"], rebuilt["tags"])
	}
	if len(tags) != 0 {
		t.Errorf("rebuilt tags = %#v, want empty array", tags)
	}
}

func TestPersistentRecord_ListRoundTrip(t *testing.T) {
	ctx := t.Context()
	registry := newListStubRegistry()
	prt := NewPersistentRecordTransformer(registry)
	rowID := uuid.Must(uuid.NewV7())

	record, err := prt.ToPersistentRecord(ctx, 100, rowID, map[string]any{
		"tags": []any{"alpha", "beta", "gamma"},
		"nums": []any{1, 2},
	})
	if err != nil {
		t.Fatalf("ToPersistentRecord: %v", err)
	}
	if len(record.OtherAttributes) != 5 {
		t.Fatalf("got %d EAV records, want 5 (3 tags + 2 nums): %+v", len(record.OtherAttributes), record.OtherAttributes)
	}
	byKey := make(map[string]model.EAVRecord)
	for _, r := range record.OtherAttributes {
		byKey[strconv.Itoa(int(r.AttrID))+"|"+r.ArrayIndices] = r
	}
	for i, want := range []string{"alpha", "beta", "gamma"} {
		r, ok := byKey["18|"+strconv.Itoa(i)]
		if !ok || r.ValueText == nil || *r.ValueText != want {
			t.Errorf("tags[%d]: got %+v, want value_text %q", i, r, want)
		}
	}
	for i, want := range []float64{1, 2} {
		r, ok := byKey["19|"+strconv.Itoa(i)]
		if !ok || r.ValueNumeric == nil || *r.ValueNumeric != want {
			t.Errorf("nums[%d]: got %+v, want value_numeric %v", i, r, want)
		}
	}

	// Round trip back to JSON rebuilds ordered arrays.
	rebuilt, err := prt.FromPersistentRecord(ctx, record)
	if err != nil {
		t.Fatalf("FromPersistentRecord: %v", err)
	}
	if !reflect.DeepEqual(rebuilt["tags"], []any{"alpha", "beta", "gamma"}) {
		t.Errorf("rebuilt tags = %#v, want [alpha beta gamma]", rebuilt["tags"])
	}
	nums, ok := rebuilt["nums"].([]any)
	if !ok || len(nums) != 2 {
		t.Fatalf("rebuilt nums = %#v, want 2-element array", rebuilt["nums"])
	}
}
