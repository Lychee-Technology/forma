package transform

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// newAncestryRegistry is newRebuildRegistry's schema with its array ancestry
// declared, as the file registry derives it from the schema document (#623):
// order.items is an array of objects, and so is order.items.lots inside it.
// It adds two lists, a second object member and a second array member
// inside the array of objects, and a list inside the inner array.
func newAncestryRegistry() *stubSchemaRegistry {
	cache := newRebuildRegistry().(*stubSchemaRegistry).cache
	cache["order.items.tags"] = forma.AttributeMetadata{AttributeName: "order.items.tags", AttributeID: 19, ValueType: forma.ValueTypeList}
	cache["order.items.lots.n"] = forma.AttributeMetadata{AttributeName: "order.items.lots.n", AttributeID: 20, ValueType: forma.ValueTypeText}
	cache["order.items.dims.h"] = forma.AttributeMetadata{AttributeName: "order.items.dims.h", AttributeID: 21, ValueType: forma.ValueTypeText}
	cache["order.items.lots.tags"] = forma.AttributeMetadata{AttributeName: "order.items.lots.tags", AttributeID: 22, ValueType: forma.ValueTypeList}
	cache["order.items.marks"] = forma.AttributeMetadata{AttributeName: "order.items.marks", AttributeID: 23, ValueType: forma.ValueTypeList}
	declareArrayAncestors(cache, "order.items", "order.items.lots")
	return &stubSchemaRegistry{schemaID: 619, schemaName: "rebuild", cache: cache}
}

// newRegistryWithoutAncestry is newAncestryRegistry's schema with no
// attribute declaring its array ancestry, as a registry that cannot derive
// it registers the schema.
func newRegistryWithoutAncestry() *stubSchemaRegistry {
	registry := newAncestryRegistry()
	for name, meta := range registry.cache {
		meta.ArrayAncestors = nil
		registry.cache[name] = meta
	}
	return registry
}

// declareArrayAncestors sets every attribute's ArrayAncestors to the arrays
// of objects among its proper prefixes.
func declareArrayAncestors(cache forma.SchemaAttributeCache, arrays ...string) {
	for name, meta := range cache {
		meta.ArrayAncestors = []string{}
		for _, array := range arrays {
			if strings.HasPrefix(name, array+".") {
				meta.ArrayAncestors = append(meta.ArrayAncestors, array)
			}
		}
		cache[name] = meta
	}
}

// With array ancestry declared, every record is placed where it was written,
// in every record order (#619, #623): an object or an array of objects
// inside an array of objects keeps its element, a member some elements lack
// is absent from them, and a list's empty-list marker clears the list of its
// own element.
func TestFromAttributesPlacesByArrayAncestry(t *testing.T) {
	tr := NewTransformer(newAncestryRegistry())
	for _, tc := range append(indexedPlacements(), fewerIndicesPlacements()...) {
		t.Run(tc.name, func(t *testing.T) {
			for _, order := range permutations(tc.attrs) {
				got, err := tr.FromAttributes(context.Background(), order)
				require.NoError(t, err)
				require.Equal(t, tc.want, got, "record order %+v", order)
			}
		})
	}
}

type ancestryPlacement struct {
	name  string
	attrs []model.EntityAttribute
	want  map[string]any
}

// orderItems is an order whose items, an array of objects, are elements.
func orderItems(elements ...any) map[string]any {
	return map[string]any{"order": map[string]any{"items": elements}}
}

// indexedPlacements are rows whose records carry an index for every array
// ancestor of their attribute, as the write stores a value nested inside
// each array it crosses.
func indexedPlacements() []ancestryPlacement {
	return []ancestryPlacement{
		{
			name:  "object inside an array of objects, in one element of two",
			attrs: []model.EntityAttribute{rebuildAttr(17, "0", "w"), rebuildAttr(9, "1", "b")},
			want:  orderItems(map[string]any{"dims": map[string]any{"w": "w"}}, map[string]any{"sku": "b"}),
		},
		{
			name: "array of objects inside an array of objects",
			attrs: []model.EntityAttribute{
				rebuildAttr(18, "0,0", "c00"), rebuildAttr(18, "0,1", "c01"),
				rebuildAttr(18, "1,0", "c10"), rebuildAttr(8, "1", int64(2)),
			},
			want: orderItems(
				map[string]any{"lots": []any{map[string]any{"code": "c00"}, map[string]any{"code": "c01"}}},
				map[string]any{"lots": []any{map[string]any{"code": "c10"}}, "qty": int64(2)},
			),
		},
		{
			name:  "an element no record reaches reads as {} (#626)",
			attrs: []model.EntityAttribute{rebuildAttr(18, "1,1", "c11")},
			want:  orderItems(map[string]any{}, map[string]any{"lots": []any{map[string]any{}, map[string]any{"code": "c11"}}}),
		},
		{
			name:  "empty-list marker in its element",
			attrs: []model.EntityAttribute{rebuildAttr(9, "0", "a"), rebuildAttr(19, "1", nil)},
			want:  orderItems(map[string]any{"sku": "a"}, map[string]any{"tags": []any{}}),
		},
		{
			name:  "list row with no value at an element's index",
			attrs: []model.EntityAttribute{rebuildAttr(11, "0", "t0"), rebuildAttr(11, "1", nil)},
			want:  map[string]any{"tags": []any{"t0"}},
		},
		{
			name:  "legacy text-typed nested array alone reads as its own array",
			attrs: []model.EntityAttribute{rebuildAttr(12, "0", "111"), rebuildAttr(12, "1", "222")},
			want:  map[string]any{"legacy": map[string]any{"phones": []any{"111", "222"}}},
		},
		{
			name:  "legacy text-typed nested array beside an array of objects",
			attrs: []model.EntityAttribute{rebuildAttr(15, "0", "x"), rebuildAttr(15, "1", "y"), rebuildAttr(8, "0", int64(1))},
			want: map[string]any{"order": map[string]any{
				"items": []any{map[string]any{"qty": int64(1)}}, "tags": []any{"x", "y"},
			}},
		},
	}
}

// fewerIndicesPlacements are rows holding a record with fewer indices than
// its attribute has array ancestors: one written through a literal dotted
// key that crosses an array, where an element held the inner array as an
// object, or collapsed by the federated reader.
func fewerIndicesPlacements() []ancestryPlacement {
	return []ancestryPlacement{
		{
			// The federated reader collapses a member of an array of objects to
			// one value with no indices; with nothing indexing the array it
			// reads as an object, as it did before #623.
			name:  "fewer indices than ancestors, nothing indexes the array",
			attrs: []model.EntityAttribute{rebuildAttr(9, "", "a"), rebuildAttr(21, "", "h")},
			want: map[string]any{"order": map[string]any{"items": map[string]any{
				"sku": "a", "dims": map[string]any{"h": "h"},
			}}},
		},
		{
			// {"order": {"items.sku": "a", "items": [{"qty": 1}]}} stores sku
			// with no indices; it reads back under the literal key it was
			// written through, beside the array.
			name:  "fewer indices than ancestors beside the array's elements",
			attrs: []model.EntityAttribute{rebuildAttr(9, "", "a"), rebuildAttr(8, "0", int64(1))},
			want: map[string]any{"order": map[string]any{
				"items": []any{map[string]any{"qty": int64(1)}}, "items.sku": "a",
			}},
		},
		{
			name:  "fewer indices than ancestors inside an element",
			attrs: []model.EntityAttribute{rebuildAttr(20, "0", "n"), rebuildAttr(18, "0,0", "c")},
			want:  orderItems(map[string]any{"lots": []any{map[string]any{"code": "c"}}, "lots.n": "n"}),
		},
		{
			// An update's validation only reports, so an element can hold the
			// inner array as an object.
			name:  "fewer indices than ancestors where its element holds no such array",
			attrs: []model.EntityAttribute{rebuildAttr(18, "1", "x"), rebuildAttr(18, "0,0", "c")},
			want: orderItems(
				map[string]any{"lots": []any{map[string]any{"code": "c"}}},
				map[string]any{"lots": map[string]any{"code": "x"}},
			),
		},
		{
			name:  "empty-list marker with fewer indices than ancestors beside the array's elements",
			attrs: []model.EntityAttribute{rebuildAttr(22, "0", nil), rebuildAttr(18, "0,0", "c")},
			want:  orderItems(map[string]any{"lots": []any{map[string]any{"code": "c"}}, "lots.tags": []any{}}),
		},
		{
			// {"order": {"items": [{"sku": "a"}], "items.tags": []}} stores
			// the marker with no indices, as a write before #623 stored one
			// inside an element.
			name:  "empty-list marker with no indices beside the array's elements",
			attrs: []model.EntityAttribute{rebuildAttr(19, "", nil), rebuildAttr(9, "0", "a")},
			want: map[string]any{"order": map[string]any{
				"items": []any{map[string]any{"sku": "a"}}, "items.tags": []any{},
			}},
		},
		{
			name:  "empty-list marker of a list inside the inner array, with no indices beside the outer array's elements",
			attrs: []model.EntityAttribute{rebuildAttr(22, "", nil), rebuildAttr(9, "0", "a")},
			want: map[string]any{"order": map[string]any{
				"items": []any{map[string]any{"sku": "a"}}, "items.lots.tags": []any{},
			}},
		},
		{
			name:  "empty-list marker written before #623 alone",
			attrs: []model.EntityAttribute{rebuildAttr(19, "", nil)},
			want:  map[string]any{"order": map[string]any{"items": map[string]any{"tags": []any{}}}},
		},
	}
}

// ancestryRows returns every combination of one record of each kind an order
// can hold under array ancestry, the ones written through a literal dotted
// key that crosses an array included.
func ancestryRows() [][]model.EntityAttribute {
	pool := []model.EntityAttribute{
		rebuildAttr(10, "", "n"),      // order.note, a scalar
		rebuildAttr(16, "0", "l"),     // order.labels, a list
		rebuildAttr(15, "0", "t"),     // order.tags, a legacy text-typed array
		rebuildAttr(8, "0", int64(1)), // order.items.qty, an array-of-objects member
		rebuildAttr(9, "1", "a"),      // order.items.sku, its sibling in another element
		rebuildAttr(17, "1", "w"),     // order.items.dims.w, an object inside the array
		rebuildAttr(18, "1,0", "c"),   // order.items.lots.code, an array inside the array
		rebuildAttr(19, "0", nil),     // order.items.tags, a list's marker inside the array
		rebuildAttr(20, "0", "ln"),    // order.items.lots.n, written as "lots.n" in an element
		rebuildAttr(21, "", "h"),      // order.items.dims.h, written as "items.dims.h" in order
		rebuildAttr(22, "1", nil),     // order.items.lots.tags, written as "lots.tags": [] in an element
		rebuildAttr(23, "", nil),      // order.items.marks, written as "items.marks": [] in order
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

// With array ancestry declared, whatever a row holds rebuilds as one document
// in every record order, and that document handed back to the write stores
// exactly the row's records (#619, #623): none is refused, and none lands
// where the write cannot read it back. The object and the array of objects
// nested in the second element keep their written shape beside every
// combination of the others.
func TestFromAttributesByAncestryWritesBackEveryRecord(t *testing.T) {
	tr := NewTransformer(newAncestryRegistry())
	ctx := context.Background()
	r := rand.New(rand.NewSource(623))
	recordKeys := func(attrs []model.EntityAttribute) []string {
		keys := make([]string, 0, len(attrs))
		for _, attr := range attrs {
			keys = append(keys, fmt.Sprintf("%d[%s]=%v", attr.AttrID, attr.ArrayIndices, attr.Value))
		}
		return keys
	}

	for _, row := range ancestryRows() {
		doc, err := tr.FromAttributes(ctx, row)
		require.NoError(t, err)
		for i := 0; i < 24; i++ {
			order := append([]model.EntityAttribute(nil), row...)
			r.Shuffle(len(order), func(a, b int) { order[a], order[b] = order[b], order[a] })
			got, err := tr.FromAttributes(ctx, order)
			require.NoError(t, err)
			require.Equal(t, doc, got, "record order %+v", order)
		}

		for _, attr := range row {
			switch attr.AttrID {
			case 17:
				require.Equal(t, map[string]any{"w": "w"}, secondItem(t, doc)["dims"], "row %+v", row)
			case 18:
				require.Equal(t, []any{map[string]any{"code": "c"}}, secondItem(t, doc)["lots"], "row %+v", row)
			}
		}

		rewritten, err := tr.ToAttributes(ctx, 619, uuid.Must(uuid.NewV7()), doc)
		require.NoError(t, err, "row %+v rebuilt as %v", row, doc)
		require.ElementsMatch(t, recordKeys(row), recordKeys(rewritten), "row %+v rebuilt as %v", row, doc)
	}
}

func secondItem(t *testing.T, doc map[string]any) map[string]any {
	t.Helper()
	order, _ := doc["order"].(map[string]any)
	items, _ := order["items"].([]any)
	require.Len(t, items, 2, "rebuilt as %v", doc)
	item, ok := items[1].(map[string]any)
	require.True(t, ok, "rebuilt as %v", doc)
	return item
}

// A row rebuilds by ancestry only when every attribute in it declares its
// ancestors: one that does not reads the whole row as before (#623), so no
// row mixes the two readings.
func TestFromAttributesWithUndeclaredAncestryInfersTheRow(t *testing.T) {
	registry := newAncestryRegistry()
	meta := registry.cache["order.note"]
	meta.ArrayAncestors = nil
	registry.cache["order.note"] = meta

	got, err := NewTransformer(registry).FromAttributes(context.Background(), []model.EntityAttribute{
		rebuildAttr(17, "0", "w"), rebuildAttr(10, "", "n"),
	})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"order": map[string]any{
		"items": map[string]any{"dims": []any{map[string]any{"w": "w"}}}, "note": "n",
	}}, got)
}

// A stored index below zero is a damaged row: the read refuses it, plainly,
// instead of placing the record nowhere.
func TestFromAttributesRefusesNegativeIndex(t *testing.T) {
	_, err := NewTransformer(newAncestryRegistry()).FromAttributes(context.Background(), []model.EntityAttribute{
		rebuildAttr(9, "-1", "a"),
	})
	require.ErrorContains(t, err, "negative index '-1'")
	require.NotErrorIs(t, err, forma.ErrInvalidInput)
}
