package internal

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/transform"
)

const nestedArraySchema = 624

// nestedArrayRegistry declares an array of objects nested inside an array of
// objects, order.items[].lots[].code, beside two attributes outside it, and a
// text-typed member of the outer array, order.items[].sku, that a pre-#204
// attributes file left holding an array. The writer stores a code, and each
// element of such a sku, at two array indices. With ancestry the registry
// declares both arrays, as the file registry derives them from a schema
// document, and the rebuild places the code where it was written (#623);
// without, it cannot place it where the write reads it back.
func nestedArrayRegistry(ancestry bool) forma.SchemaRegistry {
	cache := forma.SchemaAttributeCache{
		"order.items.lots.code": {AttributeID: 1, ValueType: forma.ValueTypeText},
		"order.note":            {AttributeID: 2, ValueType: forma.ValueTypeText},
		"stage":                 {AttributeID: 3, ValueType: forma.ValueTypeText},
		"order.items.sku":       {AttributeID: 4, ValueType: forma.ValueTypeText},
	}
	if ancestry {
		ancestors := map[string][]string{
			"order.items.lots.code": {"order.items", "order.items.lots"},
			"order.items.sku":       {"order.items"},
		}
		for name, meta := range cache {
			meta.ArrayAncestors = append([]string{}, ancestors[name]...)
			cache[name] = meta
		}
	}
	return &stubSchemaRegistry{schemaID: nestedArraySchema, schemaName: "nested_array", cache: cache}
}

func nestedArrayDocument(code string) map[string]any {
	return map[string]any{"order": map[string]any{
		"items": []any{map[string]any{"lots": []any{map[string]any{"code": code}}}},
	}}
}

// seedNestedArrayRow stores, through the writer, a row holding one code at
// order.items[0].lots[0], order.note and stage.
func seedNestedArrayRow(t *testing.T, ancestry bool) (forma.EntityManager, *mockPersistentRecordRepository, uuid.UUID) {
	t.Helper()
	return seedNestedArrayDocument(t, ancestry, nestedArrayDocument("c"))
}

// seedNestedArrayDocument stores data, with order.note and stage added,
// through the writer.
func seedNestedArrayDocument(t *testing.T, ancestry bool, data map[string]any) (forma.EntityManager, *mockPersistentRecordRepository, uuid.UUID) {
	t.Helper()
	registry := nestedArrayRegistry(ancestry)
	tr := transform.NewPersistentRecordTransformer(registry)
	repo := newMockPersistentRecordRepository()
	rowID := uuid.New()
	data["order"].(map[string]any)["note"] = "kept"
	data["stage"] = "new"
	repo.storeRecord(buildPersistentRecord(t, tr, nestedArraySchema, rowID, data))
	return mustNewEntityManager(t, tr, repo, nil, registry, createTestConfig(), nil), repo, rowID
}

func nestedArrayUpdate(rowID uuid.UUID, updates map[string]any) *forma.EntityOperation {
	return &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "nested_array", RowID: rowID},
		Type:             forma.OperationUpdate,
		Updates:          updates,
	}
}

// storedTextRecords names each stored eav_data record of record by attribute
// id, array indices and text.
func storedTextRecords(record *model.PersistentRecord) []string {
	records := make([]string, 0, len(record.OtherAttributes))
	for _, eav := range record.OtherAttributes {
		text := "<none>"
		if eav.ValueText != nil {
			text = *eav.ValueText
		}
		records = append(records, fmt.Sprintf("%d[%s]=%s", eav.AttrID, eav.ArrayIndices, text))
	}
	sort.Strings(records)
	return records
}

// unrelatedNestedArrayUpdates address none of the nested array's records,
// each with the stored records it leaves besides the code.
var unrelatedNestedArrayUpdates = map[string]struct {
	updates map[string]any
	others  []string
}{
	"attribute outside the order": {map[string]any{"stage": "contacted"}, []string{"2[]=kept", "3[]=contacted"}},
	"nested sibling":              {map[string]any{"order": map[string]any{"note": "n"}}, []string{"2[]=n", "3[]=new"}},
	"literal sibling":             {map[string]any{"order.note": "n"}, []string{"2[]=n", "3[]=new"}},
}

// With array ancestry declared, every update surface writes back the nested
// array an update does not address, and answers with it in place (#623).
func TestUnrelatedUpdateOverNestedArrayKeepsIt(t *testing.T) {
	for _, surface := range updateSurfaces {
		for name, unrelated := range unrelatedNestedArrayUpdates {
			t.Run(surface.name+"/"+name, func(t *testing.T) {
				em, repo, rowID := seedNestedArrayRow(t, true)

				updated, err := surface.update(context.Background(), em, nestedArrayUpdate(rowID, unrelated.updates))
				if err != nil {
					t.Fatalf("update of %s: %v", name, err)
				}
				assertEqualValue(t, "stored records", append([]string{"1[0,0]=c"}, unrelated.others...),
					storedTextRecords(repo.records[nestedArraySchema][rowID]))
				assertEqualValue(t, "answered items", nestedArrayDocument("c")["order"].(map[string]any)["items"],
					valueAtPath(updated, "order.items"))
			})
		}
	}
}

// A text-typed member of an array of objects stored holding an array (pre-#204
// metadata) is written back the same way under array ancestry: its indices
// are the outer array's and then its own (#623).
func TestUnrelatedUpdateOverLegacyArrayInObjectArrayKeepsIt(t *testing.T) {
	items := []any{map[string]any{"sku": []any{"a", "b"}}}
	for _, surface := range updateSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			em, repo, rowID := seedNestedArrayDocument(t, true, map[string]any{"order": map[string]any{"items": items}})

			updated, err := surface.update(context.Background(), em, nestedArrayUpdate(rowID, map[string]any{"stage": "contacted"}))
			if err != nil {
				t.Fatalf("unrelated update: %v", err)
			}
			assertEqualValue(t, "stored records", []string{"2[]=kept", "3[]=contacted", "4[0,0]=a", "4[0,1]=b"},
				storedTextRecords(repo.records[nestedArraySchema][rowID]))
			assertEqualValue(t, "answered items", items, valueAtPath(updated, "order.items"))
		})
	}
}

// Without array ancestry, an update that does not address the nested array
// would write the row without its record, which replaceEAVAttributes then
// deletes (#619 review). Every update surface refuses it instead and leaves
// the row as stored. The stored row is at fault, so the refusal is a plain
// error naming the record, never a 4xx, and a best-effort batch publishes
// nothing of it.
func TestUnrelatedUpdateOverNestedArrayIsRefused(t *testing.T) {
	for _, surface := range updateSurfaces {
		for name, unrelated := range unrelatedNestedArrayUpdates {
			updates := unrelated.updates
			t.Run(surface.name+"/"+name, func(t *testing.T) {
				em, repo, rowID := seedNestedArrayRow(t, false)
				before := repo.records[nestedArraySchema][rowID]

				_, err := surface.update(context.Background(), em, nestedArrayUpdate(rowID, updates))
				if err == nil {
					t.Fatalf("update of %s succeeded; stored records now %v", name, storedTextRecords(repo.records[nestedArraySchema][rowID]))
				}
				assertNestedArrayRefusal(t, err, rowID)
				if repo.records[nestedArraySchema][rowID] != before {
					t.Fatalf("refused update of %s stored a new row", name)
				}
			})
		}
	}
}

func assertNestedArrayRefusal(t *testing.T, err error, rowID uuid.UUID) {
	t.Helper()
	var reported bestEffortFailure
	if errors.As(err, &reported) {
		if reported.failure.Code != "UPDATE_FAILED" || reported.failure.Error != undisclosedBatchError {
			t.Fatalf("a stored-row error must not be published, got %s: %q", reported.failure.Code, reported.failure.Error)
		}
		return
	}
	if errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("a stored value the write cannot place is not caller input: %v", err)
	}
	for _, want := range []string{
		rowID.String(), "attrID=1 arrayIndices=0,0", "'order.items.lots.code'", "'order.items.lots.code.code'",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected the error to name %q, got %v", want, err)
		}
	}
}

// The update that replaces the nested array, in either spelling of its
// attribute, stores the replacement and keeps every record the update did not
// address, with or without array ancestry: the refusal without it never
// blocks the update.
func TestUpdateReplacingNestedArrayWritesIt(t *testing.T) {
	replacements := []struct {
		name    string
		updates map[string]any
		want    string
	}{
		{"nested", nestedArrayDocument("d"), "1[0,0]=d"},
		{"literal", map[string]any{"order.items.lots.code": "d"}, "1[]=d"},
	}
	for _, ancestry := range []bool{true, false} {
		for _, surface := range updateSurfaces {
			for _, replacement := range replacements {
				t.Run(fmt.Sprintf("%s/%s/ancestry=%t", surface.name, replacement.name, ancestry), func(t *testing.T) {
					em, repo, rowID := seedNestedArrayRow(t, ancestry)

					if _, err := surface.update(context.Background(), em, nestedArrayUpdate(rowID, replacement.updates)); err != nil {
						t.Fatalf("update replacing the nested array: %v", err)
					}
					assertEqualValue(t, "stored records", []string{replacement.want, "2[]=kept", "3[]=new"},
						storedTextRecords(repo.records[nestedArraySchema][rowID]))
				})
			}
		}
	}
}
