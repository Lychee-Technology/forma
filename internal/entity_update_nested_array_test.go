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
// objects, order.items[].lots[].code, beside two attributes outside it. The
// writer stores a code at two array indices, which the rebuild cannot place
// where the write reads it back (#623).
func nestedArrayRegistry() forma.SchemaRegistry {
	return &stubSchemaRegistry{schemaID: nestedArraySchema, schemaName: "nested_array", cache: forma.SchemaAttributeCache{
		"order.items.lots.code": {AttributeID: 1, ValueType: forma.ValueTypeText},
		"order.note":            {AttributeID: 2, ValueType: forma.ValueTypeText},
		"stage":                 {AttributeID: 3, ValueType: forma.ValueTypeText},
	}}
}

func nestedArrayDocument(code string) map[string]any {
	return map[string]any{"order": map[string]any{
		"items": []any{map[string]any{"lots": []any{map[string]any{"code": code}}}},
	}}
}

// seedNestedArrayRow stores, through the writer, a row holding one code at
// order.items[0].lots[0], order.note and stage.
func seedNestedArrayRow(t *testing.T) (forma.EntityManager, *mockPersistentRecordRepository, uuid.UUID) {
	t.Helper()
	registry := nestedArrayRegistry()
	tr := transform.NewPersistentRecordTransformer(registry)
	repo := newMockPersistentRecordRepository()
	rowID := uuid.New()
	data := nestedArrayDocument("c")
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

// An update that does not address the nested array would write the row
// without its record, which replaceEAVAttributes then deletes (#619 review).
// Every update surface refuses it instead and leaves the row as stored. The
// stored row is at fault, so the refusal is a plain error naming the record,
// never a 4xx, and a best-effort batch publishes nothing of it.
func TestUnrelatedUpdateOverNestedArrayIsRefused(t *testing.T) {
	unrelated := map[string]map[string]any{
		"attribute outside the order": {"stage": "contacted"},
		"nested sibling":              {"order": map[string]any{"note": "n"}},
		"literal sibling":             {"order.note": "n"},
	}
	for _, surface := range updateSurfaces {
		for name, updates := range unrelated {
			t.Run(surface.name+"/"+name, func(t *testing.T) {
				em, repo, rowID := seedNestedArrayRow(t)
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

// The refusal never blocks the update that replaces the nested array, in
// either spelling of its attribute: the row then stores the replacement and
// keeps every record the update did not address.
func TestUpdateReplacingNestedArrayWritesIt(t *testing.T) {
	replacements := []struct {
		name    string
		updates map[string]any
		want    string
	}{
		{"nested", nestedArrayDocument("d"), "1[0,0]=d"},
		{"literal", map[string]any{"order.items.lots.code": "d"}, "1[]=d"},
	}
	for _, surface := range updateSurfaces {
		for _, replacement := range replacements {
			t.Run(surface.name+"/"+replacement.name, func(t *testing.T) {
				em, repo, rowID := seedNestedArrayRow(t)

				if _, err := surface.update(context.Background(), em, nestedArrayUpdate(rowID, replacement.updates)); err != nil {
					t.Fatalf("update replacing the nested array: %v", err)
				}
				assertEqualValue(t, "stored records", []string{replacement.want, "2[]=kept", "3[]=new"},
					storedTextRecords(repo.records[nestedArraySchema][rowID]))
			})
		}
	}
}
