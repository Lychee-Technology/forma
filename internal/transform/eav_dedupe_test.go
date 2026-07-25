package transform

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// newDottedStubRegistry builds a schema whose attribute names are dotted, the
// shape that lets flattenToAttributes reach the same attribute twice: once by
// recursing into {"contact":{"email":...}} and once through a literal
// "contact.email" key (#312).
func newDottedStubRegistry() forma.SchemaRegistry {
	return &stubSchemaRegistry{
		schemaID:   100,
		schemaName: "test",
		cache: forma.SchemaAttributeCache{
			"contact.email":  {AttributeName: "contact.email", AttributeID: 10, ValueType: forma.ValueTypeText},
			"contact.phone":  {AttributeName: "contact.phone", AttributeID: 11, ValueType: forma.ValueTypeText},
			"contact.emails": {AttributeName: "contact.emails", AttributeID: 12, ValueType: forma.ValueTypeList},
		},
	}
}

// toEAV runs the full write-path conversion the repository layer sees.
func toEAV(t *testing.T, registry forma.SchemaRegistry, rowID uuid.UUID, data map[string]any) []model.EAVRecord {
	t.Helper()

	transformer := NewTransformer(registry)
	attrs, err := transformer.ToAttributes(context.Background(), 100, rowID, data)
	require.NoError(t, err)

	records, err := NewAttributeConverter(registry).ToEAVRecords(attrs, rowID)
	require.NoError(t, err)
	return records
}

// requireNoDuplicatePK asserts the slice is insertable into eav_data, whose
// primary key is (schema_id, row_id, attr_id, array_indices).
func requireNoDuplicatePK(t *testing.T, records []model.EAVRecord) {
	t.Helper()

	seen := make(map[eavPrimaryKey]struct{}, len(records))
	for _, record := range records {
		key := eavPrimaryKey{
			schemaID:     record.SchemaID,
			rowID:        record.RowID,
			attrID:       record.AttrID,
			arrayIndices: record.ArrayIndices,
		}
		_, duplicate := seen[key]
		require.Falsef(t, duplicate, "duplicate EAV primary key attr_id=%d array_indices=%q", record.AttrID, record.ArrayIndices)
		seen[key] = struct{}{}
	}
}

func findRecord(t *testing.T, records []model.EAVRecord, attrID int16, indices string) model.EAVRecord {
	t.Helper()

	for _, record := range records {
		if record.AttrID == attrID && record.ArrayIndices == indices {
			return record
		}
	}
	t.Fatalf("no record for attr_id=%d array_indices=%q", attrID, indices)
	return model.EAVRecord{}
}

// TestUpdateMergeOfDottedKeyYieldsOneRecord is the #312 regression: mergeMaps is
// key-literal while FromPersistentRecord re-nests stored attributes, so
// PUT {"contact.email":"new"} on a stored {"contact":{"email":"old"}} reaches the
// transformer as both shapes at once. Last-wins keeps the caller's value.
func TestUpdateMergeOfDottedKeyYieldsOneRecord(t *testing.T) {
	registry := newDottedStubRegistry()
	rowID := uuid.Must(uuid.NewV7())

	merged := map[string]any{
		"contact":       map[string]any{"email": "old@example.com"},
		"contact.email": "new@example.com",
	}

	records := toEAV(t, registry, rowID, merged)

	requireNoDuplicatePK(t, records)
	require.Len(t, records, 1)
	require.NotNil(t, records[0].ValueText)
	require.Equal(t, "new@example.com", *records[0].ValueText)
}

// TestCreateWithNestedAndDottedKeyYieldsOneRecord covers the create-path shape,
// where both spellings arrive in a single caller payload.
func TestCreateWithNestedAndDottedKeyYieldsOneRecord(t *testing.T) {
	registry := newDottedStubRegistry()
	rowID := uuid.Must(uuid.NewV7())

	records := toEAV(t, registry, rowID, map[string]any{
		"contact":       map[string]any{"email": "a"},
		"contact.email": "b",
	})

	requireNoDuplicatePK(t, records)
	require.Len(t, records, 1)
	require.NotNil(t, records[0].ValueText)
	require.Equal(t, "b", *records[0].ValueText)
}

// TestDottedListCollapsesPerIndex covers the equal-length list vector: every
// element index collides, and each index keeps the winning spelling's value.
func TestDottedListCollapsesPerIndex(t *testing.T) {
	registry := newDottedStubRegistry()
	rowID := uuid.Must(uuid.NewV7())

	records := toEAV(t, registry, rowID, map[string]any{
		"contact":        map[string]any{"emails": []any{"nested0", "nested1"}},
		"contact.emails": []any{"literal0", "literal1"},
	})

	requireNoDuplicatePK(t, records)
	require.Len(t, records, 2)

	first := findRecord(t, records, 12, "0")
	require.NotNil(t, first.ValueText)
	require.Equal(t, "literal0", *first.ValueText)

	second := findRecord(t, records, 12, "1")
	require.NotNil(t, second.ValueText)
	require.Equal(t, "literal1", *second.ValueText)
}

// TestDottedListShorterReplacementDropsTrailingElements is the Finding 1 vector
// that per-primary-key dedupe got silently wrong: a replacement list shorter
// than the stored one collides only on the indices it happens to cover, so the
// stored tail survived into a list the caller replaced. The unit of replacement
// is the attribute, not the index.
func TestDottedListShorterReplacementDropsTrailingElements(t *testing.T) {
	registry := newDottedStubRegistry()
	rowID := uuid.Must(uuid.NewV7())

	records := toEAV(t, registry, rowID, map[string]any{
		"contact":        map[string]any{"emails": []any{"old0", "old1"}},
		"contact.emails": []any{"new0"},
	})

	requireNoDuplicatePK(t, records)
	require.Len(t, records, 1)
	require.Equal(t, "0", records[0].ArrayIndices)
	require.NotNil(t, records[0].ValueText)
	require.Equal(t, "new0", *records[0].ValueText)
}

// TestDottedListClearDropsStoredElements is the other half of Finding 1. An
// explicit [] emits only the empty-list marker (array_indices ""), which
// collides with no element index at all, so per-key dedupe left every stored
// element in place and the clear silently did nothing.
func TestDottedListClearDropsStoredElements(t *testing.T) {
	registry := newDottedStubRegistry()
	rowID := uuid.Must(uuid.NewV7())

	records := toEAV(t, registry, rowID, map[string]any{
		"contact":        map[string]any{"emails": []any{"old0", "old1"}},
		"contact.emails": []any{},
	})

	requireNoDuplicatePK(t, records)
	require.Len(t, records, 1)
	require.Equal(t, int16(12), records[0].AttrID)
	require.Equal(t, "", records[0].ArrayIndices)
	require.Nil(t, records[0].ValueText)
	require.Nil(t, records[0].ValueNumeric)
}

// TestDottedListClearRoundTripsAsEmptyList pins the marker's purpose: a cleared
// list must read back as [] rather than as an absent attribute (#204).
func TestDottedListClearRoundTripsAsEmptyList(t *testing.T) {
	registry := newDottedStubRegistry()
	rowID := uuid.Must(uuid.NewV7())
	transformer := NewTransformer(registry)

	attrs, err := transformer.ToAttributes(context.Background(), 100, rowID, map[string]any{
		"contact":        map[string]any{"emails": []any{"old0", "old1"}},
		"contact.emails": []any{},
	})
	require.NoError(t, err)

	result, err := transformer.FromAttributes(context.Background(), attrs)
	require.NoError(t, err)

	contact, ok := result["contact"].(map[string]any)
	require.True(t, ok, "expected a nested contact object, got %#v", result)
	require.Equal(t, []any{}, contact["emails"])
}

// TestDottedListLongerReplacementKeepsAllNewElements covers the growth
// direction: the winning spelling's extra indices must all be kept.
func TestDottedListLongerReplacementKeepsAllNewElements(t *testing.T) {
	registry := newDottedStubRegistry()
	rowID := uuid.Must(uuid.NewV7())

	records := toEAV(t, registry, rowID, map[string]any{
		"contact":        map[string]any{"emails": []any{"old0"}},
		"contact.emails": []any{"new0", "new1"},
	})

	requireNoDuplicatePK(t, records)
	require.Len(t, records, 2)
	require.Equal(t, "new0", *findRecord(t, records, 12, "0").ValueText)
	require.Equal(t, "new1", *findRecord(t, records, 12, "1").ValueText)
}

// TestSingleSpellingListIsUntouched guards the non-collision case: an attribute
// the payload spells once must survive whole, every index of it.
func TestSingleSpellingListIsUntouched(t *testing.T) {
	registry := newDottedStubRegistry()
	rowID := uuid.Must(uuid.NewV7())

	records := toEAV(t, registry, rowID, map[string]any{
		"contact.emails": []any{"a", "b", "c"},
		"contact.email":  "solo@example.com",
	})

	requireNoDuplicatePK(t, records)
	require.Len(t, records, 4)
	require.Equal(t, "a", *findRecord(t, records, 12, "0").ValueText)
	require.Equal(t, "b", *findRecord(t, records, 12, "1").ValueText)
	require.Equal(t, "c", *findRecord(t, records, 12, "2").ValueText)
	require.Equal(t, "solo@example.com", *findRecord(t, records, 10, "").ValueText)
}

// TestFlattenOrderingIsDeterministic pins the property last-write-wins depends
// on: flattenToAttributes sorts each map's keys, and for any dotted name the
// nested spelling's top-level key is a proper prefix of the literal one, so the
// literal key's records are always emitted last regardless of map iteration
// order. Run with -count=10 to shake out a map-order dependency.
func TestFlattenOrderingIsDeterministic(t *testing.T) {
	registry := newDottedStubRegistry()
	rowID := uuid.Must(uuid.NewV7())

	for range 50 {
		records := toEAV(t, registry, rowID, map[string]any{
			"contact": map[string]any{
				"email":  "nested",
				"phone":  "nested-phone",
				"emails": []any{"nested0", "nested1", "nested2"},
			},
			"contact.email":  "literal",
			"contact.phone":  "literal-phone",
			"contact.emails": []any{"literal0"},
		})

		requireNoDuplicatePK(t, records)
		require.Len(t, records, 3)
		require.Equal(t, "literal", *findRecord(t, records, 10, "").ValueText)
		require.Equal(t, "literal-phone", *findRecord(t, records, 11, "").ValueText)
		require.Equal(t, "literal0", *findRecord(t, records, 12, "0").ValueText)
	}
}

// tag wraps a record with a spelling for the direct-call dedupe tests. The real
// tags come from spellingOf; these only need to be distinct where the test says
// two spellings, and equal where it says one.
func tag(spelling string, record model.EAVRecord) taggedEAVRecord {
	return taggedEAVRecord{record: record, spelling: spelling}
}

func TestDedupeEAVRecordsLeavesNonCollidingRecordsUntouched(t *testing.T) {
	rowID := uuid.Must(uuid.NewV7())
	other := uuid.Must(uuid.NewV7())
	text := func(s string) *string { return &s }

	records := []model.EAVRecord{
		{SchemaID: 100, RowID: rowID, AttrID: 10, ArrayIndices: "", ValueText: text("a")},
		{SchemaID: 100, RowID: rowID, AttrID: 12, ArrayIndices: "0", ValueText: text("b")},
		{SchemaID: 100, RowID: rowID, AttrID: 12, ArrayIndices: "1", ValueText: text("c")},
		{SchemaID: 100, RowID: other, AttrID: 10, ArrayIndices: "", ValueText: text("d")},
		{SchemaID: 101, RowID: rowID, AttrID: 10, ArrayIndices: "", ValueText: text("e")},
	}

	tagged := []taggedEAVRecord{
		tag("a", records[0]),
		tag("b", records[1]),
		tag("b", records[2]),
		tag("c", records[3]),
		tag("d", records[4]),
	}

	require.Equal(t, records, dedupeEAVRecords(tagged))
}

// TestDedupeEAVRecordsDropsEveryRecordOfALosingSpelling is the direct-call form
// of the Finding 1 semantics: a losing spelling loses all of its indices, even
// the ones the winner never covers.
func TestDedupeEAVRecordsDropsEveryRecordOfALosingSpelling(t *testing.T) {
	rowID := uuid.Must(uuid.NewV7())
	text := func(s string) *string { return &s }

	tagged := []taggedEAVRecord{
		tag("nested", model.EAVRecord{SchemaID: 100, RowID: rowID, AttrID: 12, ArrayIndices: "0", ValueText: text("old0")}),
		tag("nested", model.EAVRecord{SchemaID: 100, RowID: rowID, AttrID: 12, ArrayIndices: "1", ValueText: text("old1")}),
		tag("nested", model.EAVRecord{SchemaID: 100, RowID: rowID, AttrID: 12, ArrayIndices: "2", ValueText: text("old2")}),
		tag("literal", model.EAVRecord{SchemaID: 100, RowID: rowID, AttrID: 12, ArrayIndices: "0", ValueText: text("new0")}),
	}

	deduped := dedupeEAVRecords(tagged)

	require.Len(t, deduped, 1)
	require.Equal(t, "0", deduped[0].ArrayIndices)
	require.Equal(t, "new0", *deduped[0].ValueText)
}

// TestDedupeEAVRecordsKeepsLastAtFirstPosition pins the documented backstop: a
// residual primary-key collision inside one spelling still collapses last-wins
// at the first occurrence's position, so the slice stays insertable.
func TestDedupeEAVRecordsKeepsLastAtFirstPosition(t *testing.T) {
	rowID := uuid.Must(uuid.NewV7())
	text := func(s string) *string { return &s }

	tagged := []taggedEAVRecord{
		tag("one", model.EAVRecord{SchemaID: 100, RowID: rowID, AttrID: 10, ArrayIndices: "", ValueText: text("first")}),
		tag("two", model.EAVRecord{SchemaID: 100, RowID: rowID, AttrID: 11, ArrayIndices: "", ValueText: text("middle")}),
		tag("one", model.EAVRecord{SchemaID: 100, RowID: rowID, AttrID: 10, ArrayIndices: "", ValueText: text("last")}),
	}

	deduped := dedupeEAVRecords(tagged)

	require.Len(t, deduped, 2)
	require.Equal(t, int16(10), deduped[0].AttrID)
	require.Equal(t, "last", *deduped[0].ValueText)
	require.Equal(t, int16(11), deduped[1].AttrID)
	require.Equal(t, "middle", *deduped[1].ValueText)
}

// TestDedupeEAVRecordsSeparatesBatchRows guards the batch-assembled slices that
// insertEAVAttributes also receives: identical (attr_id, array_indices) pairs on
// different rows are distinct primary keys and must both survive, even when the
// same spelling produced them.
func TestDedupeEAVRecordsSeparatesBatchRows(t *testing.T) {
	text := func(s string) *string { return &s }
	rows := []uuid.UUID{uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7())}

	records := []model.EAVRecord{
		{SchemaID: 100, RowID: rows[0], AttrID: 10, ArrayIndices: "", ValueText: text("row0")},
		{SchemaID: 100, RowID: rows[1], AttrID: 10, ArrayIndices: "", ValueText: text("row1")},
	}

	tagged := []taggedEAVRecord{tag("same", records[0]), tag("same", records[1])}

	require.Equal(t, records, dedupeEAVRecords(tagged))
}

// TestSpellingOfDistinguishesDottedFromNested pins the encoding property the
// whole fix rests on: strings.Join would collapse these two paths into one
// name, which is precisely the collision the tag has to tell apart.
func TestSpellingOfDistinguishesDottedFromNested(t *testing.T) {
	require.NotEqual(t,
		spellingOf([]string{"contact", "emails"}),
		spellingOf([]string{"contact.emails"}),
	)
	require.Equal(t,
		spellingOf([]string{"contact", "emails"}),
		spellingOf([]string{"contact", "emails"}),
	)
	// Length prefixes make the encoding injective: no segment content can forge
	// another path's tag by containing the separator.
	require.NotEqual(t,
		spellingOf([]string{"a", "bc"}),
		spellingOf([]string{"ab", "c"}),
	)
}
