package federated

import (
	"strings"
	"testing"

	forma "github.com/lychee-technology/forma"
)

type stubSchemaRegistry struct {
	cacheByID map[int16]forma.SchemaAttributeCache
}

func (s stubSchemaRegistry) GetSchemaAttributeCacheByName(name string) (int16, forma.SchemaAttributeCache, error) {
	return 0, nil, nil
}

func (s stubSchemaRegistry) GetSchemaAttributeCacheByID(id int16) (string, forma.SchemaAttributeCache, error) {
	cache, ok := s.cacheByID[id]
	if !ok {
		return "", nil, nil
	}
	return "", cache, nil
}

func (s stubSchemaRegistry) GetSchemaByName(name string) (int16, forma.JSONSchema, error) {
	return 0, forma.JSONSchema{}, nil
}

func (s stubSchemaRegistry) GetSchemaByID(id int16) (string, forma.JSONSchema, error) {
	return "", forma.JSONSchema{}, nil
}

func (s stubSchemaRegistry) ListSchemas() []string {
	return nil
}

func TestAttributeIDForRecordUsesSchemaRegistry(t *testing.T) {
	h := &FederatedTestHarness{
		Registry: stubSchemaRegistry{cacheByID: map[int16]forma.SchemaAttributeCache{
			102: {
				"symbol": {AttributeID: 1},
			},
		}},
	}

	attrID, shouldInsert, err := h.attributeIDForRecord(102, "symbol")
	if err != nil {
		t.Fatalf("attributeIDForRecord returned error: %v", err)
	}
	if !shouldInsert {
		t.Fatal("expected schema attribute to be inserted")
	}
	if attrID != 1 {
		t.Fatalf("expected schema registry attribute ID 1, got %d", attrID)
	}
	if attrID == int16(DeterministicAttributeID(102, "symbol")) {
		t.Fatalf("expected schema registry attribute ID to differ from deterministic hash fallback")
	}
}

func TestAttributeIDForRecordSkipsBenchmarkMetadataOutsideSchema(t *testing.T) {
	h := &FederatedTestHarness{
		Registry: stubSchemaRegistry{cacheByID: map[int16]forma.SchemaAttributeCache{
			102: {
				"symbol": {AttributeID: 1},
			},
		}},
	}

	_, shouldInsert, err := h.attributeIDForRecord(102, "version")
	if err != nil {
		t.Fatalf("attributeIDForRecord returned error: %v", err)
	}
	if shouldInsert {
		t.Fatal("expected benchmark metadata field to be skipped")
	}
}

func TestAttributeIDForRecordErrorsWhenAttributeMissingFromSchemaRegistry(t *testing.T) {
	h := &FederatedTestHarness{
		Registry: stubSchemaRegistry{cacheByID: map[int16]forma.SchemaAttributeCache{
			102: {},
		}},
	}

	_, _, err := h.attributeIDForRecord(102, "symbol")
	if err == nil {
		t.Fatal("expected missing attribute error")
	}
	if !strings.Contains(err.Error(), "attribute symbol not found") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEAVValueColumnsEncodesBoolAsNumeric(t *testing.T) {
	valueText, valueNumeric := eavValueColumns(true)
	if valueText.Valid {
		t.Fatal("expected bool seeding to avoid value_text")
	}
	if !valueNumeric.Valid || valueNumeric.Float64 != 1 {
		t.Fatalf("expected bool true to encode as numeric 1, got %+v", valueNumeric)
	}

	valueText, valueNumeric = eavValueColumns(false)
	if valueText.Valid {
		t.Fatal("expected bool seeding to avoid value_text for false")
	}
	if !valueNumeric.Valid || valueNumeric.Float64 != 0 {
		t.Fatalf("expected bool false to encode as numeric 0, got %+v", valueNumeric)
	}
}

func TestDeterministicAttributeIDStable(t *testing.T) {
	first := DeterministicAttributeID(102, "symbol")
	second := DeterministicAttributeID(102, "symbol")
	if first != second {
		t.Fatalf("expected deterministic attribute IDs to match")
	}
	third := DeterministicAttributeID(102, "price")
	if first == third {
		t.Fatalf("expected different attributes to hash differently")
	}
}
