package internal

import (
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

func TestFilterAttributes_EmptyAttrs(t *testing.T) {
	attributes := map[string]any{
		"name":  "John",
		"email": "john@example.com",
		"age":   30,
	}

	// When attrs is empty, should return original attributes
	result := FilterAttributes(attributes, nil)
	if len(result) != 3 {
		t.Errorf("expected 3 attributes, got %d", len(result))
	}

	result = FilterAttributes(attributes, []string{})
	if len(result) != 3 {
		t.Errorf("expected 3 attributes, got %d", len(result))
	}
}

func TestFilterAttributes_SimpleAttributes(t *testing.T) {
	attributes := map[string]any{
		"name":  "John",
		"email": "john@example.com",
		"age":   30,
		"phone": "123-456-7890",
	}

	// Filter to only name and email
	result := FilterAttributes(attributes, []string{"name", "email"})
	if len(result) != 2 {
		t.Errorf("expected 2 attributes, got %d", len(result))
	}
	if result["name"] != "John" {
		t.Errorf("expected name to be 'John', got %v", result["name"])
	}
	if result["email"] != "john@example.com" {
		t.Errorf("expected email to be 'john@example.com', got %v", result["email"])
	}
	if _, exists := result["age"]; exists {
		t.Errorf("expected age to be filtered out")
	}
}

func TestFilterAttributes_NestedAttributes(t *testing.T) {
	attributes := map[string]any{
		"name": "John",
		"contact": map[string]any{
			"phone":   "123-456-7890",
			"email":   "john@example.com",
			"address": "123 Main St",
		},
		"age": 30,
	}

	// Filter to nested attribute contact.phone
	result := FilterAttributes(attributes, []string{"name", "contact.phone"})
	if len(result) != 2 {
		t.Errorf("expected 2 top-level attributes, got %d", len(result))
	}
	if result["name"] != "John" {
		t.Errorf("expected name to be 'John', got %v", result["name"])
	}

	contact, ok := result["contact"].(map[string]any)
	if !ok {
		t.Fatalf("expected contact to be a map, got %T", result["contact"])
	}
	if contact["phone"] != "123-456-7890" {
		t.Errorf("expected contact.phone to be '123-456-7890', got %v", contact["phone"])
	}
	if _, exists := contact["email"]; exists {
		t.Errorf("expected contact.email to be filtered out")
	}
}

func TestFilterAttributes_MultipleNestedAttributes(t *testing.T) {
	attributes := map[string]any{
		"name": "John",
		"contact": map[string]any{
			"phone":   "123-456-7890",
			"email":   "john@example.com",
			"address": "123 Main St",
		},
		"status": "active",
	}

	// Filter to multiple nested attributes
	result := FilterAttributes(attributes, []string{"contact.phone", "contact.email"})
	if len(result) != 1 {
		t.Errorf("expected 1 top-level attribute, got %d", len(result))
	}

	contact, ok := result["contact"].(map[string]any)
	if !ok {
		t.Fatalf("expected contact to be a map, got %T", result["contact"])
	}
	if len(contact) != 2 {
		t.Errorf("expected 2 nested attributes, got %d", len(contact))
	}
	if contact["phone"] != "123-456-7890" {
		t.Errorf("expected contact.phone to be '123-456-7890', got %v", contact["phone"])
	}
	if contact["email"] != "john@example.com" {
		t.Errorf("expected contact.email to be 'john@example.com', got %v", contact["email"])
	}
}

func TestFilterAttributes_NonExistentAttribute(t *testing.T) {
	attributes := map[string]any{
		"name":  "John",
		"email": "john@example.com",
	}

	// Filter with non-existent attribute
	result := FilterAttributes(attributes, []string{"name", "phone"})
	if len(result) != 1 {
		t.Errorf("expected 1 attribute, got %d", len(result))
	}
	if result["name"] != "John" {
		t.Errorf("expected name to be 'John', got %v", result["name"])
	}
}

func TestFilterAttributes_WhitespaceHandling(t *testing.T) {
	attributes := map[string]any{
		"name":  "John",
		"email": "john@example.com",
	}

	// Filter with whitespace in attrs
	result := FilterAttributes(attributes, []string{" name ", "  email  "})
	if len(result) != 2 {
		t.Errorf("expected 2 attributes, got %d", len(result))
	}
}

func TestFilterAttributes_EntireNestedObject(t *testing.T) {
	attributes := map[string]any{
		"name": "John",
		"contact": map[string]any{
			"phone":   "123-456-7890",
			"email":   "john@example.com",
			"address": "123 Main St",
		},
	}

	// Filter to include entire nested object
	result := FilterAttributes(attributes, []string{"contact"})
	if len(result) != 1 {
		t.Errorf("expected 1 attribute, got %d", len(result))
	}

	contact, ok := result["contact"].(map[string]any)
	if !ok {
		t.Fatalf("expected contact to be a map, got %T", result["contact"])
	}
	if len(contact) != 3 {
		t.Errorf("expected 3 nested attributes, got %d", len(contact))
	}
}

func TestFilterDataRecord(t *testing.T) {
	rowID := uuid.New()
	record := &forma.DataRecord{
		SchemaName: "test_schema",
		RowID:      rowID,
		Attributes: map[string]any{
			"name":   "John",
			"email":  "john@example.com",
			"status": "active",
		},
	}

	// Filter data record
	result := FilterDataRecord(record, []string{"name", "email"})
	if result.SchemaName != "test_schema" {
		t.Errorf("expected schema_name to be 'test_schema', got %s", result.SchemaName)
	}
	if result.RowID != rowID {
		t.Errorf("expected row_id to be preserved")
	}
	if len(result.Attributes) != 2 {
		t.Errorf("expected 2 attributes, got %d", len(result.Attributes))
	}
}

func TestFilterDataRecord_NilRecord(t *testing.T) {
	result := FilterDataRecord(nil, []string{"name"})
	if result != nil {
		t.Errorf("expected nil result for nil record")
	}
}

func TestFilterDataRecord_EmptyAttrs(t *testing.T) {
	rowID := uuid.New()
	record := &forma.DataRecord{
		SchemaName: "test_schema",
		RowID:      rowID,
		Attributes: map[string]any{
			"name":   "John",
			"email":  "john@example.com",
			"status": "active",
		},
	}

	// Empty attrs should return original record
	result := FilterDataRecord(record, nil)
	if result != record {
		t.Errorf("expected same record reference when attrs is empty")
	}

	result = FilterDataRecord(record, []string{})
	if result != record {
		t.Errorf("expected same record reference when attrs is empty")
	}
}
