package transform

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

func TestExtractValueFromEAVRecord(t *testing.T) {
	textVal := "hello"
	numericVal := 42.0
	uuidVal := uuid.New()
	unixTime := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	unixMillis := float64(unixTime.UnixMilli())

	tests := []struct {
		name      string
		record    model.EAVRecord
		valueType forma.ValueType
		want      any
		wantErr   string
	}{
		{
			name:      "text nil returns nil",
			record:    model.EAVRecord{},
			valueType: forma.ValueTypeText,
			want:      nil,
		},
		{
			name: "text returns string",
			record: model.EAVRecord{
				ValueText: &textVal,
			},
			valueType: forma.ValueTypeText,
			want:      "hello",
		},
		{
			name: "smallint from numeric",
			record: model.EAVRecord{
				ValueNumeric: &numericVal,
			},
			valueType: forma.ValueTypeSmallInt,
			want:      int16(42),
		},
		{
			name: "integer from numeric",
			record: model.EAVRecord{
				ValueNumeric: &numericVal,
			},
			valueType: forma.ValueTypeInteger,
			want:      int32(42),
		},
		{
			name: "bigint from numeric",
			record: model.EAVRecord{
				ValueNumeric: &numericVal,
			},
			valueType: forma.ValueTypeBigInt,
			want:      int64(42),
		},
		{
			name: "numeric returns float64",
			record: model.EAVRecord{
				ValueNumeric: &numericVal,
			},
			valueType: forma.ValueTypeNumeric,
			want:      numericVal,
		},
		{
			name: "date from unix millis",
			record: model.EAVRecord{
				ValueNumeric: &unixMillis,
			},
			valueType: forma.ValueTypeDate,
			want:      unixTime,
		},
		{
			name: "datetime from unix millis",
			record: model.EAVRecord{
				ValueNumeric: &unixMillis,
			},
			valueType: forma.ValueTypeDateTime,
			want:      unixTime,
		},
		{
			name: "uuid from text",
			record: model.EAVRecord{
				ValueText: new(uuidVal.String()),
			},
			valueType: forma.ValueTypeUUID,
			want:      uuidVal,
		},
		{
			name: "uuid parse error",
			record: model.EAVRecord{
				ValueText: new("not-a-uuid"),
			},
			valueType: forma.ValueTypeUUID,
			wantErr:   "parse uuid",
		},
		{
			name: "bool true",
			record: model.EAVRecord{
				ValueNumeric: new(0.6),
			},
			valueType: forma.ValueTypeBool,
			want:      true,
		},
		{
			name: "bool false at threshold",
			record: model.EAVRecord{
				ValueNumeric: new(0.5),
			},
			valueType: forma.ValueTypeBool,
			want:      false,
		},
		{
			name: "unsupported uses text fallback",
			record: model.EAVRecord{
				ValueText: &textVal,
			},
			valueType: forma.ValueType("unknown"),
			want:      "hello",
		},
		{
			name: "unsupported uses numeric fallback",
			record: model.EAVRecord{
				ValueNumeric: &numericVal,
			},
			valueType: forma.ValueType("unknown"),
			want:      numericVal,
		},
		{
			name:      "unsupported nil returns nil",
			record:    model.EAVRecord{},
			valueType: forma.ValueType("unknown"),
			want:      nil,
		},
		{
			name: "numeric nil returns nil",
			record: model.EAVRecord{
				ValueNumeric: nil,
			},
			valueType: forma.ValueTypeNumeric,
			want:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractValueFromEAVRecord(tt.record, tt.valueType)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("expected error %q, got %q", tt.wantErr, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			switch want := tt.want.(type) {
			case time.Time:
				gotTime, ok := got.(time.Time)
				if !ok {
					t.Fatalf("expected time.Time, got %T", got)
				}
				if !gotTime.Equal(want) {
					t.Fatalf("expected time %v, got %v", want, gotTime)
				}
			case uuid.UUID:
				gotUUID, ok := got.(uuid.UUID)
				if !ok {
					t.Fatalf("expected uuid.UUID, got %T", got)
				}
				if gotUUID != want {
					t.Fatalf("expected uuid %v, got %v", want, gotUUID)
				}
			case nil:
				if got != nil {
					t.Fatalf("expected nil, got %v", got)
				}
			default:
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("expected %v (%T), got %v (%T)", want, want, got, got)
				}
			}
		})
	}
}

func TestExtractValueFromEAVRecord_ReturnsErrorOnStorageTypeMismatch(t *testing.T) {
	textVal := "123"
	_, err := extractValueFromEAVRecord(model.EAVRecord{ValueText: &textVal}, forma.ValueTypeNumeric)
	if err == nil {
		t.Fatal("expected storage/type mismatch error")
	}
	if !strings.Contains(err.Error(), "storage type mismatch") || !strings.Contains(err.Error(), "value_text should not be populated") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExtractValueFromEAVRecord_BothColumnsPopulatedReturnsError(t *testing.T) {
	textVal := "text"
	numericVal := 123.0

	tests := []struct {
		name      string
		valueType forma.ValueType
		wantErr   string
	}{
		{name: "text type", valueType: forma.ValueTypeText, wantErr: "value_numeric should not be populated"},
		{name: "numeric type", valueType: forma.ValueTypeNumeric, wantErr: "value_text should not be populated"},
		{name: "uuid type", valueType: forma.ValueTypeUUID, wantErr: "value_numeric should not be populated"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := extractValueFromEAVRecord(model.EAVRecord{
				ValueText:    &textVal,
				ValueNumeric: &numericVal,
			}, tt.valueType)
			if err == nil {
				t.Fatal("expected storage/type mismatch error")
			}
			if !strings.Contains(err.Error(), "storage type mismatch") || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestToFloat64ForEAV(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		want    float64
		wantErr bool
	}{
		{"float64", float64(1.5), 1.5, false},
		{"float32", float32(2.5), 2.5, false},
		{"string valid", "123.45", 123.45, false},
		{"string trimmed", "  123.45  ", 123.45, false},
		{"string empty", "", 0, true},
		{"string invalid", "abc", 0, true},
		{"pointer valid", new("123.45"), 123.45, false},
		{"pointer nil", (*string)(nil), 0, true},
		{"int", int(42), 42, false},
		{"int16", int16(10), 10, false},
		{"int32", int32(20), 20, false},
		{"int64", int64(30), 30, false},
		{"unsupported type", []int{1, 2}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toFloat64ForEAV(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("toFloat64ForEAV(%#v) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("toFloat64ForEAV(%#v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestToBoolForEAV(t *testing.T) {
	trueVal := true
	falseVal := false
	trueStr := "true"
	zeroStr := "0"
	tests := []struct {
		name    string
		input   any
		want    bool
		wantErr bool
	}{
		{"string true", "true", true, false},
		{"string false", "false", false, false},
		{"string one", "1", true, false},
		{"string zero", "0", false, false},
		{"string unknown", "maybe", false, false},
		{"ptr string true", &trueStr, true, false},
		{"ptr string zero", &zeroStr, false, false},
		{"ptr string nil", (*string)(nil), false, true},

		{"bool true", true, true, false},
		{"bool false", false, false, false},
		{"ptr bool true", &trueVal, true, false},
		{"ptr bool false", &falseVal, false, false},
		{"ptr bool nil", (*bool)(nil), false, true},

		{"int positive", int(1), true, false},
		{"int zero", int(0), false, false},
		{"int negative", int(-1), false, false},

		{"int32 negative", int32(-1), true, false},
		{"int32 zero", int32(0), false, false},

		{"int64 negative", int64(-1), true, false},
		{"int64 zero", int64(0), false, false},

		{"float64 >0.5", float64(0.6), true, false},
		{"float64 =0.5", float64(0.5), false, false},
		{"float64 small positive", float64(0.001), false, false},

		{"ptr float64 nil", (*float64)(nil), false, true},

		{"unsupported slice", []int{1, 2}, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toBoolForEAV(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("toBoolForEAV(%#v) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("toBoolForEAV(%#v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestAttributeConverterFromEAVRecords_NestedRequiredDependsOnParentPresence(t *testing.T) {
	registry := &stubSchemaRegistry{
		schemaID:   400,
		schemaName: "lead_schema",
		cache: forma.SchemaAttributeCache{
			"id":                           {AttributeID: 1, ValueType: forma.ValueTypeText, Required: true},
			"propertyInterests.propertyId": {AttributeID: 2, ValueType: forma.ValueTypeText},
			"propertyInterests.status":     {AttributeID: 3, ValueType: forma.ValueTypeText, Required: true},
		},
	}

	converter := NewAttributeConverter(registry)
	rowID := uuid.Must(uuid.NewV7())

	idValue := "lead-1"
	recordsWithoutParent := []model.EAVRecord{
		{
			SchemaID:  400,
			RowID:     rowID,
			AttrID:    1,
			ValueText: &idValue,
		},
	}

	_, err := converter.FromEAVRecords(recordsWithoutParent)
	if err != nil {
		t.Fatalf("expected no error when nested parent is absent, got %v", err)
	}

	propertyID := "p-1"
	recordsWithParent := []model.EAVRecord{
		recordsWithoutParent[0],
		{
			SchemaID:     400,
			RowID:        rowID,
			AttrID:       2,
			ArrayIndices: "0",
			ValueText:    &propertyID,
		},
	}

	_, err = converter.FromEAVRecords(recordsWithParent)
	if err == nil {
		t.Fatalf("expected error when nested parent exists but required child is missing")
	}
	if !strings.Contains(err.Error(), "missing required attribute 'propertyInterests.status'") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAttributeConverterFromEAVRecords_UnknownAttributeIDReturnsError(t *testing.T) {
	registry := &stubSchemaRegistry{
		schemaID:   402,
		schemaName: "unknown_attr_schema",
		cache: forma.SchemaAttributeCache{
			"name": {AttributeID: 1, ValueType: forma.ValueTypeText},
		},
	}

	converter := NewAttributeConverter(registry)
	rowID := uuid.Must(uuid.NewV7())
	value := "mystery"
	_, err := converter.FromEAVRecords([]model.EAVRecord{{
		SchemaID:  402,
		RowID:     rowID,
		AttrID:    999,
		ValueText: &value,
	}})
	if err == nil {
		t.Fatal("expected unknown attr id error")
	}
	if !strings.Contains(err.Error(), "unknown attribute id") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAttributeConverterFromEAVRecords_RequiredChildUsesArrayIndexContext(t *testing.T) {
	registry := &stubSchemaRegistry{
		schemaID:   403,
		schemaName: "indexed_required_schema",
		cache: forma.SchemaAttributeCache{
			"id":                           {AttributeID: 1, ValueType: forma.ValueTypeText, Required: true},
			"propertyInterests.propertyId": {AttributeID: 2, ValueType: forma.ValueTypeText},
			"propertyInterests.status":     {AttributeID: 3, ValueType: forma.ValueTypeText, Required: true},
		},
	}

	converter := NewAttributeConverter(registry)
	rowID := uuid.Must(uuid.NewV7())
	idValue := "lead-1"
	propertyID := "p-1"
	status := "viewed"

	_, err := converter.FromEAVRecords([]model.EAVRecord{
		{SchemaID: 403, RowID: rowID, AttrID: 1, ValueText: &idValue},
		{SchemaID: 403, RowID: rowID, AttrID: 2, ArrayIndices: "0", ValueText: &propertyID},
		{SchemaID: 403, RowID: rowID, AttrID: 3, ArrayIndices: "1", ValueText: &status},
	})
	if err == nil {
		t.Fatal("expected per-index required validation error")
	}
	if !strings.Contains(err.Error(), "missing required attribute 'propertyInterests.status'") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAttributeConverterFromEAVRecords_NestedArrayRequiredUsesFullIndexPath(t *testing.T) {
	registry := &stubSchemaRegistry{
		schemaID:   404,
		schemaName: "nested_array_schema",
		cache: forma.SchemaAttributeCache{
			"id":                    {AttributeID: 1, ValueType: forma.ValueTypeText, Required: true},
			"orders.items.name":     {AttributeID: 2, ValueType: forma.ValueTypeText},
			"orders.items.price":    {AttributeID: 3, ValueType: forma.ValueTypeNumeric, Required: true},
			"contact.email":         {AttributeID: 4, ValueType: forma.ValueTypeText, Required: true},
			"contact.phones.number": {AttributeID: 5, ValueType: forma.ValueTypeText},
			"contact.phones.kind":   {AttributeID: 6, ValueType: forma.ValueTypeText, Required: true},
		},
	}

	converter := NewAttributeConverter(registry)
	rowID := uuid.Must(uuid.NewV7())
	idValue := "lead-1"
	itemNameA := "item-a"
	itemPriceA := 10.0
	itemNameB := "item-b"
	email := "test@example.com"
	phoneNumber := "555-1234"

	_, err := converter.FromEAVRecords([]model.EAVRecord{
		{SchemaID: 404, RowID: rowID, AttrID: 1, ValueText: &idValue},
		{SchemaID: 404, RowID: rowID, AttrID: 2, ArrayIndices: "0.0", ValueText: &itemNameA},
		{SchemaID: 404, RowID: rowID, AttrID: 3, ArrayIndices: "0.0", ValueNumeric: &itemPriceA},
		{SchemaID: 404, RowID: rowID, AttrID: 2, ArrayIndices: "0.1", ValueText: &itemNameB},
		{SchemaID: 404, RowID: rowID, AttrID: 4, ArrayIndices: "", ValueText: &email},
		{SchemaID: 404, RowID: rowID, AttrID: 5, ArrayIndices: "0", ValueText: &phoneNumber},
	})
	if err == nil {
		t.Fatal("expected nested-array required validation error")
	}
	if !strings.Contains(err.Error(), "missing required attribute '") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(err.Error(), "orders.items.price") && !strings.Contains(err.Error(), "contact.phones.kind") {
		t.Fatalf("expected nested required attribute error, got %v", err)
	}
}

func TestAttributeConverterFromEAVRecords_MixedArrayAndNonArrayChildrenDoNotConflict(t *testing.T) {
	registry := &stubSchemaRegistry{
		schemaID:   405,
		schemaName: "mixed_contact_schema",
		cache: forma.SchemaAttributeCache{
			"id":                    {AttributeID: 1, ValueType: forma.ValueTypeText, Required: true},
			"contact.email":         {AttributeID: 2, ValueType: forma.ValueTypeText, Required: true},
			"contact.phones.number": {AttributeID: 3, ValueType: forma.ValueTypeText},
			"contact.phones.kind":   {AttributeID: 4, ValueType: forma.ValueTypeText, Required: true},
		},
	}

	converter := NewAttributeConverter(registry)
	rowID := uuid.Must(uuid.NewV7())
	idValue := "lead-1"
	email := "test@example.com"
	phoneNumber := "555-1234"
	phoneKind := "mobile"

	_, err := converter.FromEAVRecords([]model.EAVRecord{
		{SchemaID: 405, RowID: rowID, AttrID: 1, ValueText: &idValue},
		{SchemaID: 405, RowID: rowID, AttrID: 2, ArrayIndices: "", ValueText: &email},
		{SchemaID: 405, RowID: rowID, AttrID: 3, ArrayIndices: "0", ValueText: &phoneNumber},
		{SchemaID: 405, RowID: rowID, AttrID: 4, ArrayIndices: "0", ValueText: &phoneKind},
	})
	if err != nil {
		t.Fatalf("expected mixed array/non-array required attributes to pass, got %v", err)
	}
}

func TestShouldEnforceRequiredAttribute(t *testing.T) {
	tests := []struct {
		name            string
		attrName        string
		presentAttrName []string
		expected        bool
	}{
		{
			name:            "top-level required is always enforced",
			attrName:        "id",
			presentAttrName: nil,
			expected:        true,
		},
		{
			name:            "nested required skipped when parent missing",
			attrName:        "contact.email",
			presentAttrName: []string{"id"},
			expected:        false,
		},
		{
			name:            "nested required enforced when parent descendant exists",
			attrName:        "contact.email",
			presentAttrName: []string{"contact.phone"},
			expected:        true,
		},
		{
			name:            "deep nested required skipped when immediate parent missing",
			attrName:        "contact.snapshot.code",
			presentAttrName: []string{"contact.phone"},
			expected:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			present := make(map[string]map[string]struct{}, len(tt.presentAttrName))
			for _, name := range tt.presentAttrName {
				present[name] = map[string]struct{}{"": {}}
			}
			got := shouldEnforceRequiredAttribute(tt.attrName, present)
			if got != tt.expected {
				t.Fatalf("expected %v, got %v", tt.expected, got)
			}
		})
	}
}

func TestAttributeConverterFromEAVRecords_RequiredAlwaysIgnoresParentPresence(t *testing.T) {
	registry := &stubSchemaRegistry{
		schemaID:   401,
		schemaName: "always_schema",
		cache: forma.SchemaAttributeCache{
			"id":               {AttributeID: 1, ValueType: forma.ValueTypeText, RequiredPolicy: forma.RequiredPolicyAlways},
			"contact.email":    {AttributeID: 2, ValueType: forma.ValueTypeText, RequiredPolicy: forma.RequiredPolicyAlways},
			"contact.nickname": {AttributeID: 3, ValueType: forma.ValueTypeText},
		},
	}

	converter := NewAttributeConverter(registry)
	rowID := uuid.Must(uuid.NewV7())
	idValue := "lead-1"
	records := []model.EAVRecord{
		{
			SchemaID:  401,
			RowID:     rowID,
			AttrID:    1,
			ValueText: &idValue,
		},
	}

	_, err := converter.FromEAVRecords(records)
	if err == nil {
		t.Fatalf("expected error for required_always attribute when parent path is absent")
	}
	if !strings.Contains(err.Error(), "missing required attribute 'contact.email'") {
		t.Fatalf("unexpected error: %v", err)
	}
}
