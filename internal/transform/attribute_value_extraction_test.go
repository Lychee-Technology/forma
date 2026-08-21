package transform

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

func TestExtractValueFromEAVRecord(t *testing.T) { //nolint:funlen // #319 follow-up: oversized test functions, tracked separately
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
