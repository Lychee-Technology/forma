package internal

import (
	"reflect"
	"strings"
	"testing"
	"time"

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
		record    EAVRecord
		valueType forma.ValueType
		want      any
		wantErr   string
	}{
		{
			name:      "text nil returns nil",
			record:    EAVRecord{},
			valueType: forma.ValueTypeText,
			want:      nil,
		},
		{
			name: "text returns string",
			record: EAVRecord{
				ValueText: &textVal,
			},
			valueType: forma.ValueTypeText,
			want:      "hello",
		},
		{
			name: "smallint from numeric",
			record: EAVRecord{
				ValueNumeric: &numericVal,
			},
			valueType: forma.ValueTypeSmallInt,
			want:      int16(42),
		},
		{
			name: "integer from numeric",
			record: EAVRecord{
				ValueNumeric: &numericVal,
			},
			valueType: forma.ValueTypeInteger,
			want:      int32(42),
		},
		{
			name: "bigint from numeric",
			record: EAVRecord{
				ValueNumeric: &numericVal,
			},
			valueType: forma.ValueTypeBigInt,
			want:      int64(42),
		},
		{
			name: "numeric returns float64",
			record: EAVRecord{
				ValueNumeric: &numericVal,
			},
			valueType: forma.ValueTypeNumeric,
			want:      numericVal,
		},
		{
			name: "date from unix millis",
			record: EAVRecord{
				ValueNumeric: &unixMillis,
			},
			valueType: forma.ValueTypeDate,
			want:      unixTime,
		},
		{
			name: "datetime from unix millis",
			record: EAVRecord{
				ValueNumeric: &unixMillis,
			},
			valueType: forma.ValueTypeDateTime,
			want:      unixTime,
		},
		{
			name: "uuid from text",
			record: EAVRecord{
				ValueText: ptrString(uuidVal.String()),
			},
			valueType: forma.ValueTypeUUID,
			want:      uuidVal,
		},
		{
			name: "uuid parse error",
			record: EAVRecord{
				ValueText: ptrString("not-a-uuid"),
			},
			valueType: forma.ValueTypeUUID,
			wantErr:   "parse uuid",
		},
		{
			name: "bool true",
			record: EAVRecord{
				ValueNumeric: ptrFloat64(0.6),
			},
			valueType: forma.ValueTypeBool,
			want:      true,
		},
		{
			name: "bool false at threshold",
			record: EAVRecord{
				ValueNumeric: ptrFloat64(0.5),
			},
			valueType: forma.ValueTypeBool,
			want:      false,
		},
		{
			name: "unsupported uses text fallback",
			record: EAVRecord{
				ValueText: &textVal,
			},
			valueType: forma.ValueType("unknown"),
			want:      "hello",
		},
		{
			name: "unsupported uses numeric fallback",
			record: EAVRecord{
				ValueNumeric: &numericVal,
			},
			valueType: forma.ValueType("unknown"),
			want:      numericVal,
		},
		{
			name:      "unsupported nil returns nil",
			record:    EAVRecord{},
			valueType: forma.ValueType("unknown"),
			want:      nil,
		},
		{
			name: "numeric nil returns nil",
			record: EAVRecord{
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
		{"pointer valid", ptrString("123.45"), 123.45, false},
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

func ptrString(val string) *string {
	return &val
}

func ptrFloat64(val float64) *float64 {
	return &val
}

func TestToBoolForEAV(t *testing.T) {
	trueVal := true
	falseVal := false
	tests := []struct {
		name    string
		input   interface{}
		want    bool
		wantErr bool
	}{
		{"string true", "true", true, false},
		{"string false", "false", false, false},
		{"string one", "1", true, false},
		{"string zero", "0", false, false},
		{"string unknown", "maybe", false, false},

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
