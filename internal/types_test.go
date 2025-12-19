package internal

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
)

// =============================================================================
// AttributeOrder Tests
// =============================================================================

func TestAttributeOrder_AttrIDInt(t *testing.T) {
	tests := []struct {
		name   string
		attrID int16
		want   int
	}{
		{name: "zero", attrID: 0, want: 0},
		{name: "positive", attrID: 42, want: 42},
		{name: "negative", attrID: -10, want: -10},
		{name: "max int16", attrID: 32767, want: 32767},
		{name: "min int16", attrID: -32768, want: -32768},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ao := &AttributeOrder{AttrID: tt.attrID}
			assert.Equal(t, tt.want, ao.AttrIDInt())
		})
	}
}

func TestAttributeOrder_ValueColumn(t *testing.T) {
	tests := []struct {
		name      string
		valueType forma.ValueType
		want      string
	}{
		{name: "text", valueType: forma.ValueTypeText, want: "value_text"},
		{name: "numeric", valueType: forma.ValueTypeNumeric, want: "value_numeric"},
		{name: "smallint defaults to text", valueType: forma.ValueTypeSmallInt, want: "value_text"},
		{name: "integer defaults to text", valueType: forma.ValueTypeInteger, want: "value_text"},
		{name: "bigint defaults to text", valueType: forma.ValueTypeBigInt, want: "value_text"},
		{name: "date defaults to text", valueType: forma.ValueTypeDate, want: "value_text"},
		{name: "datetime defaults to text", valueType: forma.ValueTypeDateTime, want: "value_text"},
		{name: "uuid defaults to text", valueType: forma.ValueTypeUUID, want: "value_text"},
		{name: "bool defaults to text", valueType: forma.ValueTypeBool, want: "value_text"},
		{name: "unknown defaults to text", valueType: forma.ValueType("unknown"), want: "value_text"},
		{name: "empty defaults to text", valueType: forma.ValueType(""), want: "value_text"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ao := &AttributeOrder{ValueType: tt.valueType}
			assert.Equal(t, tt.want, ao.ValueColumn())
		})
	}
}

func TestAttributeOrder_Desc(t *testing.T) {
	tests := []struct {
		name      string
		sortOrder forma.SortOrder
		want      bool
	}{
		{name: "descending", sortOrder: forma.SortOrderDesc, want: true},
		{name: "ascending", sortOrder: forma.SortOrderAsc, want: false},
		{name: "empty", sortOrder: forma.SortOrder(""), want: false},
		{name: "unknown", sortOrder: forma.SortOrder("unknown"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ao := &AttributeOrder{SortOrder: tt.sortOrder}
			assert.Equal(t, tt.want, ao.Desc())
		})
	}
}

func TestAttributeOrder_IsMainColumn(t *testing.T) {
	tests := []struct {
		name            string
		storageLocation forma.AttributeStorageLocation
		columnName      string
		want            bool
	}{
		{name: "main with column name", storageLocation: forma.AttributeStorageLocationMain, columnName: "my_column", want: true},
		{name: "main without column name", storageLocation: forma.AttributeStorageLocationMain, columnName: "", want: false},
		{name: "eav with column name", storageLocation: forma.AttributeStorageLocationEAV, columnName: "my_column", want: false},
		{name: "eav without column name", storageLocation: forma.AttributeStorageLocationEAV, columnName: "", want: false},
		{name: "unknown with column name", storageLocation: forma.AttributeStorageLocationUnknown, columnName: "my_column", want: false},
		{name: "unknown without column name", storageLocation: forma.AttributeStorageLocationUnknown, columnName: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ao := &AttributeOrder{
				StorageLocation: tt.storageLocation,
				ColumnName:      tt.columnName,
			}
			assert.Equal(t, tt.want, ao.IsMainColumn())
		})
	}
}

func TestAttributeOrder_MainColumnName(t *testing.T) {
	tests := []struct {
		name       string
		columnName string
		want       string
	}{
		{name: "with column name", columnName: "my_column", want: "my_column"},
		{name: "empty column name", columnName: "", want: ""},
		{name: "special characters", columnName: "column_with_underscore", want: "column_with_underscore"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ao := &AttributeOrder{ColumnName: tt.columnName}
			assert.Equal(t, tt.want, ao.MainColumnName())
		})
	}
}

// =============================================================================
// EntityAttribute Tests
// =============================================================================

func TestEntityAttribute_Text(t *testing.T) {
	textValue := "hello"

	tests := []struct {
		name      string
		ea        EntityAttribute
		want      *string
		wantErr   bool
		errSubstr string
	}{
		{
			name:    "nil value returns nil",
			ea:      EntityAttribute{ValueType: forma.ValueTypeText, Value: nil},
			want:    nil,
			wantErr: false,
		},
		{
			name:      "wrong value type returns error",
			ea:        EntityAttribute{ValueType: forma.ValueTypeNumeric, Value: "test"},
			want:      nil,
			wantErr:   true,
			errSubstr: "expected ValueType 'text'",
		},
		{
			name:      "wrong go type returns error",
			ea:        EntityAttribute{ValueType: forma.ValueTypeText, Value: 123},
			want:      nil,
			wantErr:   true,
			errSubstr: "value is not a string",
		},
		{
			name:    "valid text value",
			ea:      EntityAttribute{ValueType: forma.ValueTypeText, Value: textValue},
			want:    &textValue,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.ea.Text()
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errSubstr)
				assert.Nil(t, got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestEntityAttribute_SmallInt(t *testing.T) {
	smallIntValue := int16(42)

	tests := []struct {
		name      string
		ea        EntityAttribute
		want      *int16
		wantErr   bool
		errSubstr string
	}{
		{
			name:    "nil value returns nil",
			ea:      EntityAttribute{ValueType: forma.ValueTypeSmallInt, Value: nil},
			want:    nil,
			wantErr: false,
		},
		{
			name:      "wrong value type returns error",
			ea:        EntityAttribute{ValueType: forma.ValueTypeText, Value: int16(10)},
			want:      nil,
			wantErr:   true,
			errSubstr: "expected ValueType 'smallint'",
		},
		{
			name:      "wrong go type returns error",
			ea:        EntityAttribute{ValueType: forma.ValueTypeSmallInt, Value: "not an int"},
			want:      nil,
			wantErr:   true,
			errSubstr: "value is not an int16",
		},
		{
			name:    "valid smallint value",
			ea:      EntityAttribute{ValueType: forma.ValueTypeSmallInt, Value: smallIntValue},
			want:    &smallIntValue,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.ea.SmallInt()
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errSubstr)
				assert.Nil(t, got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestEntityAttribute_Integer(t *testing.T) {
	intValue := int32(12345)

	tests := []struct {
		name      string
		ea        EntityAttribute
		want      *int32
		wantErr   bool
		errSubstr string
	}{
		{
			name:    "nil value returns nil",
			ea:      EntityAttribute{ValueType: forma.ValueTypeInteger, Value: nil},
			want:    nil,
			wantErr: false,
		},
		{
			name:      "wrong value type returns error",
			ea:        EntityAttribute{ValueType: forma.ValueTypeText, Value: int32(10)},
			want:      nil,
			wantErr:   true,
			errSubstr: "expected ValueType 'integer'",
		},
		{
			name:      "wrong go type returns error",
			ea:        EntityAttribute{ValueType: forma.ValueTypeInteger, Value: "not an int"},
			want:      nil,
			wantErr:   true,
			errSubstr: "value is not an int32",
		},
		{
			name:    "valid integer value",
			ea:      EntityAttribute{ValueType: forma.ValueTypeInteger, Value: intValue},
			want:    &intValue,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.ea.Integer()
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errSubstr)
				assert.Nil(t, got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestEntityAttribute_BigInt(t *testing.T) {
	bigIntValue := int64(9223372036854775807)

	tests := []struct {
		name      string
		ea        EntityAttribute
		want      *int64
		wantErr   bool
		errSubstr string
	}{
		{
			name:    "nil value returns nil",
			ea:      EntityAttribute{ValueType: forma.ValueTypeBigInt, Value: nil},
			want:    nil,
			wantErr: false,
		},
		{
			name:      "wrong value type returns error",
			ea:        EntityAttribute{ValueType: forma.ValueTypeText, Value: int64(10)},
			want:      nil,
			wantErr:   true,
			errSubstr: "expected ValueType 'bigint'",
		},
		{
			name:      "wrong go type returns error",
			ea:        EntityAttribute{ValueType: forma.ValueTypeBigInt, Value: "not an int"},
			want:      nil,
			wantErr:   true,
			errSubstr: "value is not an int64",
		},
		{
			name:    "valid bigint value",
			ea:      EntityAttribute{ValueType: forma.ValueTypeBigInt, Value: bigIntValue},
			want:    &bigIntValue,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.ea.BigInt()
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errSubstr)
				assert.Nil(t, got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestEntityAttribute_Numeric(t *testing.T) {
	numericValue := float64(3.14159)

	tests := []struct {
		name      string
		ea        EntityAttribute
		want      *float64
		wantErr   bool
		errSubstr string
	}{
		{
			name:    "nil value returns nil",
			ea:      EntityAttribute{ValueType: forma.ValueTypeNumeric, Value: nil},
			want:    nil,
			wantErr: false,
		},
		{
			name:      "wrong value type returns error",
			ea:        EntityAttribute{ValueType: forma.ValueTypeText, Value: float64(10.5)},
			want:      nil,
			wantErr:   true,
			errSubstr: "expected ValueType 'numeric'",
		},
		{
			name:      "wrong go type returns error",
			ea:        EntityAttribute{ValueType: forma.ValueTypeNumeric, Value: "not a float"},
			want:      nil,
			wantErr:   true,
			errSubstr: "value is not a float64",
		},
		{
			name:    "valid numeric value",
			ea:      EntityAttribute{ValueType: forma.ValueTypeNumeric, Value: numericValue},
			want:    &numericValue,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.ea.Numeric()
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errSubstr)
				assert.Nil(t, got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestEntityAttribute_Date(t *testing.T) {
	dateValue := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		ea        EntityAttribute
		want      *time.Time
		wantErr   bool
		errSubstr string
	}{
		{
			name:    "nil value returns nil",
			ea:      EntityAttribute{ValueType: forma.ValueTypeDate, Value: nil},
			want:    nil,
			wantErr: false,
		},
		{
			name:      "wrong value type returns error",
			ea:        EntityAttribute{ValueType: forma.ValueTypeText, Value: time.Now()},
			want:      nil,
			wantErr:   true,
			errSubstr: "expected ValueType 'date'",
		},
		{
			name:      "wrong go type returns error",
			ea:        EntityAttribute{ValueType: forma.ValueTypeDate, Value: "2024-01-15"},
			want:      nil,
			wantErr:   true,
			errSubstr: "value is not a time.Time",
		},
		{
			name:    "valid date value",
			ea:      EntityAttribute{ValueType: forma.ValueTypeDate, Value: dateValue},
			want:    &dateValue,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.ea.Date()
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errSubstr)
				assert.Nil(t, got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestEntityAttribute_DateTime(t *testing.T) {
	dateTimeValue := time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC)

	tests := []struct {
		name      string
		ea        EntityAttribute
		want      *time.Time
		wantErr   bool
		errSubstr string
	}{
		{
			name:    "nil value returns nil",
			ea:      EntityAttribute{ValueType: forma.ValueTypeDateTime, Value: nil},
			want:    nil,
			wantErr: false,
		},
		{
			name:      "wrong value type returns error",
			ea:        EntityAttribute{ValueType: forma.ValueTypeText, Value: time.Now()},
			want:      nil,
			wantErr:   true,
			errSubstr: "expected ValueType 'datetime'",
		},
		{
			name:      "wrong go type returns error",
			ea:        EntityAttribute{ValueType: forma.ValueTypeDateTime, Value: "2024-01-15T10:30:45Z"},
			want:      nil,
			wantErr:   true,
			errSubstr: "value is not a time.Time",
		},
		{
			name:    "valid datetime value",
			ea:      EntityAttribute{ValueType: forma.ValueTypeDateTime, Value: dateTimeValue},
			want:    &dateTimeValue,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.ea.DateTime()
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errSubstr)
				assert.Nil(t, got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestEntityAttribute_UUID(t *testing.T) {
	uuidValue := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

	tests := []struct {
		name      string
		ea        EntityAttribute
		want      *uuid.UUID
		wantErr   bool
		errSubstr string
	}{
		{
			name:    "nil value returns nil",
			ea:      EntityAttribute{ValueType: forma.ValueTypeUUID, Value: nil},
			want:    nil,
			wantErr: false,
		},
		{
			name:      "wrong value type returns error",
			ea:        EntityAttribute{ValueType: forma.ValueTypeText, Value: uuid.New()},
			want:      nil,
			wantErr:   true,
			errSubstr: "expected ValueType 'uuid'",
		},
		{
			name:      "wrong go type returns error",
			ea:        EntityAttribute{ValueType: forma.ValueTypeUUID, Value: "550e8400-e29b-41d4-a716-446655440000"},
			want:      nil,
			wantErr:   true,
			errSubstr: "value is not a uuid.UUID",
		},
		{
			name:    "valid uuid value",
			ea:      EntityAttribute{ValueType: forma.ValueTypeUUID, Value: uuidValue},
			want:    &uuidValue,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.ea.UUID()
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errSubstr)
				assert.Nil(t, got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestEntityAttribute_Bool(t *testing.T) {
	boolValueTrue := true
	boolValueFalse := false

	tests := []struct {
		name      string
		ea        EntityAttribute
		want      *bool
		wantErr   bool
		errSubstr string
	}{
		{
			name:    "nil value returns nil",
			ea:      EntityAttribute{ValueType: forma.ValueTypeBool, Value: nil},
			want:    nil,
			wantErr: false,
		},
		{
			name:      "wrong value type returns error",
			ea:        EntityAttribute{ValueType: forma.ValueTypeText, Value: true},
			want:      nil,
			wantErr:   true,
			errSubstr: "expected ValueType 'bool'",
		},
		{
			name:      "wrong go type returns error",
			ea:        EntityAttribute{ValueType: forma.ValueTypeBool, Value: "true"},
			want:      nil,
			wantErr:   true,
			errSubstr: "value is not a bool",
		},
		{
			name:    "valid bool true value",
			ea:      EntityAttribute{ValueType: forma.ValueTypeBool, Value: boolValueTrue},
			want:    &boolValueTrue,
			wantErr: false,
		},
		{
			name:    "valid bool false value",
			ea:      EntityAttribute{ValueType: forma.ValueTypeBool, Value: boolValueFalse},
			want:    &boolValueFalse,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.ea.Bool()
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errSubstr)
				assert.Nil(t, got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
