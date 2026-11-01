package model

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
)

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
