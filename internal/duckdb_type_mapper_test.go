package internal

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Tests for MapValueTypeToDuckDBType
// ============================================================================

func TestMapValueTypeToDuckDBType_AllSupportedTypes(t *testing.T) {
	tests := []struct {
		name     string
		vt       forma.ValueType
		expected string
	}{
		{
			name:     "ValueTypeText",
			vt:       forma.ValueTypeText,
			expected: "VARCHAR",
		},
		{
			name:     "ValueTypeUUID",
			vt:       forma.ValueTypeUUID,
			expected: "VARCHAR",
		},
		{
			name:     "ValueTypeSmallInt",
			vt:       forma.ValueTypeSmallInt,
			expected: "SMALLINT",
		},
		{
			name:     "ValueTypeInteger",
			vt:       forma.ValueTypeInteger,
			expected: "INTEGER",
		},
		{
			name:     "ValueTypeBigInt",
			vt:       forma.ValueTypeBigInt,
			expected: "BIGINT",
		},
		{
			name:     "ValueTypeNumeric",
			vt:       forma.ValueTypeNumeric,
			expected: "DOUBLE",
		},
		{
			name:     "ValueTypeDate",
			vt:       forma.ValueTypeDate,
			expected: "TIMESTAMP",
		},
		{
			name:     "ValueTypeDateTime",
			vt:       forma.ValueTypeDateTime,
			expected: "TIMESTAMP",
		},
		{
			name:     "ValueTypeBool",
			vt:       forma.ValueTypeBool,
			expected: "BOOLEAN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MapValueTypeToDuckDBType(tt.vt)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestMapValueTypeToDuckDBType_UnknownTypeDefaultsToVarchar(t *testing.T) {
	// Test with an invalid/unknown ValueType
	unknownType := forma.ValueType("unknown_type_xyz")
	result := MapValueTypeToDuckDBType(unknownType)
	require.Equal(t, "VARCHAR", result)
}

// ============================================================================
// Tests for CastExpression
// ============================================================================

func TestCastExpression_SimpleColumn(t *testing.T) {
	tests := []struct {
		name     string
		column   string
		vt       forma.ValueType
		expected string
	}{
		{
			name:     "CastColumnToInteger",
			column:   "age",
			vt:       forma.ValueTypeInteger,
			expected: "CAST(age AS INTEGER)",
		},
		{
			name:     "CastColumnToBoolean",
			column:   "is_active",
			vt:       forma.ValueTypeBool,
			expected: "CAST(is_active AS BOOLEAN)",
		},
		{
			name:     "CastColumnToTimestamp",
			column:   "created_at",
			vt:       forma.ValueTypeDateTime,
			expected: "CAST(created_at AS TIMESTAMP)",
		},
		{
			name:     "CastColumnToVarchar",
			column:   "email",
			vt:       forma.ValueTypeText,
			expected: "CAST(email AS VARCHAR)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CastExpression(tt.column, tt.vt)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestCastExpression_ComplexExpression(t *testing.T) {
	// Test with a complex expression (not just a column name)
	expr := "SUBSTRING(email FROM 1 FOR 5)"
	vt := forma.ValueTypeText
	result := CastExpression(expr, vt)
	require.Equal(t, "CAST(SUBSTRING(email FROM 1 FOR 5) AS VARCHAR)", result)
}

// ============================================================================
// Tests for ToDuckDBParam
// ============================================================================

func TestToDuckDBParam_NilValue(t *testing.T) {
	result, err := ToDuckDBParam(nil, forma.ValueTypeText)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestToDuckDBParam_UUID_FromUUID(t *testing.T) {
	testUUID := uuid.New()
	result, err := ToDuckDBParam(testUUID, forma.ValueTypeUUID)
	require.NoError(t, err)
	require.Equal(t, testUUID.String(), result)
}

func TestToDuckDBParam_UUID_FromPointerUUID(t *testing.T) {
	testUUID := uuid.New()
	result, err := ToDuckDBParam(&testUUID, forma.ValueTypeUUID)
	require.NoError(t, err)
	require.Equal(t, testUUID.String(), result)
}

func TestToDuckDBParam_UUID_FromNilPointer(t *testing.T) {
	var ptr *uuid.UUID
	result, err := ToDuckDBParam(ptr, forma.ValueTypeUUID)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestToDuckDBParam_UUID_FromString(t *testing.T) {
	testStr := "550e8400-e29b-41d4-a716-446655440000"
	result, err := ToDuckDBParam(testStr, forma.ValueTypeUUID)
	require.NoError(t, err)
	require.Equal(t, testStr, result)
}

func TestToDuckDBParam_UUID_InvalidType(t *testing.T) {
	result, err := ToDuckDBParam(123, forma.ValueTypeUUID)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "cannot convert")
}

func TestToDuckDBParam_DateTime_FromTime(t *testing.T) {
	now := time.Now()
	result, err := ToDuckDBParam(now, forma.ValueTypeDateTime)
	require.NoError(t, err)
	require.Equal(t, now.UTC(), result)
}

func TestToDuckDBParam_DateTime_FromPointerTime(t *testing.T) {
	now := time.Now()
	result, err := ToDuckDBParam(&now, forma.ValueTypeDateTime)
	require.NoError(t, err)
	require.Equal(t, now.UTC(), result)
}

func TestToDuckDBParam_DateTime_FromNilPointer(t *testing.T) {
	var ptr *time.Time
	result, err := ToDuckDBParam(ptr, forma.ValueTypeDateTime)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestToDuckDBParam_DateTime_InvalidType(t *testing.T) {
	result, err := ToDuckDBParam("2024-01-01", forma.ValueTypeDateTime)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "cannot convert")
}

func TestToDuckDBParam_Date_FromTime(t *testing.T) {
	now := time.Now()
	result, err := ToDuckDBParam(now, forma.ValueTypeDate)
	require.NoError(t, err)
	require.Equal(t, now.UTC(), result)
}

func TestToDuckDBParam_Bool_FromBool(t *testing.T) {
	result, err := ToDuckDBParam(true, forma.ValueTypeBool)
	require.NoError(t, err)
	require.Equal(t, true, result)

	result, err = ToDuckDBParam(false, forma.ValueTypeBool)
	require.NoError(t, err)
	require.Equal(t, false, result)
}

func TestToDuckDBParam_Bool_FromPointerBool(t *testing.T) {
	trueVal := true
	result, err := ToDuckDBParam(&trueVal, forma.ValueTypeBool)
	require.NoError(t, err)
	require.Equal(t, true, result)
}

func TestToDuckDBParam_Bool_FromNilPointer(t *testing.T) {
	var ptr *bool
	result, err := ToDuckDBParam(ptr, forma.ValueTypeBool)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestToDuckDBParam_Bool_InvalidType(t *testing.T) {
	result, err := ToDuckDBParam("true", forma.ValueTypeBool)
	require.Error(t, err)
	require.Nil(t, result)
}

// Numeric type tests
func TestToDuckDBParam_Numeric_FromFloat64(t *testing.T) {
	result, err := ToDuckDBParam(42.5, forma.ValueTypeNumeric)
	require.NoError(t, err)
	require.Equal(t, 42.5, result)
}

func TestToDuckDBParam_Numeric_FromPointerFloat64(t *testing.T) {
	val := 42.5
	result, err := ToDuckDBParam(&val, forma.ValueTypeNumeric)
	require.NoError(t, err)
	require.Equal(t, 42.5, result)
}

func TestToDuckDBParam_Numeric_FromFloat32(t *testing.T) {
	val := float32(42.5)
	result, err := ToDuckDBParam(val, forma.ValueTypeNumeric)
	require.NoError(t, err)
	require.Equal(t, float64(42.5), result.(float64))
}

func TestToDuckDBParam_Numeric_FromInt(t *testing.T) {
	result, err := ToDuckDBParam(42, forma.ValueTypeInteger)
	require.NoError(t, err)
	require.Equal(t, float64(42), result)
}

func TestToDuckDBParam_Numeric_FromInt16(t *testing.T) {
	val := int16(42)
	result, err := ToDuckDBParam(val, forma.ValueTypeSmallInt)
	require.NoError(t, err)
	require.Equal(t, float64(42), result)
}

func TestToDuckDBParam_Numeric_FromInt32(t *testing.T) {
	val := int32(42)
	result, err := ToDuckDBParam(val, forma.ValueTypeInteger)
	require.NoError(t, err)
	require.Equal(t, float64(42), result)
}

func TestToDuckDBParam_Numeric_FromInt64(t *testing.T) {
	val := int64(42)
	result, err := ToDuckDBParam(val, forma.ValueTypeBigInt)
	require.NoError(t, err)
	require.Equal(t, float64(42), result)
}

func TestToDuckDBParam_Numeric_FromString(t *testing.T) {
	result, err := ToDuckDBParam("42.5", forma.ValueTypeNumeric)
	require.NoError(t, err)
	require.Equal(t, "42.5", result)
}

func TestToDuckDBParam_Numeric_FromPointerFloat64Nil(t *testing.T) {
	var ptr *float64
	result, err := ToDuckDBParam(ptr, forma.ValueTypeNumeric)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestToDuckDBParam_Numeric_InvalidType(t *testing.T) {
	result, err := ToDuckDBParam(map[string]any{}, forma.ValueTypeNumeric)
	require.Error(t, err)
	require.Nil(t, result)
}

// Text type tests
func TestToDuckDBParam_Text_FromString(t *testing.T) {
	result, err := ToDuckDBParam("hello", forma.ValueTypeText)
	require.NoError(t, err)
	require.Equal(t, "hello", result)
}

func TestToDuckDBParam_Text_FromPointerString(t *testing.T) {
	val := "hello"
	result, err := ToDuckDBParam(&val, forma.ValueTypeText)
	require.NoError(t, err)
	require.Equal(t, "hello", result)
}

func TestToDuckDBParam_Text_FromNilPointer(t *testing.T) {
	var ptr *string
	result, err := ToDuckDBParam(ptr, forma.ValueTypeText)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestToDuckDBParam_Text_InvalidType(t *testing.T) {
	result, err := ToDuckDBParam(123, forma.ValueTypeText)
	require.Error(t, err)
	require.Nil(t, result)
}

// Unknown type fallback
func TestToDuckDBParam_UnknownType_ReturnsAsIs(t *testing.T) {
	val := map[string]any{"key": "value"}
	result, err := ToDuckDBParam(val, forma.ValueType("unknown"))
	require.NoError(t, err)
	require.Equal(t, val, result)
}

// ============================================================================
// Tests for LIST type support
// ============================================================================

func TestMapValueTypeToDuckDBType_ListType(t *testing.T) {
	// LIST type should return VARCHAR (default element type)
	result := MapValueTypeToDuckDBType(forma.ValueTypeList)
	require.Equal(t, "VARCHAR", result)
}

func TestMapValueTypeToListDuckDBType_TextElement(t *testing.T) {
	result := MapValueTypeToListDuckDBType(forma.ValueTypeText)
	require.Equal(t, "LIST(VARCHAR)", result)
}

func TestMapValueTypeToListDuckDBType_IntegerElement(t *testing.T) {
	result := MapValueTypeToListDuckDBType(forma.ValueTypeInteger)
	require.Equal(t, "LIST(INTEGER)", result)
}

func TestMapValueTypeToListDuckDBType_BigIntElement(t *testing.T) {
	result := MapValueTypeToListDuckDBType(forma.ValueTypeBigInt)
	require.Equal(t, "LIST(BIGINT)", result)
}

func TestMapValueTypeToListDuckDBType_DoubleElement(t *testing.T) {
	result := MapValueTypeToListDuckDBType(forma.ValueTypeNumeric)
	require.Equal(t, "LIST(DOUBLE)", result)
}

func TestMapValueTypeToListDuckDBType_BoolElement(t *testing.T) {
	result := MapValueTypeToListDuckDBType(forma.ValueTypeBool)
	require.Equal(t, "LIST(BOOLEAN)", result)
}

func TestMapValueTypeToListDuckDBType_TimestampElement(t *testing.T) {
	result := MapValueTypeToListDuckDBType(forma.ValueTypeDateTime)
	require.Equal(t, "LIST(TIMESTAMP)", result)
}

func TestIsListType_True(t *testing.T) {
	require.True(t, IsListType(forma.ValueTypeList))
}

func TestIsListType_False(t *testing.T) {
	require.False(t, IsListType(forma.ValueTypeText))
	require.False(t, IsListType(forma.ValueTypeInteger))
	require.False(t, IsListType(forma.ValueTypeBool))
}
